//! Shared application state and cache policies.

use ishikari_core::{
    metrics::NodeMetrics,
    storage::{ObjectStoreRegistry, ResourceResolver},
};
use mmpf_cluster::BootstrapReadinessGate;
use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::drain::DrainController;
use crate::membership::Membership;
use crate::options::CacheCapacities;
use crate::provider::ProviderConfig;

use super::HttpError;
use super::cpu_work::{CpuWorkGate, CpuWorkPermit};
use super::tileset::mapterhorn::MapterhornResolver;
use super::upstream::ProviderFetcher;

pub(crate) struct ServerRuntimeConfig {
    pub gossip_bootstrap_readiness: BootstrapReadinessGate,
    pub delivery_auth: Option<mmpf_auth::DeliveryAuth>,
    pub mapterhorn: Option<Arc<MapterhornResolver>>,
    pub cpu_work_concurrency: usize,
    /// Maximum admitted CPU-work units (holding a permit or queued for one)
    /// before new work is shed with 503.
    pub cpu_work_max_inflight: usize,
    pub derived_negative_ttl: Duration,
    pub cache_capacities: CacheCapacities,
}

pub(super) struct CacheMaintenanceGuard {
    running: Arc<AtomicBool>,
}

impl Drop for CacheMaintenanceGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

struct DerivedTileExpiry {
    negative_ttl: Duration,
}

impl moka::Expiry<super::tileset::terrain::DerivedTileKey, super::tileset::terrain::DerivedOutcome>
    for DerivedTileExpiry
{
    fn expire_after_create(
        &self,
        _key: &super::tileset::terrain::DerivedTileKey,
        value: &super::tileset::terrain::DerivedOutcome,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        matches!(
            value,
            super::tileset::terrain::DerivedOutcome::Absent
                | super::tileset::terrain::DerivedOutcome::Degraded(_)
        )
        .then_some(self.negative_ttl)
    }
}

struct DecodedDemExpiry {
    negative_ttl: Duration,
}

impl
    moka::Expiry<
        (ishikari_core::interned::TilesetId, u64),
        Option<Arc<super::tileset::terrain::dem::DemTile>>,
    > for DecodedDemExpiry
{
    fn expire_after_create(
        &self,
        _key: &(ishikari_core::interned::TilesetId, u64),
        value: &Option<Arc<super::tileset::terrain::dem::DemTile>>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        value.is_none().then_some(self.negative_ttl)
    }
}

#[derive(Clone)]
pub(crate) struct AppState(Arc<AppStateInner>);

pub(crate) struct AppStateInner {
    pub(super) membership: Membership,
    pub(crate) metrics: NodeMetrics,
    pub(super) resource_resolver: Arc<ResourceResolver>,
    pub(super) drain: DrainController,
    gossip_bootstrap_readiness: BootstrapReadinessGate,
    pub(super) provider: ProviderConfig,
    pub(super) provider_fetcher: ProviderFetcher,
    pub(super) delivery_auth: Option<mmpf_auth::DeliveryAuth>,
    /// Per-pod cache of transcoded MLT tiles, keyed by (resource routing key,
    /// tile id). Populated lazily on first `.mlt` request; see
    /// `server::tileset::mlt`.
    pub(super) mlt_cache:
        moka::future::Cache<(ishikari_core::interned::ResourceRoutingKey, u64), bytes::Bytes>,
    /// Generated contour/hillshade MVTs. Async cache initialization single-flights
    /// the 3x3 source fetch and CPU generation for each derived tile.
    pub(super) derived_tile_cache: moka::future::Cache<
        super::tileset::terrain::DerivedTileKey,
        super::tileset::terrain::DerivedOutcome,
    >,
    /// Decoded Terrarium DEM tiles, shared across derived products and
    /// neighboring derived tiles (each 3x3 window overlaps its neighbors in six
    /// of nine sources), so each source tile is WebP-decoded roughly once.
    pub(super) dem_tile_cache: moka::future::Cache<
        (ishikari_core::interned::TilesetId, u64),
        Option<Arc<super::tileset::terrain::dem::DemTile>>,
    >,
    /// Coalesces Moka maintenance when multiple metrics collectors scrape at
    /// once. Followers may report the previous eventually-consistent size.
    cache_maintenance_running: Arc<AtomicBool>,
    pub(super) cpu_work_gate: CpuWorkGate,
    derived_negative_ttl: Duration,
    /// Mapterhorn composite resolver, when a composite tileset is configured.
    mapterhorn: Option<Arc<MapterhornResolver>>,
}

impl Deref for AppState {
    type Target = AppStateInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AppState {
    pub(crate) fn new(
        membership: Membership,
        metrics: NodeMetrics,
        resource_resolver: Arc<ResourceResolver>,
        drain: DrainController,
        provider: ProviderConfig,
        object_store_registry: Arc<ObjectStoreRegistry>,
        runtime: ServerRuntimeConfig,
    ) -> Self {
        let ServerRuntimeConfig {
            gossip_bootstrap_readiness,
            delivery_auth,
            mapterhorn,
            cpu_work_concurrency,
            cpu_work_max_inflight,
            derived_negative_ttl,
            cache_capacities,
        } = runtime;
        let provider_fetcher = ProviderFetcher::new(
            metrics.clone(),
            object_store_registry,
            cache_capacities.provider_bytes(),
        );
        Self(Arc::new(AppStateInner {
            membership,
            metrics,
            resource_resolver,
            drain,
            gossip_bootstrap_readiness,
            provider,
            provider_fetcher,
            delivery_auth,
            mapterhorn,
            // Bounded, byte-weighted: first `.mlt` request transcodes, the rest
            // hit this cache. 64 MiB ≈ a few hundred warm MLT tiles per pod.
            mlt_cache: moka::future::Cache::builder()
                .max_capacity(cache_capacities.mlt_bytes())
                .weigher(|_key, value: &bytes::Bytes| {
                    u32::try_from(value.len()).unwrap_or(u32::MAX)
                })
                .build(),
            derived_tile_cache: moka::future::Cache::builder()
                .max_capacity(cache_capacities.derived_tile_bytes())
                .weigher(
                    |_key: &super::tileset::terrain::DerivedTileKey,
                     value: &super::tileset::terrain::DerivedOutcome| {
                        match value {
                            super::tileset::terrain::DerivedOutcome::Tile(tile)
                            | super::tileset::terrain::DerivedOutcome::Degraded(tile) => {
                                u32::try_from(tile.bytes.len()).unwrap_or(u32::MAX)
                            }
                            super::tileset::terrain::DerivedOutcome::Absent => 1,
                        }
                    },
                )
                .expire_after(DerivedTileExpiry {
                    negative_ttl: derived_negative_ttl,
                })
                .build(),
            // Weighted by the actual decoded f32 allocation. Ishikari's
            // Mapterhorn loader rejects dimensions above the documented 512px
            // source contract, so one positive entry is about 1 MiB rather
            // than the generic decoder's 2048px / 16 MiB safety ceiling.
            dem_tile_cache: moka::future::Cache::builder()
                .max_capacity(cache_capacities.dem_tile_bytes())
                .weigher(
                    |_key: &(ishikari_core::interned::TilesetId, u64),
                     value: &Option<Arc<super::tileset::terrain::dem::DemTile>>| {
                        value.as_ref().map_or(1, |tile| {
                            u32::try_from(tile.byte_size()).unwrap_or(u32::MAX)
                        })
                    },
                )
                .expire_after(DecodedDemExpiry {
                    negative_ttl: derived_negative_ttl,
                })
                .build(),
            cache_maintenance_running: Arc::new(AtomicBool::new(false)),
            cpu_work_gate: CpuWorkGate::new(cpu_work_concurrency, cpu_work_max_inflight),
            derived_negative_ttl,
        }))
    }

    /// Per-pod transcoded-MLT cache, keyed by `(resource routing key, tile id)`.
    pub(crate) fn mlt_cache(
        &self,
    ) -> &moka::future::Cache<(ishikari_core::interned::ResourceRoutingKey, u64), bytes::Bytes>
    {
        &self.mlt_cache
    }

    /// The configured Mapterhorn composite resolver, if any.
    pub(crate) fn mapterhorn(&self) -> Option<&Arc<MapterhornResolver>> {
        self.mapterhorn.as_ref()
    }

    pub(crate) fn derived_tile_cache(
        &self,
    ) -> &moka::future::Cache<
        super::tileset::terrain::DerivedTileKey,
        super::tileset::terrain::DerivedOutcome,
    > {
        &self.derived_tile_cache
    }

    /// Decoded-DEM cache backing derived terrain generation.
    pub(crate) fn dem_tile_cache(
        &self,
    ) -> &moka::future::Cache<
        (ishikari_core::interned::TilesetId, u64),
        Option<Arc<super::tileset::terrain::dem::DemTile>>,
    > {
        &self.dem_tile_cache
    }

    pub(super) async fn admit_cpu_work(
        &self,
        work: &'static str,
    ) -> Result<CpuWorkPermit, HttpError> {
        self.cpu_work_gate.admit(&self.metrics, work).await
    }

    pub(crate) fn derived_negative_ttl(&self) -> Duration {
        self.derived_negative_ttl
    }

    pub(super) async fn is_gossip_bootstrap_ready(&self) -> bool {
        let observed = self.gossip_bootstrap_readiness.observe_with_logging(false);
        if observed.is_ready() {
            return true;
        }

        let observed = self
            .gossip_bootstrap_readiness
            .observe_with_logging(self.membership.has_other_live_node().await);
        observed.is_ready()
    }

    pub(super) fn try_start_cache_maintenance(&self) -> Option<CacheMaintenanceGuard> {
        self.cache_maintenance_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| CacheMaintenanceGuard {
                running: Arc::clone(&self.cache_maintenance_running),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        mem::size_of,
        sync::Arc,
        time::{Duration, Instant},
    };

    use moka::Expiry;

    use super::{AppState, AppStateInner, DecodedDemExpiry, DerivedTileExpiry};
    use crate::server::tileset::terrain::DerivedOutcome;

    #[test]
    fn app_state_is_a_single_shared_pointer() {
        assert_eq!(size_of::<AppState>(), size_of::<Arc<AppStateInner>>());
    }

    #[test]
    fn absent_and_transiently_degraded_derived_results_expire() {
        let expiry = DerivedTileExpiry {
            negative_ttl: Duration::from_secs(45),
        };
        let key = crate::server::tileset::terrain::DerivedTileKey::for_test();
        assert_eq!(
            expiry.expire_after_create(&key, &DerivedOutcome::Absent, Instant::now(),),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            expiry.expire_after_create(
                &key,
                &DerivedOutcome::Degraded(ishikari_core::pmtiles::TileData {
                    bytes: bytes::Bytes::new(),
                    content_type: "application/vnd.mapbox-vector-tile",
                    content_encoding: None,
                }),
                Instant::now(),
            ),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            expiry.expire_after_create(
                &key,
                &DerivedOutcome::Tile(ishikari_core::pmtiles::TileData {
                    bytes: bytes::Bytes::new(),
                    content_type: "application/vnd.mapbox-vector-tile",
                    content_encoding: None,
                }),
                Instant::now(),
            ),
            None
        );
    }

    #[test]
    fn absent_decoded_dems_expire() {
        let expiry = DecodedDemExpiry {
            negative_ttl: Duration::from_secs(30),
        };
        let key = (
            ishikari_core::interned::TilesetId::try_new("terrain").unwrap(),
            1,
        );
        assert_eq!(
            expiry.expire_after_create(&key, &None, Instant::now()),
            Some(Duration::from_secs(30))
        );
    }
}
