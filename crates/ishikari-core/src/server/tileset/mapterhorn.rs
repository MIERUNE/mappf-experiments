//! Mapterhorn composite tileset resolution.
//!
//! Mapterhorn ships a base `planet.pmtiles` covering z0–12 plus optional
//! per-region detail archives keyed by the z6 ancestor tile
//! (`6-{x6}-{y6}.pmtiles`) covering z13+ where high-resolution data exists.
//!
//! This thin preset makes one logical tileset (e.g. `mapterhorn/planet`) serve
//! both: z<=12 reads the base archive, z>12 is rewritten onto the matching
//! detail archive. The rewrite happens before routing, so the normal PMTiles
//! read, chunk cache, peer routing, and range batching all apply unchanged —
//! the detail archive is just another tileset key.
//!
//! Detail archives are sparse. Rather than ship a manifest, we probe object
//! storage for a detail archive's presence on first use and cache the result
//! (present or absent) per z6 tile. The probe is single-flighted: concurrent
//! z13+ requests for the same cold archive coalesce onto one object-store
//! lookup, and absent regions are then served from cache for `negative_ttl`
//! instead of re-probed. Transient probe errors are not cached. A z12 parent is
//! never returned for a z13+ URL (that would be spatially wrong); a missing
//! detail archive yields a plain 404.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};

use crate::interned::TilesetId;
use crate::storage::TilesetError;

/// Highest zoom served from the base archive; higher zooms use detail archives.
const BASE_MAX_ZOOM: u8 = 12;
/// Detail archives are keyed by the z6 ancestor of the requested tile.
const DETAIL_ANCESTOR_ZOOM: u8 = 6;
/// Upper bound for an advertised max zoom (PMTiles tops out at 31; TileJSON
/// consumers never need more for terrain).
const MAX_ADVERTISED_ZOOM: u8 = 30;

/// Which archive should back a given tile request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolved {
    /// Serve from the base archive (the logical tileset itself), z<=12.
    Base(TilesetId),
    /// Serve from this present detail archive, z>12.
    Detail(TilesetId),
    /// z>12 but the detail archive is absent: 404 (no z12 overzoom fallback).
    Absent,
}

/// Resolves Mapterhorn composite tile requests, single-flighting and caching
/// detail-archive presence.
#[derive(Clone)]
pub struct MapterhornResolver {
    /// Logical tileset key clients address (e.g. `mapterhorn/planet`).
    tileset: TilesetId,
    /// Object-store namespace the detail archives live under (the logical key's
    /// first path segment, e.g. `mapterhorn`).
    namespace: String,
    /// Zoom advertised in TileJSON so clients request the z13+ detail tiles.
    maxzoom: u8,
    /// Presence cache for detail archives: `true` = present, `false` = absent.
    /// `try_get_with` coalesces concurrent probes for the same key and skips
    /// caching transient errors.
    presence: moka::future::Cache<TilesetId, bool>,
}

impl MapterhornResolver {
    /// Builds a resolver for one composite tileset key (e.g.
    /// `mapterhorn/planet`). `maxzoom` is advertised in TileJSON and must be a
    /// detail zoom (>12); `negative_ttl` bounds how long a probed presence
    /// result (notably an absent archive) is cached.
    pub fn new(tileset: &str, maxzoom: u8, negative_ttl: Duration) -> Result<Self> {
        if !(BASE_MAX_ZOOM + 1..=MAX_ADVERTISED_ZOOM).contains(&maxzoom) {
            bail!(
                "mapterhorn maxzoom must be {}..={} (got {maxzoom}): it advertises the detail \
                 archives' zoom, so <= {BASE_MAX_ZOOM} would never request detail tiles",
                BASE_MAX_ZOOM + 1,
                MAX_ADVERTISED_ZOOM
            );
        }
        let tileset = TilesetId::try_new(tileset)?;
        let namespace = tileset
            .as_str()
            .split('/')
            .next()
            .unwrap_or_else(|| tileset.as_str())
            .to_string();
        let presence = moka::future::Cache::builder()
            .time_to_live(negative_ttl)
            // At most one entry per z6 tile (4096 globally); most stay empty.
            .max_capacity(4096)
            .build();
        Ok(Self {
            tileset,
            namespace,
            maxzoom,
            presence,
        })
    }

    /// Whether `tileset_id` is this composite tileset.
    pub(crate) fn matches(&self, tileset_id: &TilesetId) -> bool {
        tileset_id == &self.tileset
    }

    /// Max zoom to advertise for the composite tileset's TileJSON.
    pub(crate) fn maxzoom(&self) -> u8 {
        self.maxzoom
    }

    /// Resolves which archive backs `(z, x, y)`. For z>12, `probe` is invoked at
    /// most once per z6 tile per TTL to test detail-archive presence (it should
    /// return `Ok(true)` if present, `Ok(false)` if absent, `Err` on a transient
    /// failure); concurrent callers for the same archive share one probe.
    pub(crate) async fn resolve<F, Fut>(
        &self,
        z: u8,
        x: u32,
        y: u32,
        probe: F,
    ) -> Result<Resolved, Arc<TilesetError>>
    where
        F: FnOnce(TilesetId) -> Fut,
        Fut: Future<Output = Result<bool, TilesetError>>,
    {
        if z <= BASE_MAX_ZOOM {
            return Ok(Resolved::Base(self.tileset.clone()));
        }
        let detail = self.detail_tileset(z, x, y);
        let present = self
            .presence
            .try_get_with(detail.clone(), {
                let detail = detail.clone();
                async move { probe(detail).await }
            })
            .await?;
        Ok(if present {
            Resolved::Detail(detail)
        } else {
            Resolved::Absent
        })
    }

    /// Detail archive key for a z>12 tile: `{namespace}/6-{x6}-{y6}`, where the
    /// z6 ancestor is `x >> (z - 6)`, `y >> (z - 6)`.
    fn detail_tileset(&self, z: u8, x: u32, y: u32) -> TilesetId {
        let shift = z - DETAIL_ANCESTOR_ZOOM;
        let x6 = x >> shift;
        let y6 = y >> shift;
        TilesetId::new_unchecked(&format!("{}/6-{x6}-{y6}", self.namespace))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn resolver() -> MapterhornResolver {
        MapterhornResolver::new("mapterhorn/planet", 16, Duration::from_secs(3600)).unwrap()
    }

    // `try_get_with` requires `'static + Send` init futures, so probes own their
    // captures (an `Arc` counter) rather than borrowing locals — matching how
    // the real caller moves an `Arc<ResourceResolver>` into the probe.

    /// A probe that records its call count and returns a fixed presence result.
    fn counting_probe(
        calls: Arc<AtomicUsize>,
        present: bool,
    ) -> impl Fn(
        TilesetId,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<bool, TilesetError>> + Send>>
    + Clone {
        move |_| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(present)
            })
        }
    }

    #[test]
    fn maxzoom_must_be_a_detail_zoom() {
        assert!(MapterhornResolver::new("mapterhorn/planet", 12, Duration::from_secs(1)).is_err());
        assert!(MapterhornResolver::new("mapterhorn/planet", 13, Duration::from_secs(1)).is_ok());
        assert!(MapterhornResolver::new("mapterhorn/planet", 31, Duration::from_secs(1)).is_err());
    }

    #[tokio::test]
    async fn base_archive_serves_low_zoom_without_probing() {
        let r = resolver();
        let probe = |_| async { panic!("probe must not run for base-zoom tiles") };
        assert_eq!(
            r.resolve(12, 100, 200, probe).await.unwrap(),
            Resolved::Base(TilesetId::new_unchecked("mapterhorn/planet"))
        );
        assert_eq!(
            r.resolve(0, 0, 0, |_| async { panic!("no probe") })
                .await
                .unwrap(),
            Resolved::Base(TilesetId::new_unchecked("mapterhorn/planet"))
        );
    }

    #[test]
    fn detail_archive_uses_z6_ancestor() {
        let r = resolver();
        // z13: shift = 7, so x6 = x >> 7, y6 = y >> 7. (8000, 3000) -> (62, 23).
        assert_eq!(
            r.detail_tileset(13, 8000, 3000),
            TilesetId::new_unchecked("mapterhorn/6-62-23")
        );
        // A z13 tile and its z14 children share one z6 ancestor.
        assert_eq!(
            r.detail_tileset(13, 8000, 3000),
            r.detail_tileset(14, 16000, 6000)
        );
    }

    #[tokio::test]
    async fn present_detail_is_served_then_cached() {
        let r = resolver();
        let calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            r.resolve(13, 8000, 3000, counting_probe(calls.clone(), true))
                .await
                .unwrap(),
            Resolved::Detail(TilesetId::new_unchecked("mapterhorn/6-62-23"))
        );
        // Second request hits the presence cache, no second probe.
        assert_eq!(
            r.resolve(13, 8000, 3000, |_| async { panic!("should be cached") })
                .await
                .unwrap(),
            Resolved::Detail(TilesetId::new_unchecked("mapterhorn/6-62-23"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn absent_detail_is_negative_cached() {
        let r = resolver();
        let calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            r.resolve(13, 8000, 3000, counting_probe(calls.clone(), false))
                .await
                .unwrap(),
            Resolved::Absent
        );
        // Negative result is cached: no re-probe.
        assert_eq!(
            r.resolve(13, 8000, 3000, |_| async {
                panic!("absent should be cached")
            })
            .await
            .unwrap(),
            Resolved::Absent
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_probes_are_single_flighted() {
        let r = resolver();
        let calls = Arc::new(AtomicUsize::new(0));
        let probe = {
            let calls = calls.clone();
            move |_| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<_, TilesetError>(false)
                }
            }
        };
        // Two concurrent requests for the same cold archive coalesce onto one probe.
        let (a, b) = tokio::join!(
            r.resolve(13, 8000, 3000, probe.clone()),
            r.resolve(13, 8000, 3000, probe)
        );
        assert_eq!(a.unwrap(), Resolved::Absent);
        assert_eq!(b.unwrap(), Resolved::Absent);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "probe should run once");
    }

    #[tokio::test]
    async fn transient_probe_error_is_not_cached() {
        let r = resolver();
        let calls = Arc::new(AtomicUsize::new(0));
        let probe = {
            let calls = calls.clone();
            move |_| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err::<bool, _>(TilesetError::Timeout("backend timeout".into()))
                }
            }
        };
        assert!(r.resolve(13, 8000, 3000, probe.clone()).await.is_err());
        // The error was not cached: a retry probes again.
        assert!(r.resolve(13, 8000, 3000, probe).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn matches_only_the_configured_tileset() {
        let r = resolver();
        assert!(r.matches(&TilesetId::new_unchecked("mapterhorn/planet")));
        assert!(!r.matches(&TilesetId::new_unchecked("mierune/omt")));
        assert!(!r.matches(&TilesetId::new_unchecked("mapterhorn/6-0-0")));
    }
}
