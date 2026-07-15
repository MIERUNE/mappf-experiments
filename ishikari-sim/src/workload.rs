use std::{collections::HashSet, io::Read, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};

const MESH_DEGREES: f64 = 1.0 / 80.0;
const HALF_MESH_DEGREES: f64 = MESH_DEGREES / 2.0;
const MAX_WEB_MERCATOR_LAT: f64 = 85.051_128_78;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryAffinity {
    #[default]
    PerRequest,
    PerSession,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadConfig {
    pub tileset: String,
    pub users: usize,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub focus_zoom: f64,
    pub zoom_sigma: f64,
    pub session_reset_probability: f64,
    pub move_step_tiles: f64,
    pub seed: u64,
    pub node_count: usize,
    pub entry_affinity: EntryAffinity,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            tileset: "mierune/omt".to_string(),
            users: 50,
            min_zoom: 4,
            max_zoom: 15,
            focus_zoom: 13.0,
            zoom_sigma: 1.8,
            session_reset_probability: 0.07,
            move_step_tiles: 1.0,
            seed: 1,
            node_count: 0,
            entry_affinity: EntryAffinity::PerRequest,
        }
    }
}

impl WorkloadConfig {
    fn validate(&self) -> Result<()> {
        ensure!(self.users > 0, "users must be greater than zero");
        ensure!(self.min_zoom <= self.max_zoom, "min_zoom exceeds max_zoom");
        ensure!(self.max_zoom <= 30, "max_zoom must not exceed 30");
        ensure!(self.focus_zoom.is_finite(), "focus_zoom must be finite");
        ensure!(
            self.zoom_sigma.is_finite() && self.zoom_sigma > 0.0,
            "zoom_sigma must be finite and greater than zero"
        );
        ensure!(
            (0.0..=1.0).contains(&self.session_reset_probability),
            "session_reset_probability must be between 0 and 1"
        );
        ensure!(
            self.move_step_tiles.is_finite() && self.move_step_tiles >= 0.0,
            "move_step_tiles must be finite and non-negative"
        );
        ensure!(!self.tileset.is_empty(), "tileset must not be empty");
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TraceEntry {
    pub step: u64,
    pub user: usize,
    pub ordinal: usize,
    pub tileset: String,
    pub z: u8,
    pub x: u32,
    pub y: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_node: Option<usize>,
}

#[derive(Debug, Clone)]
struct PopulationPoint {
    lng: f64,
    lat: f64,
    cumulative_weight: f64,
}

/// Positive-population point distribution loaded from the census GeoJSON.
#[derive(Debug, Clone)]
pub struct PopulationCdf {
    points: Vec<PopulationPoint>,
    total_weight: f64,
}

#[derive(Deserialize)]
struct FeatureCollection {
    #[serde(rename = "type")]
    kind: String,
    features: Vec<Feature>,
}

#[derive(Deserialize)]
struct Feature {
    geometry: Option<Geometry>,
    properties: Option<Properties>,
}

#[derive(Deserialize)]
struct Geometry {
    #[serde(rename = "type")]
    kind: String,
    coordinates: Vec<f64>,
}

#[derive(Deserialize)]
struct Properties {
    population: Option<f64>,
}

impl PopulationCdf {
    pub fn from_reader(reader: impl Read) -> Result<Self> {
        let collection: FeatureCollection =
            serde_json::from_reader(reader).context("parse census GeoJSON")?;
        ensure!(
            collection.kind == "FeatureCollection",
            "census GeoJSON is not a FeatureCollection"
        );

        let mut points = Vec::new();
        let mut cumulative_weight = 0.0;
        for feature in collection.features {
            let (Some(geometry), Some(properties)) = (feature.geometry, feature.properties) else {
                continue;
            };
            if geometry.kind != "Point" || geometry.coordinates.len() < 2 {
                continue;
            }
            let Some(population) = properties.population else {
                continue;
            };
            let lng = geometry.coordinates[0];
            let lat = geometry.coordinates[1];
            if !lng.is_finite() || !lat.is_finite() || !population.is_finite() || population <= 0.0
            {
                continue;
            }
            cumulative_weight += population;
            points.push(PopulationPoint {
                lng,
                lat,
                cumulative_weight,
            });
        }
        if points.is_empty() {
            bail!("census GeoJSON contains no positive-population points");
        }
        Ok(Self {
            points,
            total_weight: cumulative_weight,
        })
    }

    pub fn point_count(&self) -> usize {
        self.points.len()
    }

    pub fn total_weight(&self) -> f64 {
        self.total_weight
    }

    fn sample(&self, rng: &mut StdRng) -> (f64, f64) {
        let target = rng.random::<f64>() * self.total_weight;
        let index = self
            .points
            .partition_point(|point| point.cumulative_weight <= target)
            .min(self.points.len() - 1);
        let point = &self.points[index];
        let lng = wrap_lng(point.lng + random_signed(rng) * HALF_MESH_DEGREES);
        let lat = (point.lat + random_signed(rng) * HALF_MESH_DEGREES)
            .clamp(-MAX_WEB_MERCATOR_LAT, MAX_WEB_MERCATOR_LAT);
        (lng, lat)
    }
}

struct UserState {
    rng: StdRng,
    position: Option<(f64, f64, u8)>,
    previous_viewport: HashSet<(u8, u32, u32)>,
}

/// Stateful, deterministic population-driven viewport workload.
pub struct Workload {
    config: WorkloadConfig,
    population: Arc<PopulationCdf>,
    zoom_cdf: Vec<(u8, f64)>,
    zoom_total: f64,
    users: Vec<UserState>,
    request_index: u64,
}

impl Workload {
    pub fn new(config: WorkloadConfig, population: Arc<PopulationCdf>) -> Result<Self> {
        config.validate()?;
        let sigma = config.zoom_sigma;
        let mut zoom_cdf = Vec::new();
        let mut zoom_total = 0.0;
        for zoom in config.min_zoom..=config.max_zoom {
            let distance = f64::from(zoom) - config.focus_zoom;
            zoom_total += (-(distance * distance) / (2.0 * sigma * sigma)).exp();
            zoom_cdf.push((zoom, zoom_total));
        }
        let users = (0..config.users)
            .map(|user| UserState {
                rng: StdRng::seed_from_u64(derive_user_seed(config.seed, user as u64)),
                position: None,
                previous_viewport: HashSet::with_capacity(9),
            })
            .collect();
        Ok(Self {
            config,
            population,
            zoom_cdf,
            zoom_total,
            users,
            request_index: 0,
        })
    }

    /// Advances one user by one viewport step and returns only newly visible tiles.
    pub fn step(&mut self, step: u64, user: usize) -> Result<Vec<TraceEntry>> {
        let seed = self.config.seed;
        let node_count = self.config.node_count;
        let entry_affinity = self.config.entry_affinity;
        let tileset = self.config.tileset.clone();
        let mut request_index = self.request_index;
        let Some(state) = self.users.get_mut(user) else {
            bail!("user index {user} is out of range");
        };
        let reset = state.position.is_none()
            || state.rng.random::<f64>() < self.config.session_reset_probability;
        let (lng, lat, zoom) = if reset {
            state.previous_viewport.clear();
            let (lng, lat) = self.population.sample(&mut state.rng);
            let zoom = sample_zoom(&self.zoom_cdf, self.zoom_total, &mut state.rng);
            (lng, lat, zoom)
        } else {
            let (lng, lat, zoom) = state.position.expect("position checked above");
            let (mercator_x, mercator_y) = lng_lat_to_web_mercator(lng, lat);
            let move_step = self.config.move_step_tiles / f64::from(1_u32 << zoom);
            let moved_x = wrap_unit(mercator_x + random_signed(&mut state.rng) * move_step);
            let moved_y = (mercator_y + random_signed(&mut state.rng) * move_step).clamp(0.0, 1.0);
            let (lng, lat) = web_mercator_to_lng_lat(moved_x, moved_y);
            (lng, lat, zoom)
        };
        state.position = Some((lng, lat, zoom));

        let (center_x, center_y, dimension) = lng_lat_to_tile(lng, lat, zoom);
        let mut current_viewport = HashSet::with_capacity(9);
        let mut entries = Vec::with_capacity(9);
        for dy in -1_i64..=1 {
            for dx in -1_i64..=1 {
                let x = (i64::from(center_x) + dx).rem_euclid(i64::from(dimension)) as u32;
                let y = (i64::from(center_y) + dy).clamp(0, i64::from(dimension - 1)) as u32;
                let tile = (zoom, x, y);
                current_viewport.insert(tile);
                if state.previous_viewport.contains(&tile) {
                    continue;
                }
                let entry_node =
                    select_entry_node(seed, node_count, entry_affinity, user, request_index);
                entries.push(TraceEntry {
                    step,
                    user,
                    ordinal: entries.len(),
                    tileset: tileset.clone(),
                    z: zoom,
                    x,
                    y,
                    entry_node,
                });
                request_index += 1;
            }
        }
        state.previous_viewport = current_viewport;
        self.request_index = request_index;
        Ok(entries)
    }
}

fn select_entry_node(
    seed: u64,
    node_count: usize,
    affinity: EntryAffinity,
    user: usize,
    request_index: u64,
) -> Option<usize> {
    if node_count == 0 {
        return None;
    }
    let discriminator = match affinity {
        EntryAffinity::PerRequest => request_index,
        EntryAffinity::PerSession => user as u64,
    };
    Some((splitmix64(seed ^ discriminator) % node_count as u64) as usize)
}

fn sample_zoom(cdf: &[(u8, f64)], total: f64, rng: &mut StdRng) -> u8 {
    let target = rng.random::<f64>() * total;
    let index = cdf.partition_point(|(_, cumulative)| *cumulative <= target);
    cdf[index.min(cdf.len() - 1)].0
}

fn lng_lat_to_tile(lng: f64, lat: f64, zoom: u8) -> (u32, u32, u32) {
    let (mercator_x, mercator_y) = lng_lat_to_web_mercator(lng, lat);
    let dimension = 1_u32 << zoom;
    let x = ((mercator_x * f64::from(dimension)).floor() as i64).rem_euclid(i64::from(dimension))
        as u32;
    let y = (mercator_y * f64::from(dimension)).floor() as i64;
    (x, y.clamp(0, i64::from(dimension - 1)) as u32, dimension)
}

fn lng_lat_to_web_mercator(lng: f64, lat: f64) -> (f64, f64) {
    let latitude = lat.clamp(-MAX_WEB_MERCATOR_LAT, MAX_WEB_MERCATOR_LAT);
    let x = ((lng + 180.0) / 360.0).clamp(0.0, 1.0);
    let latitude_radians = latitude.to_radians();
    let y = (1.0
        - (latitude_radians.tan() + 1.0 / latitude_radians.cos()).ln() / std::f64::consts::PI)
        / 2.0;
    (x, y.clamp(0.0, 1.0))
}

fn web_mercator_to_lng_lat(x: f64, y: f64) -> (f64, f64) {
    let lng = wrap_unit(x) * 360.0 - 180.0;
    let n = std::f64::consts::PI * (1.0 - 2.0 * y.clamp(0.0, 1.0));
    let lat = n.sinh().atan().to_degrees();
    (lng, lat.clamp(-MAX_WEB_MERCATOR_LAT, MAX_WEB_MERCATOR_LAT))
}

fn random_signed(rng: &mut StdRng) -> f64 {
    rng.random::<f64>() * 2.0 - 1.0
}

fn wrap_lng(lng: f64) -> f64 {
    (lng + 180.0).rem_euclid(360.0) - 180.0
}

fn wrap_unit(value: f64) -> f64 {
    value.rem_euclid(1.0)
}

fn derive_user_seed(seed: u64, user: u64) -> u64 {
    splitmix64(seed ^ user.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use super::{EntryAffinity, PopulationCdf, Workload, WorkloadConfig};

    const CENSUS: &str = r#"{
        "type":"FeatureCollection",
        "features":[
            {"geometry":{"type":"Point","coordinates":[139.75,35.68]},"properties":{"population":100}},
            {"geometry":{"type":"Point","coordinates":[141.35,43.06]},"properties":{"population":0}},
            {"geometry":{"type":"LineString","coordinates":[0,0]},"properties":{"population":200}}
        ]
    }"#;

    fn population() -> Arc<PopulationCdf> {
        Arc::new(PopulationCdf::from_reader(Cursor::new(CENSUS)).expect("population CDF"))
    }

    #[test]
    fn census_loader_keeps_only_positive_point_features() {
        let population = population();

        assert_eq!(population.point_count(), 1);
        assert_eq!(population.total_weight(), 100.0);
    }

    #[test]
    fn rejects_invalid_zoom_distributions() {
        for config in [
            WorkloadConfig {
                focus_zoom: f64::NAN,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                zoom_sigma: 0.0,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                zoom_sigma: f64::INFINITY,
                ..WorkloadConfig::default()
            },
        ] {
            assert!(Workload::new(config, population()).is_err());
        }
    }

    #[test]
    fn workload_is_reproducible_and_first_viewport_has_nine_tiles() {
        let config = WorkloadConfig {
            users: 2,
            node_count: 3,
            entry_affinity: EntryAffinity::PerRequest,
            ..WorkloadConfig::default()
        };
        let mut first = Workload::new(config.clone(), population()).expect("first workload");
        let mut second = Workload::new(config, population()).expect("second workload");

        let first_entries = first.step(0, 0).expect("first step");
        let second_entries = second.step(0, 0).expect("second step");

        assert_eq!(first_entries.len(), 9);
        assert_eq!(first_entries, second_entries);
        assert!(
            first_entries
                .iter()
                .all(|entry| entry.entry_node.is_some_and(|node| node < 3))
        );
    }

    #[test]
    fn unchanged_viewport_emits_no_duplicate_requests() {
        let config = WorkloadConfig {
            users: 1,
            min_zoom: 10,
            max_zoom: 10,
            session_reset_probability: 0.0,
            move_step_tiles: 0.0,
            ..WorkloadConfig::default()
        };
        let mut workload = Workload::new(config, population()).expect("workload");

        assert_eq!(workload.step(0, 0).expect("initial step").len(), 9);
        assert!(workload.step(1, 0).expect("stationary step").is_empty());
    }

    #[test]
    fn per_session_entry_affinity_is_stable() {
        let config = WorkloadConfig {
            users: 1,
            min_zoom: 10,
            max_zoom: 10,
            session_reset_probability: 1.0,
            node_count: 5,
            entry_affinity: EntryAffinity::PerSession,
            ..WorkloadConfig::default()
        };
        let mut workload = Workload::new(config, population()).expect("workload");

        let first = workload.step(0, 0).expect("first step");
        let second = workload.step(1, 0).expect("second step");
        let expected = first[0].entry_node;

        assert!(
            first
                .iter()
                .chain(&second)
                .all(|entry| entry.entry_node == expected)
        );
    }
}
