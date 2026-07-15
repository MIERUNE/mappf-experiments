use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use anyhow::{Context, Result, ensure};
use ishikari::storage::BackendLatencyModel;
use serde::{Deserialize, Serialize};

/// Serializable object-store latency parameters used by the simulator.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct BackendLatencyConfig {
    #[serde(rename = "artificial_backend_delay_ms", alias = "median_ms")]
    pub median_ms: u64,
    #[serde(rename = "artificial_backend_delay_sigma", alias = "lognormal_sigma")]
    pub lognormal_sigma: f64,
    #[serde(
        rename = "artificial_backend_transfer_ms_per_mib",
        alias = "transfer_ms_per_mib"
    )]
    pub transfer_ms_per_mib: f64,
    #[serde(
        default = "default_seed",
        rename = "artificial_backend_delay_seed",
        alias = "seed"
    )]
    pub seed: u64,
}

impl BackendLatencyConfig {
    pub fn model_for_node(self, node_index: usize) -> Result<BackendLatencyModel> {
        BackendLatencyModel::lognormal(
            self.median_ms as f64,
            self.lognormal_sigma,
            self.transfer_ms_per_mib,
            self.seed.wrapping_add(node_index as u64),
        )
    }
}

impl Default for BackendLatencyConfig {
    fn default() -> Self {
        Self {
            median_ms: 0,
            lognormal_sigma: 0.0,
            transfer_ms_per_mib: 0.0,
            seed: default_seed(),
        }
    }
}

/// Versioned measurement document containing a simulator-ready fitted model.
#[derive(Debug, Clone, Deserialize)]
pub struct BackendLatencyProfile {
    schema_version: u32,
    pub model: BackendLatencyConfig,
}

impl BackendLatencyProfile {
    pub fn from_path(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("open backend latency profile {}", path.display()))?;
        Self::from_reader(BufReader::new(file))
            .with_context(|| format!("parse backend latency profile {}", path.display()))
    }

    fn from_reader(reader: impl Read) -> Result<Self> {
        let profile: Self = serde_json::from_reader(reader)?;
        ensure!(
            profile.schema_version == 1,
            "unsupported backend latency profile schema version {}",
            profile.schema_version
        );
        Ok(profile)
    }
}

const fn default_seed() -> u64 {
    1
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::BackendLatencyProfile;

    #[test]
    fn profile_ignores_measurement_metadata_and_defaults_seed() {
        let profile = BackendLatencyProfile::from_reader(Cursor::new(
            br#"{
                "schema_version": 1,
                "environment": { "region": "asia-northeast1" },
                "model": {
                    "median_ms": 55,
                    "lognormal_sigma": 0.9,
                    "transfer_ms_per_mib": 6.0
                }
            }"#,
        ))
        .expect("profile");

        assert_eq!(profile.model.median_ms, 55);
        assert_eq!(profile.model.seed, 1);
    }

    #[test]
    fn profile_rejects_unknown_schema_versions() {
        let error = BackendLatencyProfile::from_reader(Cursor::new(
            br#"{
                "schema_version": 2,
                "model": {
                    "median_ms": 55,
                    "lognormal_sigma": 0.9,
                    "transfer_ms_per_mib": 6.0
                }
            }"#,
        ))
        .expect_err("unsupported version");

        assert!(error.to_string().contains("schema version 2"));
    }
}
