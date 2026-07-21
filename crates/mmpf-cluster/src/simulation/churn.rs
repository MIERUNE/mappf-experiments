use std::{fs::File, io::BufReader, path::Path};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

/// A request-ordered sequence of simulated cluster membership changes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChurnPlan {
    pub events: Vec<ChurnEvent>,
}

impl ChurnPlan {
    /// Creates an event-free plan.
    pub fn empty() -> Self {
        Self { events: Vec::new() }
    }

    /// Loads, parses, and validates a churn plan from JSON.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("open churn plan {}", path.display()))?;
        let plan: Self = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parse churn plan {}", path.display()))?;
        plan.validate()?;
        Ok(plan)
    }

    /// Validates that events are ordered by nondecreasing request number.
    pub fn validate(&self) -> Result<()> {
        let mut previous = 0;
        for (index, event) in self.events.iter().enumerate() {
            ensure!(
                index == 0 || event.at_request() >= previous,
                "churn events must be ordered by at_request"
            );
            previous = event.at_request();
        }
        Ok(())
    }
}

/// A simulated cluster membership change tagged by its JSON `action`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ChurnEvent {
    Add { at_request: u64 },
    Remove { at_request: u64, node_id: String },
}

impl ChurnEvent {
    pub fn at_request(&self) -> u64 {
        match self {
            Self::Add { at_request } | Self::Remove { at_request, .. } => *at_request,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use serde_json::json;

    use super::{ChurnEvent, ChurnPlan};

    static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(contents: &str) -> Self {
            let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mmpf-cluster-churn-{}-{id}.json",
                std::process::id()
            ));
            fs::write(&path, contents).expect("write churn plan test file");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn preserves_flat_tagged_json_contract() {
        let plan: ChurnPlan = serde_json::from_value(json!({
            "events": [
                {"at_request": 10, "action": "add"},
                {
                    "at_request": 20,
                    "action": "remove",
                    "node_id": "node-0"
                }
            ],
            "accepted_unknown_plan_field": true
        }))
        .expect("parse churn plan");

        assert!(matches!(plan.events[0], ChurnEvent::Add { at_request: 10 }));
        assert!(matches!(
            &plan.events[1],
            ChurnEvent::Remove {
                at_request: 20,
                node_id
            } if node_id == "node-0"
        ));
        assert_eq!(
            serde_json::to_value(&plan).expect("serialize churn plan"),
            json!({
                "events": [
                    {"action": "add", "at_request": 10},
                    {
                        "action": "remove",
                        "at_request": 20,
                        "node_id": "node-0"
                    }
                ]
            })
        );
    }

    #[test]
    fn validates_nondecreasing_request_order_and_exposes_request() {
        let plan = ChurnPlan {
            events: vec![
                ChurnEvent::Add { at_request: 10 },
                ChurnEvent::Remove {
                    at_request: 10,
                    node_id: "node-0".to_string(),
                },
            ],
        };

        plan.validate().expect("equal request positions are valid");
        assert_eq!(plan.events[0].at_request(), 10);
        assert_eq!(plan.events[1].at_request(), 10);

        let unordered = ChurnPlan {
            events: vec![
                ChurnEvent::Add { at_request: 20 },
                ChurnEvent::Add { at_request: 10 },
            ],
        };
        assert_eq!(
            unordered
                .validate()
                .expect_err("unordered events must fail")
                .to_string(),
            "churn events must be ordered by at_request"
        );
    }

    #[test]
    fn from_path_loads_parses_and_validates_with_existing_errors() {
        let valid = TestFile::new(
            r#"{"events":[{"at_request":0,"action":"add"},{"at_request":0,"action":"remove","node_id":"node-0"}]}"#,
        );
        let plan = ChurnPlan::from_path(valid.path()).expect("load valid churn plan");
        assert_eq!(plan.events.len(), 2);

        let unordered = TestFile::new(
            r#"{"events":[{"at_request":2,"action":"add"},{"at_request":1,"action":"add"}]}"#,
        );
        assert_eq!(
            ChurnPlan::from_path(unordered.path())
                .expect_err("unordered file must fail")
                .to_string(),
            "churn events must be ordered by at_request"
        );

        let malformed = TestFile::new(r#"{"events":[{"action":"unknown","at_request":0}]}"#);
        assert_eq!(
            ChurnPlan::from_path(malformed.path())
                .expect_err("unknown action must fail")
                .to_string(),
            format!("parse churn plan {}", malformed.path().display())
        );

        let missing = std::env::temp_dir().join(format!(
            "mmpf-cluster-missing-churn-{}.json",
            std::process::id()
        ));
        let _ = fs::remove_file(&missing);
        assert_eq!(
            ChurnPlan::from_path(&missing)
                .expect_err("missing file must fail")
                .to_string(),
            format!("open churn plan {}", missing.display())
        );
    }
}
