use std::{fs, path::Path};

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use crate::report::REPORT_SCHEMA_VERSION;

const TEMPLATE: &str = include_str!("visualization.html");
const REPORT_PLACEHOLDER: &str = "__BIEI_REPORT_JSON__";

pub fn write_visualization(input: impl AsRef<Path>, output: impl AsRef<Path>) -> Result<()> {
    let input = input.as_ref();
    let output = output.as_ref();
    let bytes = fs::read(input).with_context(|| format!("read report {}", input.display()))?;
    let report: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse report {}", input.display()))?;
    fs::write(output, render_visualization(&report)?)
        .with_context(|| format!("write visualization {}", output.display()))
}

pub fn render_visualization(report: &Value) -> Result<String> {
    ensure!(
        report.get("schema_version").and_then(Value::as_u64)
            == Some(u64::from(REPORT_SCHEMA_VERSION)),
        "unsupported or missing report schema version"
    );
    ensure!(report.get("result").is_some(), "report is missing result");
    ensure!(report.get("config").is_some(), "report is missing config");
    let embedded = serde_json::to_string(report)
        .context("serialize report for visualization")?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    Ok(TEMPLATE.replace(REPORT_PLACEHOLDER, &embedded))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render_visualization;
    use crate::report::REPORT_SCHEMA_VERSION;

    #[test]
    fn embeds_report_without_script_termination() {
        let report = json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "config": {"label": "</script><script>alert(1)</script>"},
            "result": {"total": 1},
        });
        let html = render_visualization(&report).expect("visualization");
        assert!(html.contains("Biei Simulator"));
        assert!(!html.contains("</script><script>alert(1)</script>"));
        assert!(html.contains("\\u003c/script\\u003e"));
    }

    #[test]
    fn rejects_unrelated_json() {
        assert!(render_visualization(&json!({"requests": 1})).is_err());
    }

    #[test]
    fn rejects_unknown_schema_version() {
        assert!(
            render_visualization(&json!({
                "schema_version": 999,
                "config": {},
                "result": {}
            }))
            .is_err()
        );
    }
}
