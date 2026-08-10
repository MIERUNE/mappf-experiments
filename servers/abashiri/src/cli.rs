//! Abashiri's command-line and environment contract.

use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand};
use url::Url;

use crate::operations::OperationalStatusEndpoint;

#[derive(Debug, Parser)]
#[command(about = "MMPF management and publishing API")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Abashiri runtime and operator commands.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Serve the management API.
    Serve(Box<ServeArgs>),
    /// Verify conditional writes and required attributes on an object-store root.
    ///
    /// The probe creates a uniquely named object below
    /// `.abashiri-capability-check/`, verifies duplicate-create and stale-update
    /// rejection, required attribute persistence, and the successful update.
    /// By default the object is retained for lifecycle expiry. Some backends,
    /// including GCS, still require delete permission to replace an object.
    CheckStorage {
        /// Object-store URL used for the probe, for example
        /// `gs://bucket/control-plane/`.
        #[arg(long)]
        root: Url,
        /// Delete the probe object after checking. Use only with a diagnostic
        /// identity that already has delete permission.
        #[arg(long, default_value_t = false)]
        cleanup: bool,
    },
    /// Read one management credential from stdin and print its registry digest.
    HashCredential,
}

#[derive(Debug, Args)]
pub(crate) struct ServeArgs {
    /// Management HTTP listener. It defaults to loopback so an unfinished
    /// authentication boundary is not accidentally exposed.
    #[arg(long, env = "ABASHIRI_HTTP_ADDR", default_value = "127.0.0.1:8080")]
    pub(crate) http_addr: SocketAddr,
    /// Object-store directory containing the management-auth `current.json`.
    /// When omitted, the health-only server exposes no authenticated API.
    #[arg(long, env = "ABASHIRI_AUTH_ROOT")]
    pub(crate) auth_root: Option<Url>,
    /// Object-store root containing published delivery state.
    /// Its remote bucket or authority must differ from `--journal-root`.
    #[arg(long, env = "ABASHIRI_STATE_ROOT")]
    pub(crate) state_root: Option<Url>,
    /// Private object-store root containing mutation intents and completions.
    /// Its bucket must not be readable by delivery workloads.
    #[arg(long, env = "ABASHIRI_JOURNAL_ROOT")]
    pub(crate) journal_root: Option<Url>,
    /// URL of the trusted style-catalog JSON object. Required together with
    /// both publication roots; management credentials remain separately configured.
    #[arg(long, env = "ABASHIRI_STYLE_CATALOG")]
    pub(crate) style_catalog: Option<Url>,
    /// Biei or Ishikari internal style-refresh receiver. Repeat the option, or
    /// provide a comma-separated environment value, to notify both services.
    #[arg(
        long = "style-refresh-endpoint",
        env = "ABASHIRI_STYLE_REFRESH_ENDPOINTS",
        value_delimiter = ','
    )]
    pub(crate) style_refresh_endpoints: Vec<Url>,
    /// Named Biei or Ishikari operational status endpoint. Repeat as
    /// `<source-id>=<url>` to aggregate independently deployed services.
    #[arg(
        long = "operational-status-endpoint",
        env = "ABASHIRI_OPERATIONAL_STATUS_ENDPOINTS",
        value_delimiter = ','
    )]
    pub(crate) operational_status_endpoints: Vec<OperationalStatusEndpoint>,
}

pub(crate) fn load() -> Command {
    Cli::parse().command
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::*;

    #[test]
    fn management_listener_defaults_to_loopback() {
        let cli = Cli::try_parse_from(["abashiri", "serve"]).unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve command");
        };
        assert_eq!(args.http_addr, "127.0.0.1:8080".parse().unwrap());
        assert!(args.auth_root.is_none());
        assert!(args.state_root.is_none());
        assert!(args.journal_root.is_none());
        assert!(args.style_catalog.is_none());
        assert!(args.style_refresh_endpoints.is_empty());
        assert!(args.operational_status_endpoints.is_empty());
    }

    #[test]
    fn parses_management_auth_root() {
        let cli = Cli::try_parse_from([
            "abashiri",
            "serve",
            "--auth-root",
            "gs://example-control/auth/",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve command");
        };
        assert_eq!(
            args.auth_root.unwrap().as_str(),
            "gs://example-control/auth/"
        );
    }

    #[test]
    fn parses_style_publication_roots_and_catalog() {
        let cli = Cli::try_parse_from([
            "abashiri",
            "serve",
            "--state-root",
            "gs://example-delivery/state/",
            "--journal-root",
            "gs://example-control/journal/",
            "--style-catalog",
            "gs://example-control/catalog/current.json",
            "--style-refresh-endpoint",
            "http://biei:9090/_internal/refresh/style",
            "--style-refresh-endpoint",
            "http://ishikari:9090/_internal/refresh/style",
            "--operational-status-endpoint",
            "renderer=http://biei:9090/_internal/operations/v1/status",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve command");
        };
        assert_eq!(
            args.state_root.unwrap().as_str(),
            "gs://example-delivery/state/"
        );
        assert_eq!(
            args.journal_root.unwrap().as_str(),
            "gs://example-control/journal/"
        );
        assert_eq!(
            args.style_catalog.unwrap().as_str(),
            "gs://example-control/catalog/current.json"
        );
        assert_eq!(args.style_refresh_endpoints.len(), 2);
        assert_eq!(args.operational_status_endpoints.len(), 1);
    }

    #[test]
    fn parses_storage_root() {
        let cli = Cli::try_parse_from([
            "abashiri",
            "check-storage",
            "--root",
            "gs://example/control/",
        ])
        .unwrap();
        let Command::CheckStorage { root, cleanup } = cli.command else {
            panic!("expected check-storage command");
        };
        assert_eq!(root.as_str(), "gs://example/control/");
        assert!(!cleanup);
    }

    #[test]
    fn cleanup_is_explicit() {
        let cli = Cli::try_parse_from([
            "abashiri",
            "check-storage",
            "--root",
            "memory:///control",
            "--cleanup",
        ])
        .unwrap();
        let Command::CheckStorage { cleanup, .. } = cli.command else {
            panic!("expected check-storage command");
        };
        assert!(cleanup);
    }

    #[test]
    fn parses_hash_credential_command() {
        let cli = Cli::try_parse_from(["abashiri", "hash-credential"]).unwrap();
        assert!(matches!(cli.command, Command::HashCredential));
    }
}
