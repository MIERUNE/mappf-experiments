#![deny(unreachable_pub)]

mod cli;
mod notifier;
mod operations;
mod server;
#[cfg(test)]
mod test_http;

use std::io::Read as _;

use anyhow::ensure;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    init_tracing()?;
    run()
}

#[tokio::main(flavor = "multi_thread")]
async fn run() -> anyhow::Result<()> {
    match cli::load() {
        cli::Command::Serve(args) => {
            let cli::ServeArgs {
                http_addr,
                auth_root,
                state_root,
                journal_root,
                style_catalog,
                style_refresh_endpoints,
                operational_status_endpoints,
            } = *args;
            let options: Vec<_> = std::env::vars().collect();
            let auth = if let Some(root) = auth_root {
                let auth = abashiri_core::auth::ObjectStoreManagementAuth::from_url(
                    &root,
                    options.clone(),
                )?;
                auth.prime().await?;
                tracing::info!(%root, "Abashiri object-store management auth loaded");
                Some(auth)
            } else {
                tracing::warn!(
                    "Abashiri management auth is disabled; only health endpoints are available"
                );
                None
            };
            let publishing = match (state_root, journal_root, style_catalog) {
                (Some(state_root), Some(journal_root), Some(catalog_url)) => {
                    ensure!(auth.is_some(), "style publication requires --auth-root");
                    let catalog = abashiri_core::catalog::StyleCatalog::from_url(
                        &catalog_url,
                        options.clone(),
                    )
                    .await?;
                    let publisher = abashiri_core::style::StylePublisher::from_urls(
                        &state_root,
                        &journal_root,
                        options,
                    )?;
                    let notifier = (!style_refresh_endpoints.is_empty())
                        .then(|| notifier::StyleRefreshNotifier::new(style_refresh_endpoints))
                        .transpose()?;
                    tracing::info!(
                        %state_root,
                        %journal_root,
                        %catalog_url,
                        styles = catalog.len(),
                        "Abashiri style publication enabled"
                    );
                    Some(server::StylePublishing::new(catalog, publisher, notifier)?)
                }
                (None, None, None) => {
                    ensure!(
                        style_refresh_endpoints.is_empty(),
                        "style refresh endpoints require style publication"
                    );
                    None
                }
                _ => {
                    anyhow::bail!(
                        "--state-root, --journal-root, and --style-catalog must be configured together"
                    )
                }
            };
            let operations = if operational_status_endpoints.is_empty() {
                None
            } else {
                ensure!(
                    auth.is_some(),
                    "operational status aggregation requires --auth-root"
                );
                Some(operations::OperationalStatusClient::new(
                    operational_status_endpoints,
                )?)
            };
            server::serve(http_addr, auth, publishing, operations).await
        }
        cli::Command::CheckStorage { root, cleanup } => {
            let outcome =
                abashiri_core::storage::check_backend(&root, std::env::vars(), cleanup).await?;
            if outcome.cleaned_up() {
                println!("Abashiri object-store capabilities verified at {root}; probe deleted");
            } else {
                println!(
                    "Abashiri object-store capabilities verified at {root}; probe object {} retained for lifecycle expiry",
                    outcome.location()
                );
            }
            Ok(())
        }
        cli::Command::HashCredential => {
            let mut input = String::new();
            std::io::stdin().take(4_097).read_to_string(&mut input)?;
            let credential = input.trim_end_matches(['\r', '\n']);
            ensure!(
                !credential.contains(['\r', '\n']),
                "stdin must contain exactly one management credential"
            );
            println!("{}", abashiri_core::auth::credential_sha256(credential)?);
            Ok(())
        }
    }
}

fn init_tracing() -> anyhow::Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("abashiri=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))
}
