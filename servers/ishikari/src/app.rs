use std::io::IsTerminal;

use anyhow::Result;
use tracing_subscriber::{EnvFilter, fmt};

use crate::cli;

pub(crate) async fn run() -> Result<()> {
    init_tracing();
    let options = cli::load()?;
    let auth = mmpf_auth::DeliveryAuth::new(options.auth_registries.clone(), std::env::vars());
    crate::runtime::run(options, auth, mmpf_http::serve::wait_for_shutdown_signal()).await
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("ishikari=info,ishikari_core=info"));
    let use_ansi = std::io::stdout().is_terminal();
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_ansi(use_ansi)
        .compact()
        .try_init();
}
