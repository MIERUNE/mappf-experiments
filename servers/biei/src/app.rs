use crate::cli;

pub(crate) async fn run() -> anyhow::Result<()> {
    init_tracing();
    let options = cli::load()?;
    crate::runtime::run(options, mmpf_http::serve::wait_for_shutdown_signal()).await
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Use FmtSubscriber directly. This avoids carrying span-enter guards from
    // the sharded Registry layer across await points.
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
