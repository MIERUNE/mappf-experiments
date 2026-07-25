#![deny(unreachable_pub)]

mod app;
mod cli;
mod config_file;
mod drain;
mod http_client;
mod internal_transport;
mod mapterhorn;
mod membership;
mod options;
mod provider;
mod request_id;
mod runtime;
#[cfg(feature = "unstable-schemas")]
mod schemas;
mod server;

fn main() -> anyhow::Result<()> {
    // Schema emission is build tooling: it is synchronous, so it answers before
    // any async runtime or runtime configuration exists. The whole module is
    // absent from a served build, so neither schema crate is linked.
    #[cfg(feature = "unstable-schemas")]
    if let Some(result) = schemas::emit_if_requested() {
        return result;
    }
    serve()
}

#[tokio::main(flavor = "multi_thread")]
async fn serve() -> anyhow::Result<()> {
    app::run().await
}
