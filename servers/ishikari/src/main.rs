#![deny(unreachable_pub)]

mod app;
mod cli;
mod drain;
mod http_client;
mod internal_transport;
mod mapterhorn;
mod membership;
mod options;
mod provider;
mod request_id;
mod runtime;
mod server;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    app::run().await
}
