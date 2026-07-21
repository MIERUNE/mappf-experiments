#![deny(unreachable_pub)]

mod app;
mod cli;
mod drain;
mod http;
mod membership;
mod options;
mod renderer;
mod runtime;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    app::run().await
}
