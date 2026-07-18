#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    biei_core::run().await
}
