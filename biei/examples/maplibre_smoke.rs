use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant as StdInstant;

use anyhow::{Context, bail};
use biei::renderer::ProfilePreparer;
use biei::renderer::Renderer;
use biei::renderer::actor::RendererActorConfig;
use biei::renderer::maplibre::{MapLibreProfilePreparer, MapLibreRenderer};
use biei::style_catalog::{StyleCatalog, StyleDefinition};
use biei::types::{
    ImageFormat, InternalTask, PixelRatio, Positioning, RenderRequest, Scale, StyleId,
    StyleRevision,
};
use tokio::time::Instant;

const DEFAULT_STYLE_ID: &str = "voyager-gl-style";
const DEFAULT_STYLE_URL: &str = "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse()?;
    let output_format = args.output_format()?;
    let style_id = StyleId(args.style_id.clone());
    let revision = StyleRevision {
        id: style_id.clone(),
        version: args.version,
    };
    let catalog = Arc::new(StyleCatalog::new());
    catalog.upsert_definition(
        style_id,
        StyleDefinition::new(args.style_url.clone(), revision.version),
    );

    let started = StdInstant::now();
    let mut renderer = MapLibreRenderer::spawn(RendererActorConfig {
        worker_id: 0,
        ambient_cache_path: Some(std::env::temp_dir().join("biei-maplibre-smoke-cache.sqlite")),
    })
    .context("spawn maplibre renderer")?;
    let preparer = MapLibreProfilePreparer::new(catalog, 1);
    let spawn_elapsed = started.elapsed();
    let task = args.task(revision, output_format);
    let setup_started = StdInstant::now();
    let prepared = preparer
        .prepare_profile(&task)
        .await
        .context("prepare profile")?;
    renderer
        .setup_profile(&task, prepared)
        .await
        .context("setup profile")?;
    let setup_elapsed = setup_started.elapsed();
    let mut rendered = None;
    let mut render_elapsed = Vec::with_capacity(args.repeat);
    for _ in 0..args.repeat {
        let render_started = StdInstant::now();
        rendered = Some(renderer.render(&task).await.context("render image")?);
        render_elapsed.push(render_started.elapsed());
    }
    let rendered = rendered.expect("repeat is at least 1");
    let write_started = StdInstant::now();
    std::fs::write(&args.output, &rendered.bytes)
        .with_context(|| format!("write {}", args.output.display()))?;
    let write_elapsed = write_started.elapsed();
    println!(
        "rendered {} bytes as {} to {} (spawn={:?}, setup={:?}, render_encode={:?}, write={:?})",
        rendered.bytes.len(),
        rendered.format.content_type(),
        args.output.display(),
        spawn_elapsed,
        setup_elapsed,
        render_elapsed,
        write_elapsed
    );
    Ok(())
}

#[derive(Debug)]
struct Args {
    style_id: String,
    style_url: String,
    version: u64,
    output: PathBuf,
    mode: Mode,
    scale: Scale,
    repeat: usize,
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Static,
    Tile,
}

impl Args {
    fn parse() -> anyhow::Result<Self> {
        let mut args = Self {
            style_id: DEFAULT_STYLE_ID.to_string(),
            style_url: DEFAULT_STYLE_URL.to_string(),
            version: 1,
            output: PathBuf::from("/private/tmp/biei-maplibre-smoke.png"),
            mode: Mode::Static,
            scale: Scale::X2,
            repeat: 1,
        };

        let mut iter = std::env::args().skip(1);
        while let Some(flag) = iter.next() {
            match flag.as_str() {
                "--style-id" => args.style_id = take_value(&mut iter, &flag)?,
                "--style-url" => args.style_url = take_value(&mut iter, &flag)?,
                "--version" => args.version = take_value(&mut iter, &flag)?.parse()?,
                "--output" => args.output = PathBuf::from(take_value(&mut iter, &flag)?),
                "--repeat" => args.repeat = take_value(&mut iter, &flag)?.parse()?,
                "--mode" => {
                    args.mode = match take_value(&mut iter, &flag)?.as_str() {
                        "static" => Mode::Static,
                        "tile" => Mode::Tile,
                        other => bail!("invalid --mode {other:?}; expected static or tile"),
                    };
                }
                "--scale" => {
                    args.scale = match take_value(&mut iter, &flag)?.as_str() {
                        "1x" => Scale::X1,
                        "2x" => Scale::X2,
                        other => bail!("invalid --scale {other:?}; expected 1x or 2x"),
                    };
                }
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => bail!("unknown argument {other:?}; use --help"),
            }
        }
        if args.repeat == 0 {
            bail!("--repeat must be at least 1");
        }
        Ok(args)
    }

    fn output_format(&self) -> anyhow::Result<ImageFormat> {
        match self
            .output
            .extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("png") => Ok(ImageFormat::Png),
            Some("webp") => Ok(ImageFormat::Webp),
            Some(ext) => bail!("unsupported output extension {ext:?}; use .png or .webp"),
            None => bail!("output path must have .png or .webp extension"),
        }
    }

    fn task(&self, style: StyleRevision, output_format: ImageFormat) -> InternalTask {
        let now = Instant::now();
        InternalTask {
            id: 1,
            request_id: biei::types::RequestId::from_string("maplibre-smoke"),
            style,
            source: None,
            request: match self.mode {
                Mode::Static => RenderRequest::StaticImage {
                    positioning: Positioning::Center {
                        lon: 139.767,
                        lat: 35.681,
                        zoom: 12.0,
                        bearing: 0.0,
                        pitch: 0.0,
                    },
                    width: 512,
                    height: 512,
                    overlays: Vec::new(),
                    before_layer: None,
                    padding: biei::types::Padding::default(),
                    addlayer: None,
                },
                Mode::Tile => RenderRequest::Tile {
                    z: 0,
                    x: 0,
                    y: 0,
                    tile_size: 512,
                },
            },
            pixel_ratio: PixelRatio::from(self.scale),
            output_format,
            arrived_at: now,
            deadline: now + Duration::from_secs(30),
            forwarding_hops: 0,
        }
    }
}

fn take_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> anyhow::Result<String> {
    iter.next()
        .with_context(|| format!("{flag} requires a value"))
}

fn print_help() {
    println!(
        "Usage: cargo run -p biei --example maplibre_smoke -- [options]\n\
         \n\
         Options:\n\
           --style-id <id>       Default: {DEFAULT_STYLE_ID}\n\
           --style-url <url>     Default: {DEFAULT_STYLE_URL}\n\
           --version <n>         Default: 1\n\
           --output <path>       Default: /private/tmp/biei-maplibre-smoke.png\n\
           --repeat <n>          Default: 1\n\
           --mode <static|tile>  Default: static\n\
           --scale <1x|2x>       Default: 2x"
    );
}
