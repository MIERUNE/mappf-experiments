//! End-to-end stale-while-revalidate through the real MapLibre Native call
//! path: a stale Database response is delivered to the consumer, the paired
//! low-priority Network request revalidates in the background, a 304 refreshes
//! the shared cache, and — critically — the render never waits on the
//! revalidation round trip.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use maplibre_native::{CameraUpdate, ImageRendererBuilder, LatLng, Size};
use mmpf_mln_filesource::{FileSourceIoPermits, register_file_sources};

const ETAG: &str = "\"spr-v1\"";
/// The revalidation answer is delayed by this much. A render that waits on
/// revalidation cannot finish faster than this; a stale-while-revalidate
/// render can. Kept under the source's 2s request timeout so the delayed 304
/// still counts as a successful refresh.
const REVALIDATION_DELAY: Duration = Duration::from_millis(1_500);
const SPRITE_CACHE_CONTROL: &str = "public, s-maxage=2, stale-while-revalidate=600";

const PNG_1X1: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0xda, 0x63, 0xfc, 0xcf, 0xc0, 0x50,
    0x0f, 0x00, 0x04, 0x85, 0x01, 0x80, 0x84, 0xa9, 0x8c, 0x21, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
    0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
];

struct ServerCounts {
    sprite_full: AtomicUsize,
    sprite_not_modified: AtomicUsize,
    first_conditional_at: std::sync::Mutex<Option<Instant>>,
}

fn respond(stream: &mut TcpStream, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    let mut head = format!("HTTP/1.1 {status}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn spawn_origin(counts: Arc<ServerCounts>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("origin binds");
    let addr = listener.local_addr().expect("origin addr");
    let style = format!(
        r#"{{"version":8,"name":"swr-e2e","sprite":"http://{addr}/sprite","sources":{{}},"layers":[{{"id":"bg","type":"background","paint":{{"background-color":"navy"}}}}]}}"#,
    );
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let counts = Arc::clone(&counts);
            let style = style.clone();
            thread::spawn(move || {
                let mut buffer = [0u8; 4096];
                let mut request = Vec::new();
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split_whitespace().nth(1).unwrap_or("/").to_string();
                let revalidation = request.lines().any(|line| {
                    line.to_ascii_lowercase().starts_with("if-none-match")
                        && line.contains("spr-v1")
                });
                if path.starts_with("/style.json") {
                    respond(
                        &mut stream,
                        "200 OK",
                        &[
                            ("Content-Type", "application/json"),
                            ("Cache-Control", "public, s-maxage=600"),
                        ],
                        style.as_bytes(),
                    );
                } else if path.starts_with("/sprite") {
                    if revalidation {
                        counts
                            .first_conditional_at
                            .lock()
                            .unwrap()
                            .get_or_insert_with(Instant::now);
                        counts.sprite_not_modified.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(REVALIDATION_DELAY);
                        respond(
                            &mut stream,
                            "304 Not Modified",
                            &[("ETag", ETAG), ("Cache-Control", SPRITE_CACHE_CONTROL)],
                            b"",
                        );
                    } else {
                        counts.sprite_full.fetch_add(1, Ordering::SeqCst);
                        let (content_type, body): (&str, &[u8]) = if path.contains(".json") {
                            ("application/json", b"{}")
                        } else {
                            ("image/png", PNG_1X1)
                        };
                        respond(
                            &mut stream,
                            "200 OK",
                            &[
                                ("Content-Type", content_type),
                                ("ETag", ETAG),
                                ("Cache-Control", SPRITE_CACHE_CONTROL),
                            ],
                            body,
                        );
                    }
                } else {
                    respond(&mut stream, "404 Not Found", &[], b"");
                }
            });
        }
    });
    format!("http://{addr}/style.json")
}

/// A persistent renderer on its own thread, mirroring biei's long-lived
/// renderer actors. The paired background revalidations are owned by the
/// renderer's run loop, so a per-render throwaway renderer would cancel them
/// on drop and the shared cache would never refresh.
struct RendererHarness {
    commands: Option<mpsc::Sender<mpsc::Sender<()>>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for RendererHarness {
    fn drop(&mut self) {
        // Join the renderer thread so native teardown finishes before the
        // process exits; an abandoned mid-teardown C++ thread aborts.
        drop(self.commands.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl RendererHarness {
    fn spawn(style_url: &str) -> Self {
        let style_url = style_url.to_string();
        let (commands, command_rx) = mpsc::channel::<mpsc::Sender<()>>();
        let thread = thread::spawn(move || {
            let mut renderer = ImageRendererBuilder::new()
                .with_size(NonZeroU32::new(32).unwrap(), NonZeroU32::new(32).unwrap())
                .with_pixel_ratio(1.0)
                .build_static_renderer();
            let url: url::Url = style_url.parse().unwrap();
            renderer.load_style_from_url(&url);
            renderer.set_map_size(Size {
                width: 32,
                height: 32,
            });
            while let Ok(done) = command_rx.recv() {
                renderer
                    .render_static(
                        &CameraUpdate::new()
                            .center(LatLng { lat: 0.0, lng: 0.0 })
                            .zoom(0.0)
                            .bearing(0.0)
                            .pitch(0.0),
                    )
                    .expect("render succeeds");
                let _ = done.send(());
            }
        });
        Self {
            commands: Some(commands),
            thread: Some(thread),
        }
    }

    fn render(&self, label: &'static str) -> Duration {
        let (done_tx, done_rx) = mpsc::channel();
        let started = Instant::now();
        self.commands
            .as_ref()
            .expect("harness not dropped")
            .send(done_tx)
            .expect("renderer thread alive");
        match done_rx.recv_timeout(Duration::from_secs(60)) {
            Ok(()) => started.elapsed(),
            Err(_) => panic!("{label}: render never completed"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_entry_is_served_while_revalidation_happens_in_the_background() {
    let counts = Arc::new(ServerCounts {
        sprite_full: AtomicUsize::new(0),
        sprite_not_modified: AtomicUsize::new(0),
        first_conditional_at: std::sync::Mutex::new(None),
    });
    let style_url = spawn_origin(Arc::clone(&counts));
    register_file_sources(
        16 * 1024 * 1024,
        vec!["127.0.0.1".to_string()],
        FileSourceIoPermits::default(),
        "swr-e2e-test",
    )
    .expect("file sources register");

    // Cold: full sprite fetches, entries stored with lifetime 2s + 600s grant.
    let cold = RendererHarness::spawn(&style_url);
    cold.render("cold render");
    drop(cold);
    assert!(counts.sprite_full.load(Ordering::SeqCst) >= 1);
    assert_eq!(counts.sprite_not_modified.load(Ordering::SeqCst), 0);

    // Let the lifetime elapse; the entries are now stale inside the grant.
    thread::sleep(Duration::from_millis(2_500));

    // A fresh renderer's initial load hits the stale entries. The Database
    // source serves them usable-stale, MapLibre Native delivers the bodies
    // and pairs low-priority conditional refreshes, and the render completes
    // without waiting for the deliberately delayed 304s. The renderer stays
    // alive so its run loop can receive those delayed responses.
    let stale = RendererHarness::spawn(&style_url);
    stale.render("stale render");
    let render_done = Instant::now();

    let deadline = Instant::now() + Duration::from_secs(10);
    while counts.sprite_not_modified.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    let first_conditional_at = counts
        .first_conditional_at
        .lock()
        .unwrap()
        .expect("the delivered stale entries must pair background conditional refreshes");
    assert!(
        render_done < first_conditional_at + REVALIDATION_DELAY - Duration::from_millis(100),
        "the render must complete without waiting out the delayed revalidation"
    );

    // Give the delayed 304s time to land and refresh the shared cache while
    // the renderer that owns those requests is still alive.
    thread::sleep(REVALIDATION_DELAY + Duration::from_millis(700));
    drop(stale);

    // A refreshed entry is fresh again: a further fresh renderer issues no
    // sprite requests at all.
    let full_before = counts.sprite_full.load(Ordering::SeqCst);
    let not_modified_before = counts.sprite_not_modified.load(Ordering::SeqCst);
    let refreshed = RendererHarness::spawn(&style_url);
    refreshed.render("post-refresh render");
    assert_eq!(
        counts.sprite_full.load(Ordering::SeqCst),
        full_before,
        "the 304 must refresh the shared cache entry, not force a refetch"
    );
    assert_eq!(
        counts.sprite_not_modified.load(Ordering::SeqCst),
        not_modified_before,
        "a refreshed entry is fresh again and needs no revalidation"
    );
}
