//! Glyph PBF provider endpoint.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use bytes::Bytes;
use futures_util::future::try_join_all;
use ishikari_core::storage::ProviderRequest;
use moka::{Expiry, future::Cache};
use pbf_font_tools::{Fontstack, Glyphs, prost::Message};

use crate::server::{
    AppState, HttpError, provider_body::UNTYPED_OBJECT_CONTENT_TYPE, upstream::ProviderResource,
};

const MAX_FONTSTACK_LEN: usize = 256;
const MAX_FONTS_PER_STACK: usize = 8;
pub(crate) const MAX_GLYPH_BYTES: usize = 1024 * 1024;
// Glyph ids are Unicode code points stored as uint32 in the glyph PBF. Keep
// the same full-Unicode ceiling as Martin; supplementary-plane fonts and
// upstream composers can legitimately serve ranges above the BMP.
const MAX_UNICODE_CODEPOINT: u32 = 0x10_FFFF;
const GLYPH_CONTENT_TYPES: &[&str] = &[
    "application/x-protobuf",
    "application/vnd.google.protobuf",
    "application/protobuf",
    "application/octet-stream",
    // A range stored as a gzip stream. Glyph PBFs compress to roughly 60-70% of
    // their size, which matters because one cold CJK render pulls ~150 ranges of
    // ~200 KiB. The encoding is declared by the content type rather than by
    // `Content-Encoding` because the latter triggers GCS decompressive
    // transcoding, which drops `Content-Length` and breaks `object_store`.
    "application/gzip",
    "application/x-gzip",
    UNTYPED_OBJECT_CONTENT_TYPE,
];

#[derive(Clone, Debug)]
struct ParsedFontstack {
    names: Arc<[String]>,
    canonical: Arc<str>,
    display_name: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlyphRange {
    start: u32,
    end: u32,
    canonical: Arc<str>,
}

impl GlyphRange {
    fn as_str(&self) -> &str {
        &self.canonical
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct GlyphCompositeKey {
    fontstack: Arc<str>,
    range: Arc<str>,
}

#[derive(Clone)]
pub(super) struct CachedGlyphComposite {
    resource: ProviderResource,
    stored_at: Instant,
    fresh_for: Duration,
}

impl CachedGlyphComposite {
    fn current_resource(&self) -> ProviderResource {
        self.resource
            .clone()
            .with_additional_age(self.stored_at.elapsed())
    }
}

struct GlyphCompositeExpiry;

impl Expiry<GlyphCompositeKey, CachedGlyphComposite> for GlyphCompositeExpiry {
    fn expire_after_create(
        &self,
        _key: &GlyphCompositeKey,
        value: &CachedGlyphComposite,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.fresh_for)
    }
}

pub(super) type GlyphCompositeCache = Cache<GlyphCompositeKey, CachedGlyphComposite>;

pub(super) fn glyph_composite_cache(max_bytes: u64) -> GlyphCompositeCache {
    Cache::builder()
        .max_capacity(max_bytes)
        .weigher(|_key: &GlyphCompositeKey, value: &CachedGlyphComposite| {
            u32::try_from(value.resource.bytes().len()).unwrap_or(u32::MAX)
        })
        .expire_after(GlyphCompositeExpiry)
        .build()
}

#[cfg_attr(
    feature = "unstable-schemas",
    utoipa::path(
        get,
        path = "/fonts/{fontstack}/{range}",
        tag = "delivery",
        params(
            (
                "fontstack" = String,
                Path,
                description = "One font name, or a comma-separated stack merged first-font-wins"
            ),
            (
                "range" = String,
                Path,
                description = "256-codepoint range as `{start}-{end}`, optionally suffixed `.pbf`"
            )
        ),
        responses(
            (
                status = 200,
                description = "Glyph protobuf for the range",
                body = crate::schemas::BinaryPayload,
                content_type = "application/x-protobuf"
            ),
            (status = 400, description = "Malformed fontstack or range"),
            (status = 404, description = "No glyph provider configured")
        )
    )
)]
pub(crate) async fn glyph_handler(
    State(state): State<AppState>,
    Path((fontstack, range)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    let fontstack = parse_fontstack(&fontstack)?;
    let range = validate_range(&range)?;
    let upstream = resolve_glyph_url(&state, &fontstack.canonical, range.as_str())?;
    let resource = route_glyph_bytes(&state, &fontstack, &range, upstream).await?;
    // A range may be stored gzip-compressed, and this path never
    // cross-compresses: if the client excludes the only coding available there is
    // no alternative representation to offer, so refuse rather than send a body
    // the client said it cannot read. `public_response` adds
    // `Vary: Accept-Encoding` whenever a coding is present.
    crate::server::tileset::tile::ensure_content_encoding_acceptable(
        &headers,
        resource.content_encoding(),
        "glyph",
    )?;
    Ok(resource.public_response(&headers, resource.bytes().clone(), "application/x-protobuf"))
}

pub(crate) async fn internal_glyph_handler(
    State(state): State<AppState>,
    Path((fontstack, range)): Path<(String, String)>,
) -> Result<Response<Body>, HttpError> {
    let fontstack = parse_fontstack(&fontstack)?;
    let range = validate_range(&range)?;
    let upstream = resolve_glyph_url(&state, &fontstack.canonical, range.as_str())?;
    let resource = match local_glyph_resource(&state, &fontstack, &range, upstream).await {
        Ok(resource) => resource,
        Err(error) => return crate::server::provider::internal_provider_fetch_error(error),
    };
    state
        .metrics
        .add_internal_bytes(resource.bytes().len() as u64);
    Ok(resource.internal_response("application/x-protobuf"))
}

fn resolve_glyph_url(state: &AppState, fontstack: &str, range: &str) -> Result<String, HttpError> {
    state
        .provider
        .resolve_glyph_url(fontstack, range)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "glyph provider not configured".to_string(),
            )
        })
}

async fn route_glyph_bytes(
    state: &AppState,
    fontstack: &ParsedFontstack,
    range: &GlyphRange,
    upstream: String,
) -> Result<ProviderResource, HttpError> {
    let request = ProviderRequest::glyph(&fontstack.canonical, range.as_str(), &upstream);
    if let Some(resource) =
        crate::server::provider::route_peer_resource(&state.resource_resolver, &request).await?
    {
        return Ok(resource);
    }
    local_glyph_resource(state, fontstack, range, upstream).await
}

async fn local_glyph_resource(
    state: &AppState,
    fontstack: &ParsedFontstack,
    range: &GlyphRange,
    single_or_routing_url: String,
) -> Result<ProviderResource, HttpError> {
    match fetch_glyph_bytes_local(state, single_or_routing_url).await {
        Ok(resource) => return Ok(resource),
        Err((StatusCode::NOT_FOUND, _)) if fontstack.names.len() > 1 => {}
        Err(error) => return Err(error),
    }

    // Some providers store only individual font ranges, while others publish
    // an already-composed stack at the requested fontstack path. Prefer the
    // latter when present and compose only after a definitive 404 so existing
    // providers do not regress and transient failures are never hidden.
    let key = GlyphCompositeKey {
        fontstack: Arc::clone(&fontstack.canonical),
        range: Arc::clone(&range.canonical),
    };
    let cache = state.glyph_composite_cache().clone();
    let state = state.clone();
    let fontstack = fontstack.clone();
    let range = range.clone();
    let cached = cache
        .try_get_with(key, async move {
            build_glyph_composite(state, fontstack, range).await
        })
        .await
        .map_err(|error: Arc<HttpError>| (*error).clone())?;
    Ok(cached.current_resource())
}

async fn build_glyph_composite(
    state: AppState,
    fontstack: ParsedFontstack,
    range: GlyphRange,
) -> Result<CachedGlyphComposite, HttpError> {
    // Resolve every component before starting I/O. The stack bound prevents one
    // public request from turning into unbounded provider fan-out.
    let upstreams = fontstack
        .names
        .iter()
        .map(|font| resolve_glyph_url(&state, font, range.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    let resources = try_join_all(
        upstreams
            .into_iter()
            .map(|upstream| fetch_glyph_bytes_local(&state, upstream)),
    )
    .await?;
    let (cache_control, fresh_for) = composite_cache_policy(&resources)?;

    // Protobuf decode/dedup/encode is CPU work. Fetch first, then admit it so no
    // CPU permit is held while object storage or an upstream server is pending.
    let permit = state.admit_cpu_work("glyph_merge").await?;
    let display_name = fontstack.display_name;
    let merge_range = range;
    let merged = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let bodies = resources
            .iter()
            .map(|resource| resource.decoded_bytes(MAX_GLYPH_BYTES, "glyph"))
            .collect::<Result<Vec<_>, _>>()?;
        merge_glyph_pbf(&display_name, &merge_range, bodies)
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("glyph merge task failed: {error}"),
        )
    })??;

    Ok(CachedGlyphComposite {
        resource: ProviderResource::derived(merged, cache_control),
        stored_at: Instant::now(),
        fresh_for,
    })
}

async fn fetch_glyph_bytes_local(
    state: &AppState,
    upstream: String,
) -> Result<ProviderResource, HttpError> {
    state
        .provider_fetcher
        .fetch_bytes(upstream, MAX_GLYPH_BYTES, "glyph", GLYPH_CONTENT_TYPES)
        .await
}

fn parse_fontstack(fontstack: &str) -> Result<ParsedFontstack, HttpError> {
    if fontstack.is_empty() || fontstack.len() > MAX_FONTSTACK_LEN {
        return Err((
            StatusCode::BAD_REQUEST,
            "fontstack length invalid".to_string(),
        ));
    }
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for part in fontstack.split(',') {
        let name = part.trim();
        // `.` and `..` survive percent-encoding and are normalized as relative
        // URL path segments, so they must not reach the provider template.
        if name.is_empty()
            || name.chars().any(char::is_control)
            || name.contains('/')
            || name.contains('\\')
            || matches!(name, "." | "..")
        {
            return Err((StatusCode::BAD_REQUEST, "fontstack invalid".to_string()));
        }
        if seen.insert(name) {
            names.push(name);
        }
    }
    if names.len() > MAX_FONTS_PER_STACK {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("fontstack exceeds {MAX_FONTS_PER_STACK} fonts"),
        ));
    }
    Ok(ParsedFontstack {
        canonical: Arc::from(names.join(",")),
        display_name: Arc::from(names.join(", ")),
        names: names
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>()
            .into(),
    })
}

fn invalid_range() -> HttpError {
    (StatusCode::BAD_REQUEST, "glyph range invalid".to_string())
}

fn validate_range(range: &str) -> Result<GlyphRange, HttpError> {
    let (start, end) = range
        .strip_suffix(".pbf")
        .unwrap_or(range)
        .split_once('-')
        .ok_or_else(invalid_range)?;
    let start = start.parse::<u32>().map_err(|_| invalid_range())?;
    let end = end.parse::<u32>().map_err(|_| invalid_range())?;
    if end > MAX_UNICODE_CODEPOINT || start % 256 != 0 || start.checked_add(255) != Some(end) {
        return Err(invalid_range());
    }
    Ok(GlyphRange {
        start,
        end,
        canonical: Arc::from(format!("{start}-{end}")),
    })
}

fn composite_cache_policy(
    resources: &[ProviderResource],
) -> Result<(Arc<str>, Duration), HttpError> {
    let mut client_fresh = u64::MAX;
    let mut shared_fresh = u64::MAX;
    let mut cacheable = true;
    let mut requires_revalidation = false;

    for resource in resources {
        let control = mmpf_http::cache_control::parse(resource.cache_control());
        if control.no_transform {
            return Err((
                StatusCode::BAD_GATEWAY,
                "glyph component forbids transformation".to_string(),
            ));
        }
        cacheable &= !(control.no_store || control.no_cache || control.private);
        requires_revalidation |= control.must_revalidate || control.proxy_revalidate;
        let age = resource.age_seconds();
        client_fresh = client_fresh.min(control.max_age.unwrap_or(0).saturating_sub(age));
        shared_fresh = shared_fresh.min(
            control
                .s_maxage
                .or(control.max_age)
                .unwrap_or(0)
                .saturating_sub(age),
        );
    }

    if !cacheable {
        return Ok((Arc::from("no-store"), Duration::ZERO));
    }
    let suffix = if requires_revalidation {
        ", must-revalidate"
    } else {
        ""
    };
    Ok((
        Arc::from(format!(
            "public, max-age={client_fresh}, s-maxage={shared_fresh}{suffix}"
        )),
        Duration::from_secs(shared_fresh),
    ))
}

fn merge_glyph_pbf(
    display_name: &str,
    range: &GlyphRange,
    bodies: Vec<Bytes>,
) -> Result<Bytes, HttpError> {
    let mut coverage = HashSet::with_capacity(256);
    let mut glyphs = Vec::new();

    for body in bodies {
        let source = Glyphs::decode(body).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("invalid glyph protobuf: {error}"),
            )
        })?;
        for stack in source.stacks {
            for glyph in stack.glyphs {
                if !(range.start..=range.end).contains(&glyph.id) {
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        "glyph protobuf contains an id outside the requested range".to_string(),
                    ));
                }
                if coverage.insert(glyph.id) {
                    glyphs.push(glyph);
                }
            }
        }
    }
    glyphs.sort_unstable_by_key(|glyph| glyph.id);

    let output = Glyphs {
        stacks: vec![Fontstack {
            name: display_name.to_string(),
            range: range.as_str().to_string(),
            glyphs,
        }],
    }
    .encode_to_vec();
    if output.len() > MAX_GLYPH_BYTES {
        return Err((
            StatusCode::BAD_GATEWAY,
            "merged glyph protobuf exceeds response limit".to_string(),
        ));
    }
    Ok(Bytes::from(output))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pbf_font_tools::{Fontstack, Glyph, Glyphs, prost::Message};

    use super::{MAX_FONTS_PER_STACK, merge_glyph_pbf, parse_fontstack, validate_range};

    fn glyph(id: u32, marker: u8) -> Glyph {
        Glyph {
            id,
            bitmap: Some(vec![marker]),
            width: 1,
            height: 1,
            left: 0,
            top: 0,
            advance: 1,
        }
    }

    fn encoded(name: &str, glyphs: Vec<Glyph>) -> Bytes {
        Bytes::from(
            Glyphs {
                stacks: vec![Fontstack {
                    name: name.to_string(),
                    range: "0-255".to_string(),
                    glyphs,
                }],
            }
            .encode_to_vec(),
        )
    }

    #[test]
    fn validates_256_codepoint_ranges() {
        assert_eq!(validate_range("0-255").unwrap().as_str(), "0-255");
        assert_eq!(
            validate_range("65280-65535.pbf").unwrap().as_str(),
            "65280-65535"
        );
        assert_eq!(
            validate_range("65536-65791.pbf").unwrap().as_str(),
            "65536-65791"
        );
        assert_eq!(
            validate_range("1113856-1114111.pbf").unwrap().as_str(),
            "1113856-1114111"
        );
        assert!(validate_range("1-256").is_err());
        assert!(validate_range("0-254").is_err());
        assert!(validate_range("1114112-1114367").is_err());
    }

    #[test]
    fn validates_normalizes_and_bounds_fontstacks() {
        let parsed = parse_fontstack("Noto Sans JP, Arial,Noto Sans JP").unwrap();
        assert_eq!(&*parsed.names, ["Noto Sans JP", "Arial"]);
        assert_eq!(&*parsed.canonical, "Noto Sans JP,Arial");
        assert_eq!(&*parsed.display_name, "Noto Sans JP, Arial");

        assert!(parse_fontstack("").is_err());
        assert!(parse_fontstack("Noto/../../Sans").is_err());
        assert!(parse_fontstack("Noto,,Arial").is_err());
        assert!(parse_fontstack("Noto Sans\nforged-log-line").is_err());
        assert!(parse_fontstack("Noto Sans\tJP").is_err());
        assert!(
            parse_fontstack(
                &(0..=MAX_FONTS_PER_STACK)
                    .map(|index| format!("f{index}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .is_err()
        );
    }

    #[test]
    fn merges_first_font_wins_and_emits_one_canonical_stack() {
        let range = validate_range("0-255").unwrap();
        let merged = merge_glyph_pbf(
            "Primary, Fallback",
            &range,
            vec![
                encoded("Primary", vec![glyph(1, 1), glyph(3, 3)]),
                encoded("Fallback", vec![glyph(2, 2), glyph(3, 9)]),
            ],
        )
        .unwrap();
        let decoded = Glyphs::decode(merged).unwrap();

        assert_eq!(decoded.stacks.len(), 1);
        let stack = &decoded.stacks[0];
        assert_eq!(stack.name, "Primary, Fallback");
        assert_eq!(stack.range, "0-255");
        assert_eq!(
            stack
                .glyphs
                .iter()
                .map(|glyph| (glyph.id, glyph.bitmap.as_deref().unwrap()[0]))
                .collect::<Vec<_>>(),
            [(1, 1), (2, 2), (3, 3)]
        );
    }

    #[test]
    fn merges_empty_components_into_a_valid_named_range() {
        let range = validate_range("0-255").unwrap();
        let merged = merge_glyph_pbf(
            "Primary, Fallback",
            &range,
            vec![
                encoded("Primary", Vec::new()),
                encoded("Fallback", Vec::new()),
            ],
        )
        .unwrap();
        let decoded = Glyphs::decode(merged).unwrap();

        assert_eq!(decoded.stacks.len(), 1);
        assert_eq!(decoded.stacks[0].name, "Primary, Fallback");
        assert_eq!(decoded.stacks[0].range, "0-255");
        assert!(decoded.stacks[0].glyphs.is_empty());
    }

    #[test]
    fn rejects_malformed_or_out_of_range_component_pbf() {
        let range = validate_range("0-255").unwrap();
        assert!(merge_glyph_pbf("A, B", &range, vec![Bytes::from_static(b"bad")]).is_err());
        assert!(merge_glyph_pbf("A, B", &range, vec![encoded("A", vec![glyph(256, 1)])]).is_err());
    }
    #[test]
    fn a_dot_segment_fontstack_cannot_escape_the_glyph_prefix() {
        for escape in ["..", ".", " .. "] {
            assert!(
                parse_fontstack(escape).is_err(),
                "{escape:?} must be rejected as a fontstack"
            );
        }

        // The same input, had it been accepted, resolves above the prefix.
        let raw = "gs://bucket/fonts/{fontstack}/{range}.pbf"
            .replace(
                "{fontstack}",
                &ishikari_core::storage::path_percent_encode(".."),
            )
            .replace("{range}", "0-255");
        assert_eq!(
            url::Url::parse(&raw).expect("template parses").path(),
            "/0-255.pbf",
            "this is the escape the validator now prevents"
        );

        // Dots inside a real font name stay legal.
        assert!(parse_fontstack("...").is_ok());
        assert!(parse_fontstack("Noto Sans v1.2").is_ok());
        assert!(parse_fontstack("A.B,C.D").is_ok());
    }
}
