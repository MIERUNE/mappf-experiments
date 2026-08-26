//! Provider `Cache-Control` parsing and shared-cache policy normalization.

use std::{fmt::Write as _, sync::Arc, time::Duration};

use mmpf_http::cache_control::{ParsedCacheControl as CacheControl, parse_values};

use super::cache;

const STYLE_POSITIVE_TTL: Duration = Duration::from_mins(5);
const GLYPH_POSITIVE_TTL: Duration = Duration::from_hours(24);
/// Shorter than [`GLYPH_POSITIVE_TTL`] for the same reason `cache::SPRITE` is
/// shorter than `cache::GLYPH`: a sprite path is mutable.
const SPRITE_POSITIVE_TTL: Duration = Duration::from_hours(1);
const PROVIDER_NEGATIVE_TTL: Duration = Duration::from_secs(30);
/// Upper bound on any upstream-derived freshness or stale window. Ishikari is a
/// shared cache, so a pathological `max-age` must not pin bytes for months.
const MAX_PROVIDER_TTL: Duration = Duration::from_hours(168);

/// Effective caching decision for one fetched provider resource, derived from
/// the upstream `Cache-Control` (when present) or the per-resource defaults.
#[derive(Clone)]
pub(super) struct CachePolicy {
    /// When false, the bytes are returned but never retained by this shared
    /// cache (`no-store`, `no-cache`, or `private`).
    pub(super) store: bool,
    /// How long the entry is served without revalidation.
    pub(super) fresh: Duration,
    /// Extra window past `fresh` in which the stale entry is served while a
    /// background revalidation runs (upstream `stale-while-revalidate`).
    pub(super) swr: Duration,
    /// Cache policy emitted to downstream caches. This is normalized and
    /// clamped independently of the local entry's current age; `Age` carries
    /// the time already spent in Ishikari's cache.
    pub(super) response_cache_control: Arc<str>,
}

/// Shared-cache policy for an authoritative provider miss. Negative entries
/// never use stale-while-revalidate and are capped much more tightly than
/// successful resource bodies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NegativeCachePolicy {
    pub(super) store: bool,
    pub(super) fresh: Duration,
}

impl CachePolicy {
    /// Policy for a body whose upstream did not constrain caching.
    fn defaulted(resource: &'static str) -> Self {
        Self {
            store: true,
            fresh: positive_ttl(resource),
            swr: Duration::ZERO,
            response_cache_control: Arc::from(default_response_cache_control(resource)),
        }
    }

    /// Policy for a physically present `Cache-Control` field that cannot be
    /// decoded: it may carry `no-store` or `private`, so the bytes are neither
    /// retained by this shared cache nor re-advertised as cacheable.
    fn unreadable() -> Self {
        Self {
            store: false,
            fresh: Duration::ZERO,
            swr: Duration::ZERO,
            response_cache_control: Arc::from("no-store"),
        }
    }
}

/// Decoded state of the upstream `Cache-Control` field(s). `HeaderValue`
/// accepts obs-text (0x80–0xFF) on construction while `to_str` refuses it, so
/// a present-but-undecodable field is a real input, and dropping it would
/// silently discard a `no-store`/`private` it may carry. Policy resolution
/// therefore keeps three states distinct and fails `Unreadable` closed.
pub(super) enum UpstreamCacheControl<'a> {
    Absent,
    Readable(Vec<&'a str>),
    Unreadable,
}

impl<'a> UpstreamCacheControl<'a> {
    /// Decodes every physical field. Any UTF-8 is readable (more tolerant
    /// than `to_str`, which also rejects valid multi-byte text); one
    /// undecodable field poisons the whole header, because directives in a
    /// dropped field cannot be proven absent from the ones kept.
    pub(super) fn from_fields(fields: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut values = Vec::new();
        for field in fields {
            match std::str::from_utf8(field) {
                Ok(value) => values.push(value),
                Err(_) => return Self::Unreadable,
            }
        }
        if values.is_empty() {
            Self::Absent
        } else {
            Self::Readable(values)
        }
    }
}

/// [`cache_policy_with_freshness_values`] with the unreadable state failed
/// closed instead of defaulted.
pub(super) fn cache_policy_with_freshness(
    resource: &'static str,
    upstream: UpstreamCacheControl<'_>,
) -> (CachePolicy, bool) {
    match upstream {
        UpstreamCacheControl::Unreadable => (CachePolicy::unreadable(), false),
        UpstreamCacheControl::Absent => (CachePolicy::defaulted(resource), false),
        UpstreamCacheControl::Readable(values) => {
            cache_policy_with_freshness_values(resource, values)
        }
    }
}

/// [`negative_cache_policy_with_freshness_values`] with the unreadable state
/// failed closed instead of defaulted.
pub(super) fn negative_cache_policy_with_freshness(
    upstream: UpstreamCacheControl<'_>,
) -> (NegativeCachePolicy, bool) {
    match upstream {
        UpstreamCacheControl::Unreadable => (
            NegativeCachePolicy {
                store: false,
                fresh: Duration::ZERO,
            },
            false,
        ),
        UpstreamCacheControl::Absent => {
            negative_cache_policy_with_freshness_values(std::iter::empty())
        }
        UpstreamCacheControl::Readable(values) => {
            negative_cache_policy_with_freshness_values(values)
        }
    }
}

/// Effective policy after a revalidation response. An absent field inherits
/// the stored policy (RFC 9111 §4.3.4); a readable field restates it; an
/// unreadable field must become `no-store` — inheriting would resurrect a
/// policy the origin may just have withdrawn.
pub(super) fn revalidated_cache_policy(
    resource: &'static str,
    upstream: UpstreamCacheControl<'_>,
    stored: &str,
) -> CachePolicy {
    match upstream {
        UpstreamCacheControl::Unreadable => CachePolicy::unreadable(),
        UpstreamCacheControl::Absent => cache_policy(resource, Some(stored)),
        UpstreamCacheControl::Readable(values) => cache_policy_values(resource, values),
    }
}

#[cfg(test)]
fn parse_cache_control(value: &str) -> CacheControl {
    mmpf_http::cache_control::parse(value)
}

/// Resolves the effective policy. A shared cache prefers `s-maxage` over
/// `max-age`; `no-store`, `no-cache`, and `private` bypass this shared cache.
/// Revalidation-required responses never use SWR. All windows and the emitted
/// downstream policy are clamped to [`MAX_PROVIDER_TTL`].
pub(super) fn cache_policy(resource: &'static str, upstream: Option<&str>) -> CachePolicy {
    cache_policy_values(resource, upstream)
}

pub(super) fn cache_policy_values<'a>(
    resource: &'static str,
    upstream: impl IntoIterator<Item = &'a str>,
) -> CachePolicy {
    cache_policy_with_freshness_values(resource, upstream).0
}

pub(super) fn cache_policy_with_freshness_values<'a>(
    resource: &'static str,
    upstream: impl IntoIterator<Item = &'a str>,
) -> (CachePolicy, bool) {
    let control = parse_values(upstream);
    let has_explicit_freshness = control
        .as_ref()
        .is_some_and(|control| control.max_age.is_some() || control.s_maxage.is_some());
    let Some(control) = control else {
        return (CachePolicy::defaulted(resource), false);
    };
    let response_cache_control = Arc::from(normalized_cache_control(resource, &control));
    if control.no_store || control.no_cache || control.private {
        return (
            CachePolicy {
                store: false,
                fresh: Duration::ZERO,
                swr: Duration::ZERO,
                response_cache_control,
            },
            has_explicit_freshness,
        );
    }
    let clamp = |secs: u64| Duration::from_secs(secs).min(MAX_PROVIDER_TTL);
    let fresh = match control.s_maxage.or(control.max_age) {
        Some(secs) => clamp(secs),
        None => positive_ttl(resource),
    };
    let swr = if control.must_revalidate || control.proxy_revalidate {
        Duration::ZERO
    } else {
        control.stale_while_revalidate.map_or(Duration::ZERO, clamp)
    };
    (
        CachePolicy {
            store: true,
            fresh,
            swr,
            response_cache_control,
        },
        has_explicit_freshness,
    )
}

/// Resolves negative-entry retention from every physical `Cache-Control`
/// field. Missing policy receives the bounded service default; explicit shared
/// freshness may shorten it but can never extend it. Directives requiring a
/// private cache or validation bypass Ishikari's shared negative cache.
pub(super) fn negative_cache_policy_values<'a>(
    upstream: impl IntoIterator<Item = &'a str>,
) -> NegativeCachePolicy {
    negative_cache_policy_with_freshness_values(upstream).0
}

pub(super) fn negative_cache_policy_with_freshness_values<'a>(
    upstream: impl IntoIterator<Item = &'a str>,
) -> (NegativeCachePolicy, bool) {
    let control = parse_values(upstream);
    let has_explicit_freshness = control
        .as_ref()
        .is_some_and(|control| control.max_age.is_some() || control.s_maxage.is_some());
    let Some(control) = control else {
        return (
            NegativeCachePolicy {
                store: true,
                fresh: PROVIDER_NEGATIVE_TTL,
            },
            false,
        );
    };
    if control.no_store || control.no_cache || control.private {
        return (
            NegativeCachePolicy {
                store: false,
                fresh: Duration::ZERO,
            },
            has_explicit_freshness,
        );
    }
    let fresh = control
        .s_maxage
        .or(control.max_age)
        .map_or(PROVIDER_NEGATIVE_TTL, Duration::from_secs)
        .min(PROVIDER_NEGATIVE_TTL);
    (
        NegativeCachePolicy { store: true, fresh },
        has_explicit_freshness,
    )
}

fn normalized_cache_control(resource: &'static str, control: &CacheControl) -> String {
    if control.no_store {
        return "no-store".to_string();
    }
    if control.no_cache {
        return "no-cache".to_string();
    }

    let clamp = |seconds: u64| seconds.min(MAX_PROVIDER_TTL.as_secs());
    if control.private {
        let max_age = control.max_age.map_or(0, clamp);
        return format!("private, max-age={max_age}");
    }

    let default_fresh = positive_ttl(resource).as_secs();
    let max_age = control.max_age.map_or_else(
        || {
            if control.s_maxage.is_some() {
                0
            } else {
                default_fresh
            }
        },
        clamp,
    );
    let s_maxage = control
        .s_maxage
        .map(clamp)
        .or_else(|| control.max_age.map(clamp))
        .unwrap_or(default_fresh);
    let mut directives = String::with_capacity(96);
    write!(directives, "public, max-age={max_age}, s-maxage={s_maxage}")
        .expect("writing to String cannot fail");
    if !(control.must_revalidate || control.proxy_revalidate)
        && let Some(swr) = control.stale_while_revalidate.map(clamp)
        && swr > 0
    {
        write!(directives, ", stale-while-revalidate={swr}")
            .expect("writing to String cannot fail");
    }
    if control.must_revalidate {
        directives.push_str(", must-revalidate");
    } else if control.proxy_revalidate {
        directives.push_str(", proxy-revalidate");
    }
    if control.no_transform {
        directives.push_str(", no-transform");
    }
    if control.immutable {
        directives.push_str(", immutable");
    }
    directives
}

fn default_response_cache_control(resource: &'static str) -> &'static str {
    match resource {
        "style" => cache::STYLE,
        "glyph" => cache::GLYPH,
        "sprite" => cache::SPRITE,
        _ => "no-cache",
    }
}

fn positive_ttl(resource: &'static str) -> Duration {
    match resource {
        "glyph" => GLYPH_POSITIVE_TTL,
        "sprite" => SPRITE_POSITIVE_TTL,
        _ => STYLE_POSITIVE_TTL,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::HeaderValue;

    use super::{
        MAX_PROVIDER_TTL, UpstreamCacheControl, cache_policy, cache_policy_values,
        cache_policy_with_freshness, negative_cache_policy_values,
        negative_cache_policy_with_freshness, normalized_cache_control, parse_cache_control,
        positive_ttl, revalidated_cache_policy,
    };

    #[test]
    fn provider_cache_ttl_follows_how_mutable_the_resource_is() {
        assert_eq!(positive_ttl("style"), Duration::from_mins(5));
        // A glyph range is immutable by construction.
        assert_eq!(positive_ttl("glyph"), Duration::from_hours(24));
        // A sprite path is mutable until sprites are content-addressed, so it
        // must not inherit the glyph lifetime.
        assert_eq!(positive_ttl("sprite"), Duration::from_hours(1));
        assert!(positive_ttl("sprite") < positive_ttl("glyph"));
    }

    #[test]
    fn missing_upstream_cache_control_uses_resource_default() {
        let style = cache_policy("style", None);
        assert!(style.store);
        assert_eq!(style.fresh, Duration::from_mins(5));
        assert_eq!(style.swr, Duration::ZERO);
        assert_eq!(cache_policy("glyph", None).fresh, Duration::from_hours(24));
    }

    #[test]
    fn shared_cache_prefers_s_maxage_and_honors_swr() {
        let policy = cache_policy(
            "style",
            Some("public, max-age=60, s-maxage=600, stale-while-revalidate=120"),
        );
        assert!(policy.store);
        assert_eq!(policy.fresh, Duration::from_mins(10));
        assert_eq!(policy.swr, Duration::from_mins(2));
    }

    #[test]
    fn max_age_is_used_when_s_maxage_absent() {
        let policy = cache_policy("style", Some("max-age=45"));
        assert_eq!(policy.fresh, Duration::from_secs(45));
        assert_eq!(policy.swr, Duration::ZERO);
    }

    #[test]
    fn no_store_bypasses_the_cache() {
        let policy = cache_policy("glyph", Some("no-store"));
        assert!(!policy.store);
        assert_eq!(policy.fresh, Duration::ZERO);
    }

    #[test]
    fn no_cache_and_private_bypass_the_shared_cache() {
        for directive in ["no-cache", "private", "private, max-age=600"] {
            let policy = cache_policy("style", Some(directive));
            assert!(!policy.store, "{directive} must not enter the shared cache");
            assert_eq!(policy.fresh, Duration::ZERO, "{directive}");
            assert_eq!(policy.swr, Duration::ZERO, "{directive}");
        }
    }

    #[test]
    fn negative_policy_honors_origin_bypass_and_bounded_freshness() {
        for directive in ["no-store", "no-cache", "private, max-age=30"] {
            let policy = negative_cache_policy_values([directive]);
            assert!(!policy.store, "{directive}");
            assert_eq!(policy.fresh, Duration::ZERO, "{directive}");
        }

        assert_eq!(
            negative_cache_policy_values(std::iter::empty::<&str>()).fresh,
            Duration::from_secs(30)
        );
        assert_eq!(
            negative_cache_policy_values(["s-maxage=7"]).fresh,
            Duration::from_secs(7)
        );
        assert_eq!(
            negative_cache_policy_values(["max-age=3600"]).fresh,
            Duration::from_secs(30)
        );
        assert_eq!(
            negative_cache_policy_values(["max-age=0"]).fresh,
            Duration::ZERO
        );
    }

    #[test]
    fn revalidation_directives_disable_stale_serving() {
        for directive in ["must-revalidate", "proxy-revalidate"] {
            let policy = cache_policy(
                "style",
                Some(&format!(
                    "max-age=60, stale-while-revalidate=600, {directive}"
                )),
            );
            assert!(policy.store);
            assert_eq!(policy.fresh, Duration::from_mins(1));
            assert_eq!(policy.swr, Duration::ZERO);
            assert!(
                !policy
                    .response_cache_control
                    .contains("stale-while-revalidate")
            );
        }
    }

    #[test]
    fn duplicate_freshness_directives_use_the_most_conservative_value() {
        for value in ["max-age=0, max-age=604800", "max-age=604800, max-age=0"] {
            let control = parse_cache_control(value);
            assert_eq!(control.max_age, Some(0));
            assert_eq!(cache_policy("style", Some(value)).fresh, Duration::ZERO);
        }

        let control = parse_cache_control(
            "s-maxage=600, s-maxage=30, stale-while-revalidate=120, stale-while-revalidate=10",
        );
        assert_eq!(control.s_maxage, Some(30));
        assert_eq!(control.stale_while_revalidate, Some(10));
    }

    #[test]
    fn an_unreadable_cache_control_field_fails_the_positive_policy_closed() {
        // One invalid-UTF-8 byte in a physically present field. Decoding it
        // as "absent" would default to store + public re-advertisement.
        let upstream = UpstreamCacheControl::from_fields([b"private, ext=\"\xe9\"".as_slice()]);
        let (policy, has_explicit_freshness) = cache_policy_with_freshness("glyph", upstream);
        assert!(
            !policy.store,
            "an undecodable field may carry no-store/private and must fail closed"
        );
        assert_eq!(policy.response_cache_control.as_ref(), "no-store");
        assert!(!has_explicit_freshness);
    }

    #[test]
    fn an_unreadable_cache_control_field_fails_the_negative_policy_closed() {
        let upstream = UpstreamCacheControl::from_fields([b"no-store, ext=\"\xe9\"".as_slice()]);
        let (policy, _) = negative_cache_policy_with_freshness(upstream);
        assert!(!policy.store);
        assert_eq!(policy.fresh, Duration::ZERO);
    }

    #[test]
    fn an_unreadable_cache_control_field_on_a_304_never_inherits_the_stored_policy() {
        let policy = revalidated_cache_policy(
            "style",
            UpstreamCacheControl::from_fields([b"max-age=60, ext=\"\xe9\"".as_slice()]),
            "public, max-age=300, s-maxage=3600",
        );
        assert!(
            !policy.store,
            "an undecodable restatement must not resurrect the stored policy"
        );
        assert_eq!(policy.response_cache_control.as_ref(), "no-store");

        let inherited = revalidated_cache_policy(
            "style",
            UpstreamCacheControl::Absent,
            "public, max-age=300, s-maxage=3600",
        );
        assert!(inherited.store, "a truly absent field still inherits");
        assert_eq!(inherited.fresh, Duration::from_secs(3600));
    }

    #[test]
    fn utf8_obs_text_stays_readable_and_its_directives_apply() {
        // `HeaderValue::to_str` rejects any obs-text and would have dropped
        // this whole field; UTF-8 decoding keeps its `no-store` effective.
        let upstream = UpstreamCacheControl::from_fields(["no-store, ext=\"café\"".as_bytes()]);
        let (policy, _) = cache_policy_with_freshness("style", upstream);
        assert!(!policy.store);
        assert_eq!(policy.response_cache_control.as_ref(), "no-store");
    }

    #[test]
    fn physical_cache_control_fields_cannot_hide_each_other() {
        let policy = cache_policy_values(
            "style",
            [r#"extension="unterminated"#, "max-age=600", "no-store"],
        );
        assert!(!policy.store, "a separate no-store field must win");

        let policy = cache_policy_values("style", ["max-age=600", "MAX-AGE=0"]);
        assert_eq!(policy.fresh, Duration::ZERO);
    }

    #[test]
    fn quoted_extension_commas_do_not_create_policy_directives() {
        let policy = cache_policy("style", Some(r#"extension="private,no-store", max-age=60"#));
        assert!(policy.store);
        assert_eq!(policy.fresh, Duration::from_mins(1));
    }

    #[test]
    fn upstream_windows_are_clamped_to_the_ceiling() {
        let policy = cache_policy(
            "style",
            Some("max-age=999999999, stale-while-revalidate=999999999"),
        );
        assert_eq!(policy.fresh, MAX_PROVIDER_TTL);
        assert_eq!(policy.swr, MAX_PROVIDER_TTL);
    }

    #[test]
    fn cache_control_parsing_is_case_insensitive_and_tolerant() {
        let control = parse_cache_control("  Public , Max-Age=\"30\" , unknown-directive ");
        assert_eq!(control.max_age, Some(30));
        assert!(!control.no_store);
    }

    #[test]
    fn cache_control_parser_tolerates_hostile_inputs() {
        // None of these may panic; each must resolve to a usable policy.
        for input in [
            "",
            "   ",
            ",,,",
            "max-age",
            "max-age=",
            "max-age=abc",
            "max-age=-5",
            "max-age=99999999999999999999999999999999",
            "max-age=\"30",
            "MAX-AGE=30, No-Store",
            "max-age=30, max-age=0",
            &"a=1,".repeat(5_000),
        ] {
            let policy = cache_policy("style", Some(input));
            assert!(
                policy.fresh <= MAX_PROVIDER_TTL,
                "fresh unbounded for {input:?}"
            );
            assert!(
                policy.swr <= MAX_PROVIDER_TTL,
                "swr unbounded for {input:?}"
            );
            let normalized = normalized_cache_control("style", &parse_cache_control(input));
            assert!(
                HeaderValue::from_str(&normalized).is_ok(),
                "unemittable Cache-Control for {input:?}: {normalized:?}"
            );
        }
    }

    #[test]
    fn cache_control_invariants_hold_for_freshness_directives() {
        assert_eq!(
            cache_policy("style", Some("max-age=abc")).fresh,
            Duration::ZERO
        );
        assert_eq!(
            cache_policy("style", Some("s-maxage=99999999999999999999")).fresh,
            MAX_PROVIDER_TTL
        );
        assert_eq!(
            cache_policy("style", Some("max-age=604800, max-age=0")).fresh,
            cache_policy("style", Some("max-age=0, max-age=604800")).fresh
        );
    }
}
