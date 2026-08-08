//! Rules every object-store-backed component in Abashiri obeys.
//!
//! The management plane configures several independent stores — the auth
//! registry, the style catalog, and the state and journal roots. Each parses an
//! operator-supplied URL through `parse_url_opts` and each bounds its reads. Both
//! rules belong in one place so a new store cannot be added with a subtly weaker
//! version of either.

use anyhow::ensure;
use std::time::Duration;
use url::Url;

/// Ceiling on a single object-store operation.
///
/// Every management operation is interactive: a caller is waiting on an HTTP
/// response, so an unbounded store call would hold the request open for as long
/// as the backend stays silent. This bounds one `get`/`put` or one body
/// collection, not a whole route.
pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Reject a configured object-store URL that carries anything but a location.
///
/// `parse_url_opts` reads query parameters as backend configuration, so a query
/// string on an operator-supplied URL is either a mistake or an attempt to smuggle
/// store options — a credential, an endpoint override — past the setting the
/// operator believes they are writing. Embedded userinfo is refused for the same
/// reason, and additionally because it would otherwise reach logs and error
/// context. A fragment is meaningless to an object store and signals the value was
/// copied from somewhere it did not belong.
///
/// `label` names the setting, so the refusal points at the flag to fix rather than
/// at an internal type.
pub(crate) fn ensure_location_only(url: &Url, label: &str) -> anyhow::Result<()> {
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "{label} must not contain a query or fragment"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "{label} must not contain embedded credentials"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> Url {
        Url::parse(value).expect("test URL")
    }

    #[test]
    fn a_location_only_url_is_accepted() {
        for value in [
            "memory:///control/",
            "file:///var/lib/abashiri/state/",
            "gs://bucket/prefix/catalog.json",
            "s3://bucket/prefix/",
        ] {
            ensure_location_only(&url(value), "test root").expect(value);
        }
    }

    #[test]
    fn anything_beyond_a_location_is_refused_and_named() {
        for value in [
            "memory:///control/?x=1",
            "memory:///control/#fragment",
            "https://user@example.test/root/",
            "https://user:secret@example.test/root/",
        ] {
            let error = ensure_location_only(&url(value), "storage root")
                .expect_err(&format!("{value} was accepted"));
            let message = error.to_string();
            assert!(
                message.starts_with("storage root must not"),
                "refusal did not name the setting: {message}"
            );
        }
    }

    /// The refusal must not echo the URL: an operator-supplied value can hold a
    /// credential, which is exactly the case being rejected.
    #[test]
    fn a_refusal_never_repeats_the_offending_url() {
        let error = ensure_location_only(&url("https://user:secret@example.test/root/"), "root")
            .expect_err("credentials rejected");
        let message = error.to_string();
        assert!(!message.contains("secret"), "{message}");
        assert!(!message.contains("example.test"), "{message}");
    }
}
