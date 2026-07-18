//! URL and resolved-address policy for MapLibre resource requests.

use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

#[derive(Clone)]
pub(crate) struct ResourceUrlPolicy {
    private_hosts: Arc<[String]>,
}

impl ResourceUrlPolicy {
    pub(crate) fn new(private_hosts: Vec<String>) -> Self {
        Self {
            private_hosts: private_hosts
                .into_iter()
                .map(|host| host.trim().trim_end_matches('.').to_ascii_lowercase())
                .filter(|host| !host.is_empty())
                .collect(),
        }
    }

    pub(crate) fn permits_url_without_dns(&self, url: &url::Url) -> bool {
        if !matches!(url.scheme(), "http" | "https") {
            return false;
        }
        let Some(host) = url.host() else {
            return false;
        };
        let host_label = host
            .to_string()
            .trim_matches(['[', ']'])
            .to_ascii_lowercase();
        match host {
            url::Host::Ipv4(address) => {
                !is_forbidden_address(IpAddr::V4(address)) || self.allows_private_host(&host_label)
            }
            url::Host::Ipv6(address) => {
                !is_forbidden_address(IpAddr::V6(address)) || self.allows_private_host(&host_label)
            }
            url::Host::Domain(_) => true,
        }
    }

    fn allows_private_host(&self, host: &str) -> bool {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        self.private_hosts.iter().any(|pattern| {
            if let Some(suffix) = pattern.strip_prefix("*.") {
                host != suffix && host.ends_with(&format!(".{suffix}"))
            } else {
                host == *pattern
            }
        })
    }
}

#[derive(Clone)]
pub(super) struct FilteringResolver {
    policy: ResourceUrlPolicy,
}

impl FilteringResolver {
    pub(super) fn new(policy: ResourceUrlPolicy) -> Self {
        Self { policy }
    }
}

impl Resolve for FilteringResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().trim_end_matches('.').to_ascii_lowercase();
        let allow_private = self.policy.allows_private_host(&host);
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let addresses: Vec<_> = addresses
                .filter(|address| allow_private || !is_forbidden_address(address.ip()))
                .collect();
            if addresses.is_empty() {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("resource host {host} resolves only to blocked addresses"),
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

fn is_forbidden_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_forbidden_ipv4(address),
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4() {
                return is_forbidden_ipv4(mapped);
            }
            let segments = address.segments();
            if segments[..6] == [0x0064, 0xff9b, 0, 0, 0, 0] {
                let embedded = Ipv4Addr::new(
                    (segments[6] >> 8) as u8,
                    segments[6] as u8,
                    (segments[7] >> 8) as u8,
                    segments[7] as u8,
                );
                return is_forbidden_ipv4(embedded);
            }
            address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || address.is_unique_local()
                || address.is_unicast_link_local()
                || segments[..3] == [0x0064, 0xff9b, 0x0001] // local-use NAT64
                || segments[0] == 0x2002 // deprecated 6to4 transition addresses
                || segments[..2] == [0x2001, 0] // Teredo transition addresses
                || segments[..2] == [0x2001, 0x0db8] // documentation
        }
    }
}

fn is_forbidden_ipv4(address: Ipv4Addr) -> bool {
    let value = u32::from(address);
    address.is_unspecified()
        || address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_broadcast()
        || address.is_documentation()
        || value >> 24 == 0
        || value & 0xffc0_0000 == 0x6440_0000 // 100.64.0.0/10 shared address space
        || value & 0xffff_ff00 == 0xc000_0000 // 192.0.0.0/24 protocol assignments
        || value & 0xfffe_0000 == 0xc612_0000 // 198.18.0.0/15 benchmarking
        || value & 0xf000_0000 == 0xf000_0000 // reserved / limited broadcast
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_non_public_addresses_unless_explicit() {
        let public_only = ResourceUrlPolicy::new(Vec::new());
        assert!(public_only.permits_url_without_dns(
            &url::Url::parse("https://8.8.8.8/tile").expect("public URL")
        ));
        assert!(!public_only.permits_url_without_dns(
            &url::Url::parse("http://169.254.169.254/metadata").expect("metadata URL")
        ));
        assert!(!public_only.permits_url_without_dns(
            &url::Url::parse("http://127.0.0.1/private").expect("loopback URL")
        ));

        let configured = ResourceUrlPolicy::new(vec![
            "127.0.0.1".to_string(),
            "*.svc.cluster.local".to_string(),
        ]);
        assert!(configured.permits_url_without_dns(
            &url::Url::parse("http://127.0.0.1/private").expect("allowed loopback URL")
        ));
        assert!(configured.allows_private_host("tiles.default.svc.cluster.local"));
        assert!(!configured.allows_private_host("svc.cluster.local"));

        let nat64_metadata: IpAddr = "64:ff9b::169.254.169.254".parse().expect("NAT64 address");
        assert!(is_forbidden_address(nat64_metadata));
    }

    #[tokio::test]
    async fn dns_filter_requires_explicit_private_host_allowlist() {
        let blocked = FilteringResolver::new(ResourceUrlPolicy::new(Vec::new()))
            .resolve("localhost".parse().expect("DNS name"))
            .await;
        assert!(blocked.is_err());

        let allowed = FilteringResolver::new(ResourceUrlPolicy::new(vec!["localhost".to_string()]))
            .resolve("localhost".parse().expect("DNS name"))
            .await
            .expect("explicit private hostname is allowed")
            .collect::<Vec<_>>();
        assert!(!allowed.is_empty());
        assert!(
            allowed
                .iter()
                .all(|address| is_forbidden_address(address.ip()))
        );
    }
}
