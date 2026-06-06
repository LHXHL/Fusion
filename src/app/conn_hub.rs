use std::{
    io::{Error, ErrorKind},
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::app::config::{ConnPolicy, TunnelEndpoint};

static ROUND_ROBIN_CURSOR: AtomicUsize = AtomicUsize::new(0);

pub fn build_proxy_chain(front_proxy: Option<&str>, proxies: &[String]) -> Vec<String> {
    let mut chain = Vec::with_capacity(proxies.len() + usize::from(front_proxy.is_some()));
    if let Some(front_proxy) = front_proxy {
        chain.push(front_proxy.to_string());
    }
    chain.extend(proxies.iter().cloned());
    chain
}

pub fn order_endpoints(
    endpoints: &[TunnelEndpoint],
    policy: &ConnPolicy,
) -> Result<Vec<TunnelEndpoint>, Error> {
    if endpoints.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "no connect endpoints available",
        ));
    }
    let mut ordered = endpoints.to_vec();
    match policy {
        ConnPolicy::Fallback => {}
        ConnPolicy::Random => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as usize)
                .unwrap_or_default();
            ordered.sort_by_key(|endpoint| endpoint.url.original.len() ^ now);
            let len = ordered.len();
            ordered.rotate_left(now % len);
        }
        ConnPolicy::RoundRobin => {
            let idx = ROUND_ROBIN_CURSOR.fetch_add(1, Ordering::Relaxed);
            let len = ordered.len();
            ordered.rotate_left(idx % len);
        }
    }
    Ok(ordered)
}

pub fn select_upstream_pool(
    connects: &[TunnelEndpoint],
    up_connects: &[TunnelEndpoint],
) -> Vec<TunnelEndpoint> {
    if !up_connects.is_empty() {
        up_connects.to_vec()
    } else {
        connects.to_vec()
    }
}

pub fn select_downstream_pool(
    connects: &[TunnelEndpoint],
    down_connects: &[TunnelEndpoint],
) -> Vec<TunnelEndpoint> {
    if !down_connects.is_empty() {
        down_connects.to_vec()
    } else {
        connects.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use crate::{app::config::ConnPolicy, utils::url::ParsedUrl};

    use super::{build_proxy_chain, order_endpoints, select_downstream_pool, select_upstream_pool};

    fn endpoints() -> Vec<crate::app::config::TunnelEndpoint> {
        vec![
            crate::app::config::TunnelEndpoint {
                url: ParsedUrl::parse("tcp://127.0.0.1:1").unwrap(),
            },
            crate::app::config::TunnelEndpoint {
                url: ParsedUrl::parse("tcp://127.0.0.1:2").unwrap(),
            },
        ]
    }

    #[test]
    fn builds_proxy_chain_and_selects_pools() {
        let chain = build_proxy_chain(
            Some("socks5://127.0.0.1:9000"),
            &["http://127.0.0.1:8080".into()],
        );
        assert_eq!(chain.len(), 2);

        let connects = endpoints();
        let up = select_upstream_pool(&connects, &connects[1..]);
        assert_eq!(up.len(), 1);
        let down = select_downstream_pool(&connects, &[]);
        assert_eq!(down.len(), 2);
    }

    #[test]
    fn orders_endpoints_for_supported_policies() {
        let ordered = order_endpoints(&endpoints(), &ConnPolicy::Fallback).unwrap();
        assert_eq!(ordered.len(), 2);
        let rr = order_endpoints(&endpoints(), &ConnPolicy::RoundRobin).unwrap();
        assert_eq!(rr.len(), 2);
        let rnd = order_endpoints(&endpoints(), &ConnPolicy::Random).unwrap();
        assert_eq!(rnd.len(), 2);
    }
}
