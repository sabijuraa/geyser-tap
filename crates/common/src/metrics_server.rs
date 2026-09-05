//! Prometheus exposition and health endpoints.
//!
//! The counters in [`crate::metrics`] register into the prometheus default
//! registry and are incremented as the plugin runs, but until this module
//! existed nothing ever gathered or served them: there was no `gather()`, no
//! encoder and no listener, so `metrics.bind_address` was parsed from config
//! and then never used. The plugin could not be scraped at all.
//!
//! This serves three endpoints over plain HTTP/1.1:
//!
//! | Path            | Meaning                                                |
//! |-----------------|--------------------------------------------------------|
//! | `/metrics`      | Prometheus text exposition of the default registry      |
//! | `/health/live`  | 200 once the server is accepting; the process is alive  |
//! | `/health/ready` | 200 when the plugin has finished starting its sinks,    |
//! |                 | 503 before that                                         |
//!
//! Liveness and readiness are deliberately different: the exporter binds
//! early, so `/health/live` answers while sinks are still coming up.
//! `/health/ready` only flips once [`Readiness::set_ready`] is called, so an
//! orchestrator does not route traffic to a plugin whose sinks have not bound.

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use prometheus::{Encoder, TextEncoder};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Shared readiness flag for `/health/ready`.
///
/// Cloneable; all clones observe the same flag.
#[derive(Clone, Debug, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    /// Create a new readiness flag in the not-ready state.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Mark the plugin ready to serve traffic.
    pub fn set_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Mark the plugin not ready (e.g. during shutdown).
    pub fn set_not_ready(&self) {
        self.0.store(false, Ordering::SeqCst);
    }

    /// Whether the plugin is currently ready.
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Gather the default registry and encode it in Prometheus text format.
///
/// Exposed separately from the HTTP layer so it can be tested without binding
/// a port.
pub fn encode_metrics() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::with_capacity(8 * 1024);
    encoder.encode(&families, &mut buf)?;
    String::from_utf8(buf)
        .map_err(|e| prometheus::Error::Msg(format!("metrics were not valid utf-8: {e}")))
}

async fn route(req: Request<Body>, readiness: Readiness) -> Result<Response<Body>, Infallible> {
    if req.method() != Method::GET {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from("method not allowed\n"))
            .expect("static response is valid"));
    }

    let response = match req.uri().path() {
        "/metrics" => match encode_metrics() {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                // The version marker is what Prometheus expects for text
                // exposition; without it some scrapers fall back to guessing.
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Body::from(body)),
            Err(e) => {
                tracing::error!(error = %e, "failed to encode metrics");
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(format!("failed to encode metrics: {e}\n")))
            }
        },

        "/health/live" => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from("{\"status\":\"live\"}\n")),

        "/health/ready" => {
            if readiness.is_ready() {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::from("{\"status\":\"ready\"}\n"))
            } else {
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        "{\"status\":\"not_ready\",\"reason\":\"sinks still starting\"}\n",
                    ))
            }
        }

        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found\n")),
    };

    Ok(response.expect("response construction is infallible for static bodies"))
}

/// Bind the metrics/health server and serve until the process exits.
///
/// Binds before returning so a bind failure (port in use, permission denied)
/// is reported to the caller instead of disappearing into a detached task.
/// The accept loop itself is spawned onto the current runtime.
pub async fn start(bind_address: SocketAddr, readiness: Readiness) -> std::io::Result<SocketAddr> {
    // hyper reports bind failures as hyper::Error; surface it as io::Error so
    // callers see the usual AddrInUse/PermissionDenied shape.
    let builder = Server::try_bind(&bind_address).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!("failed to bind metrics server to {bind_address}: {e}"),
        )
    })?;

    let make_svc = make_service_fn(move |_conn| {
        let readiness = readiness.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| route(req, readiness.clone()))) }
    });

    let server = builder.serve(make_svc);
    let local_addr = server.local_addr();

    tokio::spawn(async move {
        if let Err(e) = server.await {
            tracing::error!(error = %e, "metrics server terminated");
        }
    });

    Ok(local_addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_starts_not_ready_and_flips() {
        let r = Readiness::new();
        assert!(!r.is_ready());
        r.set_ready();
        assert!(r.is_ready());
        r.set_not_ready();
        assert!(!r.is_ready());
    }

    #[test]
    fn readiness_clones_share_state() {
        let a = Readiness::new();
        let b = a.clone();
        a.set_ready();
        assert!(b.is_ready(), "clones must observe the same flag");
    }

    #[test]
    fn encode_metrics_emits_registered_counters() {
        crate::metrics::record_update_received("account");
        let text = encode_metrics().expect("encoding should succeed");
        assert!(
            text.contains("geyser_tap_updates_received_total"),
            "exposition should contain the registered counter, got:\n{text}"
        );
        // TYPE metadata is what makes the output valid exposition rather than
        // just numbers.
        assert!(text.contains("# TYPE geyser_tap_updates_received_total counter"));
    }
}
