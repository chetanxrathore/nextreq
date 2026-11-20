//! `_nextreq`: the Rust core of the `nextreq` Python HTTP client.
//!
//! The Python package (`python/nextreq`) provides the ergonomic API; this
//! extension provides the I/O engine and the hot-path parsers:
//!
//! * [`client`] - connection pool, shared Tokio runtime, sync/async dispatch
//! * [`config`] - `Timeout`, `RetryPolicy`, proxy and TLS configuration
//! * [`sse`]    - WHATWG-compliant Server-Sent Events parser
//! * [`ws`]     - WebSocket client
//! * [`errors`] - typed Rust -> Python exception mapping

use pyo3::prelude::*;

mod client;
mod config;
mod errors;
mod sse;
mod ws;

/// Native extension module for nextreq.
///
/// PyO3 0.28+ declares modules free-threading compatible by default
/// (`Py_mod_gil = Py_MOD_GIL_NOT_USED`); every exposed class is `frozen` and
/// guards its state with Rust synchronisation primitives.
#[pymodule]
fn _nextreq(module: &Bound<'_, PyModule>) -> PyResult<()> {
    client::init_runtime();
    module.add_class::<client::HttpClient>()?;
    module.add_class::<client::ResponseStream>()?;
    module.add_class::<client::LoopBridge>()?;
    module.add_class::<client::TaskHandle>()?;
    module.add_class::<config::Timeout>()?;
    module.add_class::<config::RetryPolicy>()?;
    module.add_class::<sse::SSEParser>()?;
    module.add_class::<sse::PySseEvent>()?;
    module.add_class::<ws::WebSocketConnection>()?;
    module.add_function(wrap_pyfunction!(config::parse_retry_after, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add("HTTP3_ENABLED", cfg!(feature = "http3"))?;
    module.add("DEFAULT_CONNECT_TIMEOUT", config::DEFAULT_CONNECT_TIMEOUT)?;
    module.add("DEFAULT_READ_TIMEOUT", config::DEFAULT_READ_TIMEOUT)?;
    module.add("DEFAULT_WRITE_TIMEOUT", config::DEFAULT_WRITE_TIMEOUT)?;
    module.add("DEFAULT_POOL_TIMEOUT", config::DEFAULT_POOL_TIMEOUT)?;
    Ok(())
}
