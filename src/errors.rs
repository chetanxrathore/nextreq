//! Typed Rust -> Python exception mapping.
//!
//! The exception hierarchy itself lives in `nextreq/exceptions.py` so that it
//! can use multiple inheritance (e.g. `ConnectTimeout` is also a builtin
//! `TimeoutError`). Rust code produces a lightweight [`NxError`] that carries
//! no Python state, so it can be created and moved between Tokio worker threads
//! without touching the interpreter. It is only turned into a real Python
//! exception at the boundary, while attached to the interpreter.

use std::error::Error as StdError;
use std::fmt;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;

/// Which Python exception class an [`NxError`] maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    ConnectTimeout,
    ReadTimeout,
    WriteTimeout,
    PoolTimeout,
    ConnectError,
    ReadError,
    WriteError,
    ProxyError,
    RemoteProtocolError,
    LocalProtocolError,
    UnsupportedProtocol,
    InvalidURL,
    DecodingError,
    StreamClosed,
    ClientClosed,
    WebSocketError,
    TransportError,
}

impl ErrorKind {
    /// Name of the class in `nextreq.exceptions`.
    pub fn class_name(self) -> &'static str {
        match self {
            ErrorKind::ConnectTimeout => "ConnectTimeout",
            ErrorKind::ReadTimeout => "ReadTimeout",
            ErrorKind::WriteTimeout => "WriteTimeout",
            ErrorKind::PoolTimeout => "PoolTimeout",
            ErrorKind::ConnectError => "ConnectError",
            ErrorKind::ReadError => "ReadError",
            ErrorKind::WriteError => "WriteError",
            ErrorKind::ProxyError => "ProxyError",
            ErrorKind::RemoteProtocolError => "RemoteProtocolError",
            ErrorKind::LocalProtocolError => "LocalProtocolError",
            ErrorKind::UnsupportedProtocol => "UnsupportedProtocol",
            ErrorKind::InvalidURL => "InvalidURL",
            ErrorKind::DecodingError => "DecodingError",
            ErrorKind::StreamClosed => "StreamClosed",
            ErrorKind::ClientClosed => "ClientClosed",
            ErrorKind::WebSocketError => "WebSocketError",
            ErrorKind::TransportError => "TransportError",
        }
    }
}

/// A GIL-free error value produced by the Rust core.
#[derive(Debug, Clone)]
pub struct NxError {
    pub kind: ErrorKind,
    pub message: String,
}

impl NxError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        NxError {
            kind,
            message: message.into(),
        }
    }

    /// Build the Python exception instance. Must be attached to the interpreter.
    pub fn into_pyerr(self, py: Python<'_>) -> PyErr {
        static EXCEPTIONS: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
        let build = || -> PyResult<PyErr> {
            let module = EXCEPTIONS
                .get_or_try_init(py, || {
                    PyModule::import(py, "nextreq.exceptions").map(Bound::unbind)
                })?
                .bind(py);
            let class = module.getattr(self.kind.class_name())?;
            let instance = class.call1((self.message.as_str(),))?;
            Ok(PyErr::from_value(instance))
        };
        match build() {
            Ok(err) => err,
            Err(err) => err,
        }
    }
}

impl fmt::Display for NxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.class_name(), self.message)
    }
}

impl StdError for NxError {}

pub type NxResult<T> = Result<T, NxError>;

/// Marker error injected by our connector layer when the connect phase
/// (DNS + TCP + TLS handshake) exceeds the connect timeout.
#[derive(Debug)]
pub struct ConnectTimedOut;

impl fmt::Display for ConnectTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("timed out while connecting")
    }
}

impl StdError for ConnectTimedOut {}

/// Flatten an error and its `source()` chain into one readable message,
/// skipping segments that merely repeat their parent.
pub fn chain_message(err: &(dyn StdError + 'static)) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut current: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = current {
        let text = e.to_string();
        if !text.is_empty() && !parts.iter().any(|p| p.contains(&text)) {
            parts.push(text);
        }
        current = e.source();
    }
    parts.join(": ")
}

/// Facts gathered by walking an error's source chain.
#[derive(Default)]
struct ChainFacts {
    connect_timed_out: bool,
    incomplete_message: bool,
    parse_error: bool,
    canceled: bool,
    io_kind: Option<std::io::ErrorKind>,
}

fn inspect_chain(err: &(dyn StdError + 'static)) -> ChainFacts {
    let mut facts = ChainFacts::default();
    let mut current: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = current {
        if e.is::<ConnectTimedOut>() {
            facts.connect_timed_out = true;
        }
        if let Some(h) = e.downcast_ref::<hyper::Error>() {
            if h.is_incomplete_message() {
                facts.incomplete_message = true;
            }
            if h.is_parse() || h.is_parse_status() {
                facts.parse_error = true;
            }
            if h.is_canceled() || h.is_closed() {
                facts.canceled = true;
            }
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if facts.io_kind.is_none() {
                facts.io_kind = Some(io.kind());
            }
        }
        current = e.source();
    }
    facts
}

/// Where in the request lifecycle a `reqwest::Error` surfaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Sending the request / waiting for response headers.
    Send,
    /// Reading the response body.
    Body,
}

/// Map a `reqwest::Error` onto the typed exception hierarchy.
pub fn classify(err: reqwest::Error, phase: Phase) -> NxError {
    let facts = inspect_chain(&err);
    let message = chain_message(&err);
    let lowered = message.to_ascii_lowercase();

    let kind = if facts.connect_timed_out {
        ErrorKind::ConnectTimeout
    } else if err.is_timeout() {
        if err.is_connect() {
            ErrorKind::ConnectTimeout
        } else {
            ErrorKind::ReadTimeout
        }
    } else if err.is_builder() {
        if lowered.contains("scheme") {
            ErrorKind::UnsupportedProtocol
        } else {
            ErrorKind::InvalidURL
        }
    } else if err.is_connect() {
        if lowered.contains("proxy") || lowered.contains("tunnel") || lowered.contains("socks") {
            ErrorKind::ProxyError
        } else {
            ErrorKind::ConnectError
        }
    } else if err.is_decode() && phase == Phase::Body && facts.io_kind.is_none() {
        // Corrupt gzip/br/zstd/deflate stream, or a malformed chunked body.
        if facts.incomplete_message || facts.parse_error {
            ErrorKind::RemoteProtocolError
        } else {
            ErrorKind::DecodingError
        }
    } else if facts.incomplete_message || facts.parse_error {
        ErrorKind::RemoteProtocolError
    } else if let Some(io_kind) = facts.io_kind {
        use std::io::ErrorKind as Io;
        match io_kind {
            Io::BrokenPipe if phase == Phase::Send => ErrorKind::WriteError,
            Io::UnexpectedEof => ErrorKind::RemoteProtocolError,
            Io::InvalidData if phase == Phase::Body => ErrorKind::DecodingError,
            _ => ErrorKind::ReadError,
        }
    } else if facts.canceled {
        ErrorKind::RemoteProtocolError
    } else if phase == Phase::Body {
        ErrorKind::ReadError
    } else {
        ErrorKind::TransportError
    };
    NxError::new(kind, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Outer(Box<dyn StdError + Send + Sync>);
    impl fmt::Display for Outer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("request failed")
        }
    }
    impl StdError for Outer {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(self.0.as_ref())
        }
    }

    #[test]
    fn chain_message_joins_sources() {
        let err = Outer(Box::new(ConnectTimedOut));
        assert_eq!(
            chain_message(&err),
            "request failed: timed out while connecting"
        );
    }

    #[test]
    fn chain_message_skips_duplicates() {
        let inner = std::io::Error::other("request failed");
        let err = Outer(Box::new(inner));
        assert_eq!(chain_message(&err), "request failed");
    }

    #[test]
    fn inspect_chain_finds_connect_timeout_and_io_kind() {
        let err = Outer(Box::new(ConnectTimedOut));
        assert!(inspect_chain(&err).connect_timed_out);

        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        let err = Outer(Box::new(io));
        let facts = inspect_chain(&err);
        assert!(!facts.connect_timed_out);
        assert_eq!(facts.io_kind, Some(std::io::ErrorKind::ConnectionReset));
    }

    #[test]
    fn every_kind_has_a_class_name() {
        let kinds = [
            ErrorKind::ConnectTimeout,
            ErrorKind::ReadTimeout,
            ErrorKind::WriteTimeout,
            ErrorKind::PoolTimeout,
            ErrorKind::ConnectError,
            ErrorKind::ReadError,
            ErrorKind::WriteError,
            ErrorKind::ProxyError,
            ErrorKind::RemoteProtocolError,
            ErrorKind::LocalProtocolError,
            ErrorKind::UnsupportedProtocol,
            ErrorKind::InvalidURL,
            ErrorKind::DecodingError,
            ErrorKind::StreamClosed,
            ErrorKind::ClientClosed,
            ErrorKind::WebSocketError,
            ErrorKind::TransportError,
        ];
        for kind in kinds {
            assert!(!kind.class_name().is_empty());
        }
        let err = NxError::new(ErrorKind::ReadTimeout, "boom");
        assert_eq!(err.to_string(), "ReadTimeout: boom");
    }
}
