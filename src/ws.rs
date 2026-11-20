//! WebSocket client wrapper built on `tokio-tungstenite`.
//!
//! The connection is split into a sink and a stream so one task can receive
//! while another sends. Each half is guarded by an async mutex, which makes
//! the handle safe to share between asyncio tasks (and Python threads).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http::{HeaderName, HeaderValue};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use pyo3::IntoPyObjectExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};

use crate::client::Outcome;
use crate::errors::{chain_message, ErrorKind, NxError, NxResult};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Close code reported when the peer vanished without a close frame.
const ABNORMAL_CLOSURE: u16 = 1006;
/// How long `close()` waits for the peer's close acknowledgement.
const CLOSE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

struct Connection {
    sink: Mutex<SplitSink<Socket, Message>>,
    stream: Mutex<SplitStream<Socket>>,
    close_sent: AtomicBool,
}

/// A message received from the peer.
pub enum Incoming {
    Text(String),
    Binary(Bytes),
    Closed(u16, String),
}

impl<'py> IntoPyObject<'py> for Incoming {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    /// `(kind, payload, code)`: kind 0 = text (`str`), 1 = binary (`bytes`),
    /// 2 = closed (payload is the close reason, `code` the close code).
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let (kind, payload, code) = match self {
            Incoming::Text(text) => (0u8, text.into_bound_py_any(py)?, 0u16),
            Incoming::Binary(data) => (1, PyBytes::new(py, &data).into_any(), 0),
            Incoming::Closed(code, reason) => (2, reason.into_bound_py_any(py)?, code),
        };
        PyTuple::new(
            py,
            [
                kind.into_bound_py_any(py)?,
                payload,
                code.into_bound_py_any(py)?,
            ],
        )
    }
}

fn map_ws_error(err: WsError) -> NxError {
    match err {
        WsError::Http(response) => NxError::new(
            ErrorKind::WebSocketError,
            format!(
                "WebSocket handshake failed: server responded with HTTP {}",
                response.status().as_u16()
            ),
        ),
        WsError::Url(e) => {
            NxError::new(ErrorKind::InvalidURL, format!("invalid WebSocket URL: {e}"))
        }
        WsError::Io(e) => NxError::new(ErrorKind::ConnectError, chain_message(&e)),
        WsError::Tls(e) => NxError::new(ErrorKind::ConnectError, chain_message(&e)),
        WsError::ConnectionClosed | WsError::AlreadyClosed => NxError::new(
            ErrorKind::WebSocketError,
            "the WebSocket connection is closed",
        ),
        other => NxError::new(ErrorKind::WebSocketError, chain_message(&other)),
    }
}

/// Open a WebSocket connection.
pub async fn connect(
    url: String,
    headers: Vec<(String, String)>,
    subprotocols: Vec<String>,
    tls: Arc<rustls::ClientConfig>,
    connect_timeout: Option<Duration>,
    max_message_size: Option<usize>,
) -> NxResult<WebSocketConnection> {
    let mut request = url.as_str().into_client_request().map_err(map_ws_error)?;
    match request.uri().scheme_str() {
        Some("ws") | Some("wss") => {}
        other => {
            return Err(NxError::new(
                ErrorKind::UnsupportedProtocol,
                format!(
                    "unsupported WebSocket URL scheme {:?} in {url:?} (expected ws or wss)",
                    other.unwrap_or("")
                ),
            ))
        }
    }
    for (name, value) in &headers {
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) else {
            return Err(NxError::new(
                ErrorKind::LocalProtocolError,
                format!("invalid header {name:?}"),
            ));
        };
        request.headers_mut().append(name, value);
    }
    if !subprotocols.is_empty() {
        let value = HeaderValue::from_str(&subprotocols.join(", ")).map_err(|_| {
            NxError::new(
                ErrorKind::LocalProtocolError,
                "invalid WebSocket subprotocol",
            )
        })?;
        request
            .headers_mut()
            .insert("sec-websocket-protocol", value);
    }

    let mut config = WebSocketConfig::default();
    if let Some(limit) = max_message_size {
        config = config.max_message_size(Some(limit));
        config = config.max_frame_size(Some(limit));
    }
    let handshake =
        connect_async_tls_with_config(request, Some(config), true, Some(Connector::Rustls(tls)));
    let (socket, response) = match connect_timeout {
        Some(limit) => tokio::time::timeout(limit, handshake).await.map_err(|_| {
            NxError::new(
                ErrorKind::ConnectTimeout,
                format!(
                    "timed out after {:.3}s opening WebSocket connection",
                    limit.as_secs_f64()
                ),
            )
        })?,
        None => handshake.await,
    }
    .map_err(map_ws_error)?;

    let response_headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let subprotocol = response
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let (sink, stream) = socket.split();
    Ok(WebSocketConnection {
        conn: Arc::new(Connection {
            sink: Mutex::new(sink),
            stream: Mutex::new(stream),
            close_sent: AtomicBool::new(false),
        }),
        subprotocol,
        response_headers,
    })
}

async fn send(conn: &Connection, message: Message) -> NxResult<()> {
    let mut sink = conn.sink.lock().await;
    sink.send(message).await.map_err(map_ws_error)
}

async fn receive(conn: &Connection) -> NxResult<Incoming> {
    let mut stream = conn.stream.lock().await;
    loop {
        match stream.next().await {
            None => return Ok(Incoming::Closed(ABNORMAL_CLOSURE, String::new())),
            Some(Ok(Message::Text(text))) => return Ok(Incoming::Text(text.as_str().to_owned())),
            Some(Ok(Message::Binary(data))) => return Ok(Incoming::Binary(data)),
            Some(Ok(Message::Close(frame))) => {
                // tungstenite queued the close reply; make sure it is flushed.
                if let Ok(mut sink) = conn.sink.try_lock() {
                    let _ = sink.flush().await;
                }
                return Ok(match frame {
                    Some(frame) => {
                        Incoming::Closed(u16::from(frame.code), frame.reason.as_str().to_owned())
                    }
                    None => Incoming::Closed(1005, String::new()),
                });
            }
            // Pings are answered automatically; pongs and raw frames are skipped.
            Some(Ok(_)) => continue,
            Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => {
                return Ok(Incoming::Closed(ABNORMAL_CLOSURE, String::new()))
            }
            Some(Err(WsError::Io(e)))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                ) =>
            {
                return Ok(Incoming::Closed(ABNORMAL_CLOSURE, e.to_string()))
            }
            Some(Err(err)) => return Err(map_ws_error(err)),
        }
    }
}

async fn close(conn: &Connection, code: u16, reason: String) -> NxResult<()> {
    if conn.close_sent.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    {
        let mut sink = conn.sink.lock().await;
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: reason.into(),
        };
        match sink.send(Message::Close(Some(frame))).await {
            Ok(()) | Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => {}
            Err(WsError::Io(_)) | Err(WsError::Protocol(_)) => return Ok(()),
            Err(err) => return Err(map_ws_error(err)),
        }
    }
    // Wait briefly for the peer's close frame so the TCP teardown is clean.
    // Skipped if another task is currently blocked in `receive()`.
    if let Ok(mut stream) = conn.stream.try_lock() {
        let _ = tokio::time::timeout(CLOSE_HANDSHAKE_TIMEOUT, async {
            while let Some(Ok(message)) = stream.next().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        })
        .await;
    }
    Ok(())
}

/// An open WebSocket connection. All methods return awaitables.
#[pyclass(frozen, module = "nextreq._nextreq", name = "WebSocketConnection")]
pub struct WebSocketConnection {
    conn: Arc<Connection>,
    subprotocol: Option<String>,
    response_headers: Vec<(String, String)>,
}

#[pymethods]
impl WebSocketConnection {
    /// Send a text frame.
    fn send_text<'py>(&self, py: Python<'py>, text: String) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Outcome(send(&conn, Message::text(text)).await))
        })
    }

    /// Send a binary frame.
    fn send_bytes<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Outcome(send(&conn, Message::binary(data)).await))
        })
    }

    /// Send a ping frame (payload must be at most 125 bytes).
    #[pyo3(signature = (data = Vec::new()))]
    fn ping<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Outcome(send(&conn, Message::Ping(data.into())).await))
        })
    }

    /// Receive the next message as `(kind, payload, code)`.
    ///
    /// kind 0 = text, 1 = binary, 2 = connection closed.
    fn receive<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { Ok(Outcome(receive(&conn).await)) },
        )
    }

    /// Perform the closing handshake.
    #[pyo3(signature = (code = 1000, reason = String::new()))]
    fn close<'py>(
        &self,
        py: Python<'py>,
        code: u16,
        reason: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Outcome(close(&conn, code, reason).await))
        })
    }

    /// The subprotocol selected by the server, if any.
    #[getter]
    fn subprotocol(&self) -> Option<String> {
        self.subprotocol.clone()
    }

    /// Headers of the handshake response.
    #[getter]
    fn response_headers(&self) -> Vec<(String, String)> {
        self.response_headers.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_and_protocol_errors_map_to_websocket_error() {
        assert_eq!(
            map_ws_error(WsError::ConnectionClosed).kind,
            ErrorKind::WebSocketError
        );
        assert_eq!(
            map_ws_error(WsError::AlreadyClosed).kind,
            ErrorKind::WebSocketError
        );
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(map_ws_error(WsError::Io(io)).kind, ErrorKind::ConnectError);
    }

    #[test]
    fn rejects_non_websocket_schemes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let tls = Arc::new(
            crate::config::build_tls_config(&crate::config::TlsOptions::default(), &[b"http/1.1"])
                .unwrap(),
        );
        let result = rt.block_on(connect(
            "http://127.0.0.1:1/".to_owned(),
            Vec::new(),
            Vec::new(),
            tls,
            Some(Duration::from_secs(1)),
            None,
        ));
        assert_eq!(result.err().unwrap().kind, ErrorKind::UnsupportedProtocol);
    }
}
