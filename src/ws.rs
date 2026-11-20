//! WebSocket client.
//!
//! The connection is established here - TCP (optionally through an HTTP
//! `CONNECT` or SOCKS5 proxy), TLS with the client's own rustls configuration,
//! then the HTTP/1.1 upgrade - and the framing, including permessage-deflate
//! (RFC 7692), is handled by `yawc`.
//!
//! Each connection is owned by one task (the *actor*). Handles talk to it over
//! channels, so sending never waits for a receive in progress, every operation
//! is cancel-safe, and the same handle serves the blocking and the awaitable
//! API.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use futures_util::SinkExt;
use http::header::{
    HeaderName, HeaderValue, CONNECTION, HOST, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_EXTENSIONS,
    SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use http_body_util::Empty;
use hyper_util::rt::TokioIo;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use pyo3::IntoPyObjectExt;
use reqwest::Url;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use yawc::frame::{Frame, OpCode};
use yawc::{DeflateOptions, Options, WebSocketError};

use crate::client::{block_on_interruptible, spawn_awaitable, Outcome};
use crate::errors::{chain_message, ErrorKind, NxError, NxResult};

/// Close code reported when the peer vanished without a close frame.
const ABNORMAL_CLOSURE: u16 = 1006;
/// Close code reported for a close frame that carried no status code.
const NO_STATUS_RECEIVED: u16 = 1005;
/// How long `close()` waits for the peer's close acknowledgement.
const CLOSE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
/// Messages the actor may read ahead of the application.
const INCOMING_QUEUE: usize = 32;
/// Largest message accepted when `max_message_size` is not given.
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
/// Largest proxy `CONNECT` response head that is accepted.
const MAX_PROXY_RESPONSE: usize = 16 * 1024;
const WEBSOCKET_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
type BoxIo = Box<dyn Io>;

/// A stream that first replays bytes which were read past the end of the
/// handshake response (a server may send frames in the same packet).
struct Rewind {
    prefix: Bytes,
    inner: BoxIo,
}

impl AsyncRead for Rewind {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if !self.prefix.is_empty() {
            let take = self.prefix.len().min(buf.remaining());
            buf.put_slice(&self.prefix.split_to(take));
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Rewind {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

type Socket = yawc::WebSocket<Rewind>;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

fn ws_error(message: impl Into<String>) -> NxError {
    NxError::new(ErrorKind::WebSocketError, message)
}

fn closed_error() -> NxError {
    ws_error("the WebSocket connection is closed")
}

fn connect_error(err: impl std::error::Error + 'static) -> NxError {
    NxError::new(ErrorKind::ConnectError, chain_message(&err))
}

fn map_ws_error(err: WebSocketError) -> NxError {
    match err {
        WebSocketError::ConnectionClosed => closed_error(),
        WebSocketError::IoError(e) => connect_error(e),
        other => ws_error(chain_message(&other)),
    }
}

fn is_disconnect(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
    )
}

// ---------------------------------------------------------------------------
// Proxy selection
// ---------------------------------------------------------------------------

fn env_var(names: &[&str]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .find(|value| !value.trim().is_empty())
}

/// Whether `host` is excluded from proxying by a `NO_PROXY` style list:
/// `*`, exact hosts and IPs, domain suffixes (`example.com`, `.example.com`)
/// and CIDR blocks (`10.0.0.0/8`).
fn no_proxy_matches(list: &str, host: &str) -> bool {
    let host = host
        .trim_matches(|c| c == '[' || c == ']')
        .to_ascii_lowercase();
    let address = host.parse::<std::net::IpAddr>().ok();
    list.split(',').map(str::trim).any(|entry| {
        let entry = entry.to_ascii_lowercase();
        if entry.is_empty() {
            return false;
        }
        if entry == "*" {
            return true;
        }
        if let Some(address) = address {
            return match entry.split_once('/') {
                Some((network, bits)) => match (network.parse(), bits.parse::<u32>()) {
                    (Ok(network), Ok(bits)) => cidr_contains(network, bits, address),
                    _ => false,
                },
                None => entry.trim_matches(|c| c == '[' || c == ']').parse() == Ok(address),
            };
        }
        let suffix = entry.trim_start_matches('.');
        host == suffix || host.ends_with(&format!(".{suffix}"))
    })
}

fn cidr_contains(network: std::net::IpAddr, bits: u32, address: std::net::IpAddr) -> bool {
    use std::net::IpAddr::{V4, V6};
    match (network, address) {
        (V4(network), V4(address)) if bits <= 32 => {
            let mask = u32::MAX.checked_shl(32 - bits).unwrap_or(0);
            u32::from(network) & mask == u32::from(address) & mask
        }
        (V6(network), V6(address)) if bits <= 128 => {
            let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
            u128::from(network) & mask == u128::from(address) & mask
        }
        _ => false,
    }
}

/// The proxy to use for `url`, following the same rules as HTTP requests:
/// explicit proxies first (`http` covers `ws://`, `https` covers `wss://`),
/// then - with `trust_env` - the proxy environment variables. `NO_PROXY` is
/// honoured whenever `trust_env` is set.
pub fn proxy_for(
    url: &Url,
    proxies: &[(String, String)],
    trust_env: bool,
) -> NxResult<Option<Url>> {
    let secure = url.scheme() == "wss";
    let host = url.host_str().unwrap_or_default();
    if trust_env {
        if let Some(list) = env_var(&["NO_PROXY", "no_proxy"]) {
            if no_proxy_matches(&list, host) {
                return Ok(None);
            }
        }
    }
    let wanted = if secure { "https" } else { "http" };
    let explicit = proxies
        .iter()
        .find(|(scope, _)| scope == wanted || scope == "all")
        .map(|(_, proxy)| proxy.clone());
    let chosen = match explicit {
        Some(proxy) => Some(proxy),
        None if trust_env => {
            let specific: &[&str] = if secure {
                &["HTTPS_PROXY", "https_proxy"]
            } else {
                &["HTTP_PROXY", "http_proxy"]
            };
            env_var(specific).or_else(|| env_var(&["ALL_PROXY", "all_proxy"]))
        }
        None => None,
    };
    let Some(chosen) = chosen else {
        return Ok(None);
    };
    // Environment variables commonly omit the scheme ("proxy.local:3128").
    let text = if chosen.contains("://") {
        chosen
    } else {
        format!("http://{chosen}")
    };
    Url::parse(&text).map(Some).map_err(|e| {
        NxError::new(
            ErrorKind::InvalidURL,
            format!("invalid proxy URL {text:?}: {e}"),
        )
    })
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escaped = (bytes[index] == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                out.push(byte);
                index += 3;
            }
            None => {
                out.push(bytes[index]);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `host:port` as used in a request target or `Host` header (IPv6 in brackets).
fn authority(url: &Url, always_port: bool) -> String {
    let host = url.host_str().unwrap_or_default();
    match (url.port(), always_port) {
        (Some(port), _) => format!("{host}:{port}"),
        (None, true) => format!("{host}:{}", url.port_or_known_default().unwrap_or(80)),
        (None, false) => host.to_owned(),
    }
}

async fn tcp_connect(url: &Url) -> NxResult<TcpStream> {
    let host = url
        .host_str()
        .unwrap_or_default()
        .trim_matches(|c| c == '[' || c == ']');
    let port = url.port_or_known_default().unwrap_or(80);
    let stream = TcpStream::connect((host, port))
        .await
        .map_err(connect_error)?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

/// Open a tunnel to `target` through an HTTP proxy with `CONNECT`.
async fn http_tunnel(proxy: &Url, target: &Url) -> NxResult<BoxIo> {
    let mut stream = tcp_connect(proxy).await?;
    let destination = authority(target, true);
    let mut request = format!(
        "CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\nProxy-Connection: Keep-Alive\r\n"
    );
    if !proxy.username().is_empty() {
        let credentials = format!(
            "{}:{}",
            percent_decode(proxy.username()),
            percent_decode(proxy.password().unwrap_or_default())
        );
        request.push_str(&format!(
            "Proxy-Authorization: Basic {}\r\n",
            base64::engine::general_purpose::STANDARD.encode(credentials)
        ));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(connect_error)?;

    // Nothing follows the response head until this side speaks again, so
    // reading in blocks cannot swallow tunnelled data.
    let mut head = Vec::with_capacity(512);
    let mut block = [0u8; 512];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() > MAX_PROXY_RESPONSE {
            return Err(NxError::new(
                ErrorKind::ProxyError,
                "the proxy sent an oversized response to CONNECT",
            ));
        }
        let read = stream.read(&mut block).await.map_err(connect_error)?;
        if read == 0 {
            return Err(NxError::new(
                ErrorKind::ProxyError,
                "the proxy closed the connection during CONNECT",
            ));
        }
        head.extend_from_slice(&block[..read]);
    }
    let status_line =
        String::from_utf8_lossy(head.split(|b| *b == b'\r').next().unwrap_or(&[])).into_owned();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok());
    match status {
        Some(code) if (200..300).contains(&code) => Ok(Box::new(stream)),
        Some(code) => Err(NxError::new(
            ErrorKind::ProxyError,
            format!("the proxy refused the tunnel to {destination}: HTTP {code}"),
        )),
        None => Err(NxError::new(
            ErrorKind::ProxyError,
            format!("the proxy sent an invalid response to CONNECT: {status_line:?}"),
        )),
    }
}

async fn open_transport(
    url: &Url,
    proxy: Option<&Url>,
    tls: Arc<rustls::ClientConfig>,
) -> NxResult<BoxIo> {
    let stream: BoxIo = match proxy {
        None => Box::new(tcp_connect(url).await?),
        Some(proxy) => match proxy.scheme() {
            "http" => http_tunnel(proxy, url).await?,
            "socks5" | "socks5h" => {
                let socks = yawc::Proxy::socks5(proxy.clone())
                    .map_err(|e| NxError::new(ErrorKind::ProxyError, chain_message(&e)))?;
                let stream = socks
                    .connect(url, None)
                    .await
                    .map_err(|e| NxError::new(ErrorKind::ProxyError, chain_message(&e)))?;
                let _ = stream.set_nodelay(true);
                Box::new(stream)
            }
            other => {
                return Err(NxError::new(
                    ErrorKind::ProxyError,
                    format!(
                        "unsupported proxy scheme {other:?} for WebSocket connections \
                         (expected http, socks5 or socks5h)"
                    ),
                ))
            }
        },
    };
    if url.scheme() != "wss" {
        return Ok(stream);
    }
    let host = url
        .host_str()
        .unwrap_or_default()
        .trim_matches(|c| c == '[' || c == ']')
        .to_owned();
    let server_name = rustls::pki_types::ServerName::try_from(host.clone()).map_err(|_| {
        NxError::new(
            ErrorKind::InvalidURL,
            format!("invalid TLS server name {host:?}"),
        )
    })?;
    let secured = tokio_rustls::TlsConnector::from(tls)
        .connect(server_name, stream)
        .await
        .map_err(connect_error)?;
    Ok(Box::new(secured))
}

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

/// Everything needed to open a connection, gathered while attached.
pub struct ConnectOptions {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subprotocols: Vec<String>,
    pub tls: Arc<rustls::ClientConfig>,
    pub proxies: Vec<(String, String)>,
    pub trust_env: bool,
    pub connect_timeout: Option<Duration>,
    pub max_message_size: Option<usize>,
    pub compression: bool,
}

fn accept_key(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WEBSOCKET_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn header_contains_token(value: Option<&HeaderValue>, token: &str) -> bool {
    value.and_then(|v| v.to_str().ok()).is_some_and(|v| {
        v.split(',')
            .any(|part| part.trim().eq_ignore_ascii_case(token))
    })
}

async fn handshake(
    options: &ConnectOptions,
    url: &Url,
    io: BoxIo,
) -> NxResult<WebSocketConnection> {
    let mut nonce = [0u8; 16];
    nonce.iter_mut().for_each(|byte| *byte = fastrand::u8(..));
    let key = base64::engine::general_purpose::STANDARD.encode(nonce);

    let mut target = url.path().to_owned();
    if target.is_empty() {
        target.push('/');
    }
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    let mut request = http::Request::builder()
        .method("GET")
        .uri(target)
        .body(Empty::<Bytes>::new())
        .map_err(|e| NxError::new(ErrorKind::InvalidURL, format!("invalid WebSocket URL: {e}")))?;
    let invalid_header = |name: &str| {
        NxError::new(
            ErrorKind::LocalProtocolError,
            format!("invalid header {name:?}"),
        )
    };
    let headers = request.headers_mut();
    for (name, value) in &options.headers {
        let (Ok(header_name), Ok(header_value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) else {
            return Err(invalid_header(name));
        };
        headers.append(header_name, header_value);
    }
    if !headers.contains_key(HOST) {
        let host =
            HeaderValue::from_str(&authority(url, false)).map_err(|_| invalid_header("host"))?;
        headers.insert(HOST, host);
    }
    headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
    headers.insert(
        SEC_WEBSOCKET_KEY,
        HeaderValue::from_str(&key).map_err(|_| invalid_header("sec-websocket-key"))?,
    );
    if !options.subprotocols.is_empty() {
        let value = HeaderValue::from_str(&options.subprotocols.join(", ")).map_err(|_| {
            NxError::new(
                ErrorKind::LocalProtocolError,
                "invalid WebSocket subprotocol",
            )
        })?;
        headers.insert(SEC_WEBSOCKET_PROTOCOL, value);
    }
    if options.compression {
        headers.insert(
            SEC_WEBSOCKET_EXTENSIONS,
            HeaderValue::from_static("permessage-deflate"),
        );
    }

    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(connect_error)?;
    tokio::spawn(async move {
        let _ = connection.with_upgrades().await;
    });
    let mut response = sender.send_request(request).await.map_err(connect_error)?;

    if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(ws_error(format!(
            "WebSocket handshake failed: server responded with HTTP {}",
            response.status().as_u16()
        )));
    }
    let response_headers = response.headers();
    if !header_contains_token(response_headers.get(UPGRADE), "websocket")
        || !header_contains_token(response_headers.get(CONNECTION), "upgrade")
    {
        return Err(ws_error(
            "WebSocket handshake failed: the server did not upgrade the connection",
        ));
    }
    let accepted = response_headers
        .get(SEC_WEBSOCKET_ACCEPT)
        .is_some_and(|value| value.as_bytes() == accept_key(&key).as_bytes());
    if !accepted {
        return Err(ws_error(
            "WebSocket handshake failed: invalid Sec-WebSocket-Accept header",
        ));
    }
    let subprotocol = response_headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if let Some(selected) = &subprotocol {
        if !options
            .subprotocols
            .iter()
            .any(|offered| offered == selected)
        {
            return Err(ws_error(format!(
                "WebSocket handshake failed: the server selected subprotocol {selected:?}, \
                 which was not offered"
            )));
        }
    }
    let extensions = response_headers
        .get(SEC_WEBSOCKET_EXTENSIONS)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if extensions.is_some() && !options.compression {
        return Err(ws_error(
            "WebSocket handshake failed: the server enabled an extension that was not offered",
        ));
    }
    let header_list: Vec<(String, String)> = response_headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();

    let upgraded = hyper::upgrade::on(&mut response)
        .await
        .map_err(connect_error)?;
    let parts = upgraded
        .downcast::<TokioIo<BoxIo>>()
        .map_err(|_| ws_error("WebSocket handshake failed: unexpected connection type"))?;
    let io = Rewind {
        prefix: parts.read_buf,
        inner: parts.io.into_inner(),
    };

    let limit = options.max_message_size.unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
    let mut ws_options = Options::default().with_utf8().with_limits(limit, limit);
    let compressed = extensions.is_some();
    if compressed {
        ws_options.compression = Some(DeflateOptions::default());
    }
    let socket = yawc::WebSocket::from_stream_with_extensions(
        io,
        yawc::Role::Client,
        extensions.as_deref(),
        ws_options,
    )
    .map_err(map_ws_error)?;

    let (command_tx, command_rx) = mpsc::channel(INCOMING_QUEUE);
    let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_QUEUE);
    tokio::spawn(run_connection(socket, command_rx, incoming_tx));
    Ok(WebSocketConnection {
        shared: Arc::new(Shared {
            commands: command_tx,
            incoming: tokio::sync::Mutex::new(incoming_rx),
        }),
        subprotocol,
        response_headers: header_list,
        compressed,
    })
}

/// Open a WebSocket connection.
pub async fn connect(options: ConnectOptions) -> NxResult<WebSocketConnection> {
    let url = Url::parse(&options.url).map_err(|e| {
        NxError::new(
            ErrorKind::InvalidURL,
            format!("invalid WebSocket URL {:?}: {e}", options.url),
        )
    })?;
    match url.scheme() {
        "ws" | "wss" => {}
        other => {
            return Err(NxError::new(
                ErrorKind::UnsupportedProtocol,
                format!(
                    "unsupported WebSocket URL scheme {other:?} in {:?} (expected ws or wss)",
                    options.url
                ),
            ))
        }
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(NxError::new(
            ErrorKind::InvalidURL,
            format!("invalid WebSocket URL {:?}: missing host", options.url),
        ));
    }
    let proxy = proxy_for(&url, &options.proxies, options.trust_env)?;
    let opening = async {
        let io = open_transport(&url, proxy.as_ref(), options.tls.clone()).await?;
        handshake(&options, &url, io).await
    };
    match options.connect_timeout {
        Some(limit) => tokio::time::timeout(limit, opening).await.map_err(|_| {
            NxError::new(
                ErrorKind::ConnectTimeout,
                format!(
                    "timed out after {:.3}s opening WebSocket connection",
                    limit.as_secs_f64()
                ),
            )
        })?,
        None => opening.await,
    }
}

// ---------------------------------------------------------------------------
// Connection actor
// ---------------------------------------------------------------------------

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

enum Command {
    Send(Frame, oneshot::Sender<NxResult<()>>),
    Close(u16, String, oneshot::Sender<NxResult<()>>),
}

struct Shared {
    commands: mpsc::Sender<Command>,
    incoming: tokio::sync::Mutex<mpsc::Receiver<NxResult<Incoming>>>,
}

async fn next_frame(socket: &mut Socket) -> Result<Frame, WebSocketError> {
    std::future::poll_fn(|cx| socket.poll_next_frame(cx)).await
}

/// What a received frame means for the application.
enum Event {
    Deliver(NxResult<Incoming>),
    /// Deliver, then stop reading: the connection is finished.
    Finish(NxResult<Incoming>),
    Skip,
}

async fn interpret(socket: &mut Socket, frame: Result<Frame, WebSocketError>) -> Event {
    let frame = match frame {
        Ok(frame) => frame,
        Err(WebSocketError::ConnectionClosed) => {
            return Event::Finish(Ok(Incoming::Closed(ABNORMAL_CLOSURE, String::new())))
        }
        Err(WebSocketError::IoError(e)) if is_disconnect(&e) => {
            return Event::Finish(Ok(Incoming::Closed(ABNORMAL_CLOSURE, e.to_string())))
        }
        Err(err) => return Event::Finish(Err(map_ws_error(err))),
    };
    match frame.opcode() {
        OpCode::Text => match String::from_utf8(frame.into_payload().into()) {
            Ok(text) => Event::Deliver(Ok(Incoming::Text(text))),
            Err(_) => Event::Finish(Err(ws_error(
                "received a text message that is not valid UTF-8",
            ))),
        },
        OpCode::Binary => Event::Deliver(Ok(Incoming::Binary(frame.into_payload()))),
        OpCode::Close => {
            let code = frame
                .close_code()
                .map(u16::from)
                .unwrap_or(NO_STATUS_RECEIVED);
            let reason = frame
                .close_reason()
                .ok()
                .flatten()
                .unwrap_or_default()
                .to_owned();
            // The close reply was queued by the framing layer; push it out.
            let _ = socket.flush().await;
            Event::Finish(Ok(Incoming::Closed(code, reason)))
        }
        OpCode::Ping => {
            // The pong was queued by the framing layer; push it out.
            let _ = socket.flush().await;
            Event::Skip
        }
        _ => Event::Skip,
    }
}

async fn send_close(socket: &mut Socket, code: u16, reason: &str) -> NxResult<()> {
    match socket
        .send(Frame::close(yawc::close::CloseCode::from(code), reason))
        .await
    {
        // A peer that is already gone needs no close frame.
        Ok(()) | Err(WebSocketError::ConnectionClosed | WebSocketError::IoError(_)) => Ok(()),
        Err(err) => Err(map_ws_error(err)),
    }
}

async fn run_connection(
    mut socket: Socket,
    mut commands: mpsc::Receiver<Command>,
    incoming: mpsc::Sender<NxResult<Incoming>>,
) {
    // `None` once the peer's close (or a fatal error) has been delivered.
    let mut incoming = Some(incoming);
    loop {
        // A clone, so the read branch does not borrow `incoming` itself.
        let sender = incoming.clone();
        let reading = sender.is_some();
        tokio::select! {
            command = commands.recv() => match command {
                // Every handle is gone: dropping the socket closes the connection.
                None => return,
                Some(Command::Send(frame, reply)) => {
                    let _ = reply.send(socket.send(frame).await.map_err(map_ws_error));
                }
                Some(Command::Close(code, reason, reply)) => {
                    let result = send_close(&mut socket, code, &reason).await;
                    if result.is_ok() && reading {
                        // Wait briefly for the peer's close frame so the TCP
                        // teardown is clean.
                        let _ = tokio::time::timeout(CLOSE_HANDSHAKE_TIMEOUT, async {
                            while let Ok(frame) = next_frame(&mut socket).await {
                                if frame.opcode() == OpCode::Close {
                                    break;
                                }
                            }
                        })
                        .await;
                    }
                    let _ = reply.send(result);
                    return;
                }
            },
            // Read only while the application has room for another message,
            // so a slow consumer applies backpressure to the peer.
            event = async {
                let permit = sender.as_ref()?.reserve().await.ok()?;
                let frame = next_frame(&mut socket).await;
                Some((permit, interpret(&mut socket, frame).await))
            }, if reading => match event {
                // The receiving half of the handle was dropped.
                None => incoming = None,
                Some((permit, Event::Deliver(message))) => permit.send(message),
                Some((permit, Event::Finish(message))) => {
                    permit.send(message);
                    incoming = None;
                }
                Some((_, Event::Skip)) => {}
            },
        }
    }
}

async fn send_frame(shared: &Shared, frame: Frame) -> NxResult<()> {
    let (reply, done) = oneshot::channel();
    shared
        .commands
        .send(Command::Send(frame, reply))
        .await
        .map_err(|_| closed_error())?;
    done.await.unwrap_or_else(|_| Err(closed_error()))
}

async fn receive(shared: &Shared, timeout: Option<Duration>) -> NxResult<Incoming> {
    let mut incoming = shared.incoming.lock().await;
    let next = async {
        match incoming.recv().await {
            Some(message) => message,
            None => Ok(Incoming::Closed(ABNORMAL_CLOSURE, String::new())),
        }
    };
    match timeout {
        Some(limit) => tokio::time::timeout(limit, next).await.map_err(|_| {
            NxError::new(
                ErrorKind::ReadTimeout,
                format!(
                    "timed out after {:.3}s waiting for a WebSocket message",
                    limit.as_secs_f64()
                ),
            )
        })?,
        None => next.await,
    }
}

async fn close(shared: &Shared, code: u16, reason: String) -> NxResult<()> {
    let (reply, done) = oneshot::channel();
    if shared
        .commands
        .send(Command::Close(code, reason, reply))
        .await
        .is_err()
    {
        return Ok(()); // already closed
    }
    done.await.unwrap_or(Ok(()))
}

fn ping_frame(data: Vec<u8>) -> NxResult<Frame> {
    if data.len() > 125 {
        return Err(NxError::new(
            ErrorKind::LocalProtocolError,
            "a WebSocket ping payload must be at most 125 bytes",
        ));
    }
    Ok(Frame::ping(data))
}

fn timeout_duration(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    match seconds {
        None => Ok(None),
        Some(value) if value.is_finite() && value >= 0.0 => {
            Ok(Some(Duration::from_secs_f64(value)))
        }
        Some(_) => Err(pyo3::exceptions::PyValueError::new_err(
            "timeout must be >= 0 or None",
        )),
    }
}

// ---------------------------------------------------------------------------
// Python handle
// ---------------------------------------------------------------------------

/// An open WebSocket connection.
///
/// Every operation has an awaitable form and a `_blocking` form that parks
/// the calling thread with the GIL released.
#[pyclass(frozen, module = "nextreq._nextreq", name = "WebSocketConnection")]
pub struct WebSocketConnection {
    shared: Arc<Shared>,
    subprotocol: Option<String>,
    response_headers: Vec<(String, String)>,
    compressed: bool,
}

impl WebSocketConnection {
    /// Run `work` to completion on the calling thread (GIL released).
    fn blocking<F, T>(py: Python<'_>, work: F) -> PyResult<Bound<'_, PyAny>>
    where
        F: std::future::Future<Output = NxResult<T>> + Send,
        T: for<'a> IntoPyObject<'a> + Send,
    {
        let result = block_on_interruptible(py, work)?;
        Outcome(result).into_pyobject(py)
    }
}

#[pymethods]
impl WebSocketConnection {
    /// Send a text message.
    fn send_text<'py>(&self, py: Python<'py>, text: String) -> PyResult<Bound<'py, PyAny>> {
        let shared = self.shared.clone();
        spawn_awaitable(
            py,
            async move { send_frame(&shared, Frame::text(text)).await },
        )
    }

    fn send_text_blocking<'py>(
        &self,
        py: Python<'py>,
        text: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::blocking(py, send_frame(&self.shared, Frame::text(text)))
    }

    /// Send a binary message.
    fn send_bytes<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let shared = self.shared.clone();
        spawn_awaitable(
            py,
            async move { send_frame(&shared, Frame::binary(data)).await },
        )
    }

    fn send_bytes_blocking<'py>(
        &self,
        py: Python<'py>,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::blocking(py, send_frame(&self.shared, Frame::binary(data)))
    }

    /// Send a ping (payload must be at most 125 bytes).
    #[pyo3(signature = (data = Vec::new()))]
    fn ping<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let shared = self.shared.clone();
        spawn_awaitable(
            py,
            async move { send_frame(&shared, ping_frame(data)?).await },
        )
    }

    #[pyo3(signature = (data = Vec::new()))]
    fn ping_blocking<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let shared = &self.shared;
        Self::blocking(
            py,
            async move { send_frame(shared, ping_frame(data)?).await },
        )
    }

    /// Receive the next message as `(kind, payload, code)`.
    ///
    /// kind 0 = text, 1 = binary, 2 = connection closed.
    fn receive<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let shared = self.shared.clone();
        spawn_awaitable(py, async move { receive(&shared, None).await })
    }

    /// Blocking `receive()`; raises `ReadTimeout` after `timeout` seconds.
    #[pyo3(signature = (timeout = None))]
    fn receive_blocking<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = timeout_duration(timeout)?;
        Self::blocking(py, receive(&self.shared, timeout))
    }

    /// Perform the closing handshake.
    #[pyo3(signature = (code = 1000, reason = String::new()))]
    fn close<'py>(
        &self,
        py: Python<'py>,
        code: u16,
        reason: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let shared = self.shared.clone();
        spawn_awaitable(py, async move { close(&shared, code, reason).await })
    }

    #[pyo3(signature = (code = 1000, reason = String::new()))]
    fn close_blocking<'py>(
        &self,
        py: Python<'py>,
        code: u16,
        reason: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::blocking(py, close(&self.shared, code, reason))
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

    /// Whether permessage-deflate compression was negotiated.
    #[getter]
    fn compressed(&self) -> bool {
        self.compressed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(url: &str) -> ConnectOptions {
        ConnectOptions {
            url: url.to_owned(),
            headers: Vec::new(),
            subprotocols: Vec::new(),
            tls: Arc::new(
                crate::config::build_tls_config(
                    &crate::config::TlsOptions::default(),
                    &[b"http/1.1"],
                )
                .unwrap(),
            ),
            proxies: Vec::new(),
            trust_env: false,
            connect_timeout: Some(Duration::from_secs(1)),
            max_message_size: None,
            compression: false,
        }
    }

    #[test]
    fn closed_and_io_errors_map_to_the_right_kinds() {
        assert_eq!(
            map_ws_error(WebSocketError::ConnectionClosed).kind,
            ErrorKind::WebSocketError
        );
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(
            map_ws_error(WebSocketError::IoError(io)).kind,
            ErrorKind::ConnectError
        );
    }

    #[test]
    fn rejects_non_websocket_schemes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(connect(options("http://127.0.0.1:1/")));
        assert_eq!(result.err().unwrap().kind, ErrorKind::UnsupportedProtocol);
    }

    #[test]
    fn accept_key_matches_the_rfc_example() {
        // RFC 6455, section 1.3.
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn no_proxy_rules() {
        assert!(no_proxy_matches("*", "example.com"));
        assert!(no_proxy_matches(
            "localhost, example.com",
            "api.example.com"
        ));
        assert!(no_proxy_matches(".example.com", "example.com"));
        assert!(!no_proxy_matches("example.com", "badexample.com"));
        assert!(no_proxy_matches("127.0.0.1", "127.0.0.1"));
        assert!(no_proxy_matches("10.0.0.0/8", "10.20.30.40"));
        assert!(!no_proxy_matches("10.0.0.0/8", "11.0.0.1"));
        assert!(no_proxy_matches("::1", "[::1]"));
        assert!(no_proxy_matches("fd00::/8", "fd12::1"));
        assert!(!no_proxy_matches("", "example.com"));
    }

    #[test]
    fn explicit_proxies_follow_the_url_scheme() {
        let proxies = vec![
            ("http".to_owned(), "http://plain.proxy:3128".to_owned()),
            ("https".to_owned(), "http://secure.proxy:3128".to_owned()),
        ];
        let plain = proxy_for(&Url::parse("ws://example.com/").unwrap(), &proxies, false).unwrap();
        assert_eq!(plain.unwrap().host_str(), Some("plain.proxy"));
        let secure =
            proxy_for(&Url::parse("wss://example.com/").unwrap(), &proxies, false).unwrap();
        assert_eq!(secure.unwrap().host_str(), Some("secure.proxy"));
        let none = proxy_for(&Url::parse("ws://example.com/").unwrap(), &[], false).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn percent_decoding_of_proxy_credentials() {
        assert_eq!(percent_decode("p%40ss%3Aword"), "p@ss:word");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("100%"), "100%");
    }

    #[test]
    fn authority_formats_ports_and_ipv6() {
        let url = Url::parse("wss://example.com/chat").unwrap();
        assert_eq!(authority(&url, false), "example.com");
        assert_eq!(authority(&url, true), "example.com:443");
        let url = Url::parse("ws://[::1]:9000/").unwrap();
        assert_eq!(authority(&url, true), "[::1]:9000");
    }
}
