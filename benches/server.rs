//! High-throughput local benchmark target server.
//!
//! A deliberately minimal hyper HTTP/1.1 server so that the *client* is always
//! the bottleneck being measured, with zero internet jitter:
//!
//! * `GET /json`            - small JSON document
//! * `GET /user/{id}`       - a richer JSON object for typed deserialization
//! * `GET /large?size=N`    - N bytes of payload (default 1 MiB)
//! * `GET /sse?count=N`     - N Server-Sent Events, streamed
//! * `GET /health`          - liveness probe
//!
//! Run with `cargo run --release --example bench_server -- --port 8765`.
//! With `--port 0` (the default) an ephemeral port is chosen and announced on
//! stdout as `LISTENING <port>`, which is what `run_benchmarks.py` reads.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{HeaderValue, CACHE_CONTROL, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

const SMALL_JSON: &[u8] = br#"{"id":1,"name":"Ada Lovelace","email":"ada@example.com","active":true,"score":98.6,"tags":["math","engine","notes"]}"#;
const DEFAULT_LARGE_SIZE: usize = 1024 * 1024;
const MAX_LARGE_SIZE: usize = 64 * 1024 * 1024;
/// Events are emitted in frames of this many so one write carries a batch.
const SSE_EVENTS_PER_FRAME: usize = 16;

type ResponseBody = BoxBody<Bytes, Infallible>;

/// The JSON document served by `/user/{id}`: ~1.2 kB with nested objects and
/// a list, so that validation cost is realistic.
fn user_json(id: u64) -> Bytes {
    let mut orders = String::new();
    for n in 0..10u64 {
        if n > 0 {
            orders.push(',');
        }
        orders.push_str(&format!(
            r#"{{"order_id":{},"sku":"SKU-{:05}","quantity":{},"unit_price":{:.2},"shipped":{}}}"#,
            id * 1000 + n,
            (id * 7 + n * 13) % 100_000,
            1 + (n % 5),
            9.99 + n as f64 * 3.5,
            n % 2 == 0
        ));
    }
    Bytes::from(format!(
        concat!(
            r#"{{"id":{id},"username":"user{id}","email":"user{id}@example.com","#,
            r#""full_name":"Benchmark User {id}","active":{active},"score":{score:.2},"#,
            r#""created_at":"2026-01-02T03:04:05+00:00","#,
            r#""address":{{"street":"{id} Main Street","city":"Springfield","country":"US","zip":"{zip:05}"}},"#,
            r#""tags":["alpha","beta","gamma","delta"],"#,
            r#""preferences":{{"theme":"dark","language":"en","notifications":true}},"#,
            r#""orders":[{orders}]}}"#
        ),
        id = id,
        active = id % 2 == 0,
        score = 50.0 + (id % 50) as f64,
        zip = id % 100_000,
        orders = orders,
    ))
}

/// Streams `count` Server-Sent Events without buffering them all up front.
struct SseBody {
    next: u64,
    count: u64,
}

impl Body for SseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if self.next >= self.count {
            return Poll::Ready(None);
        }
        let end = (self.next + SSE_EVENTS_PER_FRAME as u64).min(self.count);
        let mut chunk = String::with_capacity(SSE_EVENTS_PER_FRAME * 96);
        for n in self.next..end {
            chunk.push_str(&format!(
                "id: {n}\nevent: tick\ndata: {{\"seq\":{n},\"symbol\":\"NXRQ\",\"price\":{:.2},\"volume\":{}}}\n\n",
                100.0 + (n % 500) as f64 * 0.25,
                1000 + (n % 97) * 13
            ));
        }
        self.next = end;
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk)))))
    }

    fn is_end_stream(&self) -> bool {
        self.next >= self.count
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

fn full(bytes: impl Into<Bytes>) -> ResponseBody {
    Full::new(bytes.into()).boxed()
}

fn query_param(query: Option<&str>, name: &str) -> Option<u64> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.parse().ok()).flatten()
    })
}

fn json_response(body: impl Into<Bytes>) -> Response<ResponseBody> {
    let mut response = Response::new(full(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn status_response(status: StatusCode, message: &'static str) -> Response<ResponseBody> {
    let mut response = Response::new(full(message));
    *response.status_mut() = status;
    response
}

async fn handle(request: Request<Incoming>) -> Result<Response<ResponseBody>, Infallible> {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return Ok(status_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let path = request.uri().path();
    let query = request.uri().query();
    let response = if path == "/json" {
        json_response(Bytes::from_static(SMALL_JSON))
    } else if let Some(id) = path.strip_prefix("/user/") {
        match id.parse::<u64>() {
            Ok(id) => json_response(user_json(id)),
            Err(_) => status_response(StatusCode::BAD_REQUEST, "invalid user id"),
        }
    } else if path == "/large" {
        let size = query_param(query, "size")
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_LARGE_SIZE)
            .min(MAX_LARGE_SIZE);
        let mut response = Response::new(full(vec![b'x'; size]));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        response
    } else if path == "/sse" {
        let count = query_param(query, "count").unwrap_or(1000);
        let mut response = Response::new(SseBody { next: 0, count }.boxed());
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        response
    } else if path == "/health" {
        Response::new(full("ok"))
    } else {
        status_response(StatusCode::NOT_FOUND, "not found")
    };
    Ok(response)
}

fn parse_port() -> u16 {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 0u16;
    let mut index = 1;
    while index < args.len() {
        if args[index] == "--port" {
            if let Some(value) = args.get(index + 1) {
                port = value.parse().expect("--port expects a number");
            }
            index += 1;
        }
        index += 1;
    }
    port
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port = parse_port();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
        // The runner parses this line to learn the ephemeral port.
        println!("LISTENING {}", listener.local_addr()?.port());
        loop {
            let (stream, _) = listener.accept().await?;
            stream.set_nodelay(true)?;
            tokio::spawn(async move {
                let connection = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(TokioIo::new(stream), service_fn(handle));
                // Clients closing a kept-alive connection is not an error worth logging.
                let _ = connection.await;
            });
        }
    })
}
