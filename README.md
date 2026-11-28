# nextreq

**A next-generation, Rust-powered, batteries-included HTTP client for Python.**

[![PyPI](https://img.shields.io/pypi/v/nextreq)](https://pypi.org/project/nextreq/)
[![Python versions](https://img.shields.io/pypi/pyversions/nextreq)](https://pypi.org/project/nextreq/)
[![CI](https://github.com/chetanxrathore/nextreq/actions/workflows/ci.yml/badge.svg)](https://github.com/chetanxrathore/nextreq/actions/workflows/ci.yml)
[![Downloads](https://img.shields.io/pypi/dm/nextreq)](https://pypistats.org/packages/nextreq)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](https://github.com/chetanxrathore/nextreq#license)
[![Rust core](https://img.shields.io/badge/core-Rust-orange?logo=rust)](https://www.rust-lang.org/)
[![Typing](https://img.shields.io/badge/typing-mypy%20strict-blue)](https://mypy-lang.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

`nextreq` pairs a Rust core (Tokio · hyper/reqwest · rustls) with a small, fully typed
Python API. You get the ergonomics of `requests`, a first-class `asyncio` client, and
the things you normally bolt on yourself - retries, typed responses, SSE, WebSockets,
concurrency helpers, mocking and tracing - in one dependency-free package.

```python
import nextreq

r = nextreq.get("https://httpbin.org/get", params={"q": "rust"})
r.raise_for_status()
print(r.json(), r.http_version, r.elapsed.total)
```

- **Fast.** The I/O engine runs in Rust with the GIL released. On the local
  benchmark below it is about 8× `requests` for sequential calls and 2.5× `aiohttp`
  for async fan-out (3.3× with the built-in `gather()`). [Benchmarks](#benchmarks)
- **Sync and async, one API.** `Client` and `AsyncClient` share every option and
  every feature, on one connection pool implementation.
- **Typed end to end.** `client.get(url, model=User)` returns a `User` - statically
  (mypy/pyright) and at runtime (Pydantic v2, dataclasses or msgspec).
- **Safe by default.** Timeouts on every phase, TLS verification, bounded
  redirects, credentials dropped on cross-origin redirects.
- **Resilient.** Built-in retries with exponential backoff, jitter and `Retry-After`.
- **Made for real APIs.** Client-side rate limits, `paginate()`, JSON Lines, and
  downloads that resume where a broken connection stopped.
- **Realtime.** WHATWG-compliant SSE parser in Rust, WebSockets (sync and async, with
  compression and proxy support), and `gather()` for bounded-concurrency fan-out.
- **Streaming both ways.** Downloads and uploads are streamed: generators and open
  files are sent chunk by chunk, never loaded into memory.
- **Testable.** `MockTransport`, plus in-memory transports for ASGI (FastAPI) and
  WSGI (Flask) apps - no sockets, no monkeypatching.
- **Observable.** Request/response/error hooks, per-phase timings, optional
  OpenTelemetry spans with W3C `traceparent` propagation.
- **Modern protocols.** HTTP/1.1 and HTTP/2 out of the box, gzip / deflate / brotli /
  zstd decoding, proxies, optional HTTP/3.
- **Zero mandatory dependencies.** Python 3.10+, one abi3 wheel per platform.

> **Status:** young but thoroughly tested: 489 Python tests and 45 Rust tests, run
> on CPython 3.10 (against the abi3 wheel) and 3.14, plus a threaded smoke test on
> free-threaded CPython 3.14t. What it does not do is listed under
> [Limitations](#limitations).

## Installation

```bash
pip install nextreq
pip install "nextreq[pydantic]"    # optional extras: pydantic, msgspec, otel
```

From source (needs a Rust toolchain):

```bash
git clone https://github.com/chetanxrathore/nextreq && cd nextreq
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
maturin develop --release
```

## Tour

### Clients

```python
import nextreq

with nextreq.Client(
    base_url="https://api.example.com",
    headers={"Accept": "application/json"},
    auth=("user", "pass"),            # or BearerAuth / DigestAuth / any callable
    timeout=10,                       # or nextreq.Timeout(connect=2, read=30)
    retries=3,
) as client:
    client.get("/users", params={"page": 2})
    client.post("/users", json={"name": "Ada"})
    client.put("/files/1", content=b"raw bytes")
    client.post("/upload", data={"title": "Report"}, files={"file": open("report.pdf", "rb")})
```

```python
import asyncio, nextreq

async def main():
    async with nextreq.AsyncClient(base_url="https://api.example.com") as client:
        response = await client.get("/users/1")
        print(response.status_code, response.json())

asyncio.run(main())
```

Both clients accept `base_url`, `headers`, `params`, `cookies` (a persistent jar),
`auth`, `timeout`, `follow_redirects` (default `True`, at most 10 hops),
`raise_for_status`, `retries`, `rate_limit`, `verify`, `cert`, `proxy`, `trust_env`,
`http2`, `uds`, `max_connections` and `keepalive_expiry`. Every request method accepts
per-request overrides. The module-level helpers (`nextreq.get`, `post`, `put`, `patch`,
`delete`, `head`, `options`, `request`, `stream`, `download`) share a keep-alive pool
but never share cookies.

To talk to a service on a Unix domain socket, such as the Docker daemon, pass its path.
The host in the URL is then only used for the `Host` header (not on Windows):

```python
with nextreq.Client(uds="/var/run/docker.sock", base_url="http://docker") as client:
    print(client.get("/version").json())
```

### Uploads

Bodies that already sit in memory (`bytes`, `str`, `json=`, form `data=`) are sent
as they are. Bodies that are *produced* - generators, async generators and open
files - are streamed: nextreq pulls the next chunk only when the connection is
ready for it, so memory use stays flat however large the upload is.

```python
with open("backup.tar", "rb") as f:
    client.put("/backups/today", content=f)             # sent in 64 KiB pieces, with Content-Length

with open("report.pdf", "rb") as f:                     # multipart: fields in memory, file streamed
    client.post("/upload", data={"title": "Q3"}, files={"file": f})

def rows():
    for row in database.iter_rows():
        yield row.to_json().encode() + b"\n"

client.post("/import", content=rows())                  # unknown length: chunked encoding

async def events():
    async for event in queue:
        yield event

await aclient.post("/ingest", content=events())         # async generators work with AsyncClient
```

`AsyncClient` reads files in a worker thread, so a slow disk does not stall the event
loop.

A retry or a `307`/`308` redirect has to send the body again. Seekable files and
re-iterable objects are rewound and replayed; a one-shot generator cannot be, so such
a request is not retried, and a redirect that needs the body raises `StreamConsumed`.

### Authentication

```python
client = nextreq.Client(auth=("user", "pass"))                  # Basic
client = nextreq.Client(auth=nextreq.BearerAuth("token"))
client = nextreq.Client(auth=nextreq.DigestAuth("user", "pass"))
client.get("/private", auth=sign_request)                       # any callable taking the request
```

`DigestAuth` implements RFC 7616 (MD5, SHA-256, SHA-512 and their `-sess` variants,
`qop=auth` and `qop=auth-int`). It answers the server's challenge transparently and
remembers it, so only the first request costs a round trip. `auth-int` also signs the
request body and is used only when the server offers nothing else; the body then has
to be in memory, a seekable file or a re-iterable object.

Schemes that depend on the server's answer subclass `Auth` and write an `auth_flow`
generator: it yields a request and receives the response to it, as often as it needs
to. The same flow drives both clients.

```python
class RefreshingAuth(nextreq.Auth):
    def auth_flow(self, request):
        request.headers["authorization"] = f"Bearer {self.token}"
        response = yield request
        if response.status_code == 401:
            refreshed = yield nextreq.Request("POST", "https://auth.example.com/token", json=...)
            self.token = refreshed.json()["access_token"]
            request.headers["authorization"] = f"Bearer {self.token}"
            yield request
```

Flow-based credentials follow redirects within the same origin and are dropped when
a redirect leaves it, like the `Authorization` header.

### Typed responses

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str

user = client.get("/users/1", model=User)            # -> User
users = client.get("/users", model=list[User])       # -> list[User]
user = (await aclient.get("/users/1", model=User))   # same for AsyncClient
```

`model=` works with Pydantic v2 models, dataclasses, `msgspec.Struct` and generics
of them. Type checkers infer the return type from the argument, and the JSON bytes
go straight to the validator. A body that does not fit raises `DecodeError` carrying
the status code, URL, model and structured `errors`; a 4xx/5xx response raises
`HTTPStatusError` first, so error payloads are never force-fed into your model.

### Timeouts

Safe defaults apply to every request: **connect 5 s, read 30 s, write 30 s, pool 10 s**.

```python
nextreq.Client(timeout=10)                                   # 10 s for every phase
nextreq.Client(timeout=nextreq.Timeout(connect=2, read=60))  # granular
client.get("/slow", timeout=None)                            # per-request, no timeout
```

Each phase raises its own exception - `ConnectTimeout`, `ReadTimeout`, `WriteTimeout`,
`PoolTimeout` - all of which are also the builtin `TimeoutError`. The read timeout
applies to each read, not to the whole response, so long downloads are not cut off.

### Retries

```python
client = nextreq.Client(retries=3)        # up to 3 retries after the first attempt

policy = nextreq.RetryPolicy(
    max_attempts=5,
    backoff_factor=0.5,                   # 0.5 s, 1 s, 2 s, 4 s ... capped at max_backoff
    jitter=True,                          # full jitter; pass a float for partial
    retry_statuses={429, 502, 503, 504},
    retry_methods={"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"},
)
response = client.get("/flaky", retries=policy)
print(response.retry_count)
```

By default only idempotent methods are retried, on transport errors and on
429 / 502 / 503 / 504. `Retry-After` is honoured in both delta-seconds and HTTP-date
form (capped by `max_retry_after`). Retries are off unless you ask for them.

### Streaming and Server-Sent Events

```python
with client.stream("GET", "/download") as response:
    for chunk in response.iter_bytes():
        ...

async with aclient.stream("GET", "/v1/events") as response:
    async for event in response.aiter_sse():
        print(event.event, event.id, event.json())
```

The SSE parser is implemented in Rust, follows the WHATWG event-stream rules (CRLF/LF/CR
line endings, comments, multi-line `data`, persistent `id`, `retry`, BOM) and yields
immutable `SSEEvent(event, data, id, retry)` objects.

For APIs that stream JSON Lines (NDJSON), one JSON value per line:

```python
with client.stream("POST", "/api/generate", json=payload) as response:
    for item in response.iter_json_lines():           # or model=Chunk for typed items
        print(item["response"], end="")
```

`aiter_json_lines()` is the async twin. Blank lines are skipped.

### WebSockets

```python
async with aclient.websocket("wss://example.com/ws", subprotocols=["json"]) as ws:
    await ws.send_json({"subscribe": "ticker"})
    async for message in ws:                 # str or bytes; stops when the peer closes
        ...

with client.websocket("wss://example.com/ws", compression=True) as ws:   # blocking API
    ws.send_text("hello")
    print(ws.receive(timeout=5), ws.compressed)
```

`send_text` / `send_bytes` / `send_json`, `receive` / `receive_text` / `receive_bytes` /
`receive_json` (optionally with `timeout=`), `ping` and `close(code, reason)` are
available on both; a closed connection raises `WebSocketDisconnect(code, reason)`.
With the blocking API one thread can wait in `receive()` while another sends.

Connections use the client's TLS settings, default headers, cookies and proxies
(`http://` and `https://` proxies via `CONNECT`, `socks5://`, and the proxy environment
variables including `NO_PROXY`). `compression=True` offers permessage-deflate
(RFC 7692); `ws.compressed` tells you whether the server accepted it. Pings from the
server are answered automatically.

`auth=` applies to the handshake, including schemes that answer a challenge:

```python
with client.websocket("wss://example.com/ws", auth=nextreq.DigestAuth("user", "pass")) as ws:
    ...
```

When the server refuses the handshake, `WebSocketError.response` holds its answer:

```python
try:
    ws = client.websocket("wss://example.com/ws")
except nextreq.WebSocketError as exc:
    if exc.response is not None:                 # None for network and protocol errors
        print(exc.response.status_code, exc.response.text)
```

### Concurrency: `gather()`

```python
async with nextreq.AsyncClient(base_url="https://api.example.com") as client:
    responses = await client.gather(
        [f"/items/{n}" for n in range(1000)],
        max_concurrency=20,                  # default
        return_exceptions=False,             # default: first failure cancels the rest
    )
```

Results come back in input order. Items may be URLs, `Request` objects or
`(method, url[, options])` tuples, and the full client pipeline (redirects, cookies,
retries, hooks) applies to each.

### Rate limits

Stay under an API's limit instead of collecting `429` responses:

```python
client = nextreq.AsyncClient(rate_limit=10)                       # 10 requests per second
client = nextreq.Client(rate_limit=nextreq.RateLimit(600, per=60, burst=5))
```

Requests beyond the limit wait for their turn, in order. They are spaced evenly (one
every `per / rate` seconds), so the limit holds over any window, not only on average;
`burst` lets that many leave back to back after an idle period. Every request that
reaches the server counts, including retries and redirects. One `RateLimit` object can
be given to several clients, sync or async, in any number of threads, and they then
share one budget. The wait is not part of the request's timeout, and WebSocket
handshakes are not counted.

### Pagination

```python
for page in client.paginate("/repos/python/cpython/issues", params={"per_page": 100}):
    for issue in page.json():
        ...
```

By default `paginate()` follows the `rel="next"` link of the `Link` header, as used by
GitHub, GitLab and many others, and yields one response per page. Pages are fetched
only as you iterate. For other schemes, `next_page` takes a page and returns where the
next one is:

```python
def next_page(page):
    cursor = page.json().get("next_cursor")
    return {"cursor": cursor} if cursor else None     # query parameters to change

async for page in aclient.paginate("/v1/events", next_page=next_page, max_pages=50):
    ...
```

`next_page` may return a URL, a mapping of query parameters, a `Request` (for example
when the cursor goes in a JSON body) or `None` at the end. Headers, auth and the other
options are sent with every page. `response.links` gives the parsed `Link` header on
any response: `response.links["next"]["url"]`.

### Downloads

```python
path = client.download("https://example.com/big.iso", "big.iso", resume=True, retries=3)
```

The body is streamed to `big.iso.part` and renamed when it is complete, so `big.iso`
never holds half a file, and an error status writes nothing.

- `retries` also covers a connection that breaks in the middle of the body: the rest is
  requested with `Range` and appended.
- `resume=True` continues a `.part` file left by an earlier run of your program.
- Both send `If-Range` with the `ETag` (or `Last-Modified`) of the first answer. If the
  file changed on the server, or the server cannot do ranges, the download starts
  again, so two versions are never mixed. Without such a header there is nothing to
  check against, and the download also starts again.
- `on_progress(bytes_on_disk, total_or_None)` reports progress.

`await aclient.download(...)` writes the file in a worker thread, and
`nextreq.download(url, path)` needs no client.

### Testing without a network

```python
import re, nextreq

mock = nextreq.MockTransport()
mock.get("https://api.example.com/users/1").respond(json={"id": 1, "name": "Ada"})
mock.post(re.compile(r"/users$"), json={"name": "Bo"}).respond(201)
mock.get("/flaky").sequence(nextreq.Response(503), nextreq.Response(200, text="ok"))
mock.get("/events").respond(sse=["one", {"n": 2}])

with nextreq.Client(transport=mock, retries=1) as client:
    assert client.get("https://api.example.com/users/1", model=User).name == "Ada"

mock.assert_all_called()        # also: assert_called_once(), .calls, .last_request
```

Call your own application in-process, through the real client pipeline:

```python
async with nextreq.AsyncClient(app=fastapi_app) as client:    # ASGI 3.0
    assert (await client.get("/health")).status_code == 200

    async with client.stream("GET", "/events") as response:  # streamed as the app sends it
        async for event in response.aiter_sse():
            ...

with nextreq.Client(app=flask_app) as client:                 # WSGI
    assert client.get("/health").json() == {"ok": True}
```

Bodies are streamed in both directions, with backpressure. Streaming and SSE endpoints
can be tested chunk by chunk, closing a response early is seen by the application as a
client disconnect, and a streamed upload reaches the application piece by piece (one
`http.request` message per chunk for ASGI, `wsgi.input` for WSGI). ASGI lifespan events
run too: startup before the first request, shutdown when the client is closed
(`ASGITransport(app, lifespan=False)` turns that off), and lifespan state is available
to the handlers.

A WSGI application can also be driven from `AsyncClient`
(`AsyncClient(transport=nextreq.WSGITransport(flask_app))`): it runs in a worker thread
of its own for as long as the response is open. `MockTransport` reads a streamed upload
into `request.content`, so matchers and handlers can look at it.

### Hooks, timings and OpenTelemetry

```python
def log(response: nextreq.Response) -> None:
    t = response.elapsed       # a timedelta with per-phase attributes, in seconds
    print(response.status_code, response.url, t.total, t.connect, t.ttfb, t.download)

client = nextreq.Client(on_request=sign_request, on_response=log, on_error=report)
```

Hooks may be plain functions, or coroutine functions on `AsyncClient`. For tracing:

```python
from nextreq.otel import instrument

client = instrument(nextreq.Client())      # client spans + W3C traceparent injection
```

`opentelemetry-api` is only needed if you call `instrument`; `nextreq.otel.traceparent_hook`
propagates trace context with no dependency at all.

### Errors

```
NextReqError
├── TransportError
│   ├── TimeoutException  → ConnectTimeout, ReadTimeout, WriteTimeout, PoolTimeout
│   ├── NetworkError      → ConnectError, ReadError, WriteError
│   ├── ProxyError, ProtocolError, UnsupportedProtocol, DecodingError, TooManyRedirects
│   └── WebSocketError    → WebSocketDisconnect
├── HTTPStatusError       (response.raise_for_status() / raise_for_status=True)
├── DecodeError           (typed parsing failed; has .status_code, .url, .errors)
└── InvalidURL, StreamError, ClientClosed
```

## Benchmarks

Measured on one machine against a local Rust server, so the numbers isolate client
overhead (no internet jitter). Everything here is produced by
`python benches/run_benchmarks.py`; the full report, methodology, caveats and raw
data are in [`benches/RESULTS.md`](benches/RESULTS.md).

<!-- BENCHMARKS:START -->
_Apple M2 Max · CPython 3.14.6 · nextreq 0.2.0 vs requests 2.34.2, httpx 0.28.1, aiohttp 3.14.5 · 2026-11-20 23:30 UTC. "nextreq speedup" is nextreq's throughput divided by that library's._

**1. Sequential sync GETs** - 500 GET requests for a small JSON document, one after another, over a single keep-alive connection.

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq** | 22,174 | baseline | 0.044 | 0.055 | 0.064 | 43.5 |
| aiohttp (awaited sequentially) | 12,695 | 1.75× | 0.078 | 0.089 | 0.094 | 52.4 |
| httpx | 5,116 | 4.33× | 0.189 | 0.219 | 0.400 | 66.8 |
| requests | 2,812 | 7.89× | 0.355 | 0.375 | 0.396 | 48.9 |

**2. High-concurrency async fan-out** - 1000 GET requests with at most 50 in flight.

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq (client.gather)** | 79,980 | — | 0.221 | 0.417 | 0.481 | 49.7 |
| **nextreq (asyncio.gather + Semaphore)** | 61,049 | baseline | 0.523 | 1.00 | 1.25 | 50.6 |
| aiohttp | 24,549 | 2.49× | 1.21 | 1.89 | 2.23 | 54.3 |
| requests (thread pool) | 3,279 | 18.62× | 13.7 | 26.3 | 33.4 | 53.6 |
| httpx | 605 | 100.84× | 58.1 | 220 | 334 | 72.2 |

**3. Typed JSON deserialization** - 500 GET requests for a ~1.3 kB JSON document, each validated into a Pydantic v2 model (nested models, a list of 10 sub-objects, a datetime).

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq** | 16,509 | baseline | 0.059 | 0.071 | 0.078 | 54.7 |
| aiohttp (awaited sequentially) | 9,576 | 1.72× | 0.104 | 0.114 | 0.123 | 62.8 |
| httpx | 4,043 | 4.08× | 0.244 | 0.263 | 0.447 | 76.1 |
| requests | 2,614 | 6.32× | 0.380 | 0.403 | 0.428 | 60.5 |

**4. SSE streaming throughput** - A stream of 10000 Server-Sent Events, consumed into event objects (event, data, id, retry).

| Library | Events/sec | nextreq speedup | Time per stream (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|
| **nextreq** | 2,963,915 | baseline | 3.37 | 44.0 |
| requests + hand-written parser | 898,610 | 3.30× | 11.1 | 48.9 |
| httpx + hand-written parser | 825,545 | 3.59× | 12.1 | 67.0 |
| aiohttp + hand-written parser | 196,637 | 15.07× | 50.9 | 52.7 |

**5. Large payload downloads (bonus)** - 100 sequential GET requests, each downloading a 1 MiB body.

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq** | 5,478 | baseline | 0.178 | 0.219 | 0.235 | 50.5 |
| aiohttp (awaited sequentially) | 4,723 | 1.16× | 0.209 | 0.231 | 0.246 | 54.4 |
| httpx | 1,742 | 3.14× | 0.571 | 0.623 | 0.818 | 136.9 |
| requests | 1,273 | 4.30× | 0.787 | 0.825 | 0.863 | 52.0 |

**Takeaways**

- **Sequential sync GETs:** nextreq handles 22,174 req/s at a p50 of 0.044 ms - **7.9×** requests, **4.3×** httpx, **1.7×** aiohttp.
- **Async fan-out:** with the same `asyncio.gather` + `Semaphore` harness nextreq reaches 61,049 req/s - **2.5×** aiohttp, **100.8×** httpx, **18.6×** requests. The built-in `client.gather()` reaches 79,980 req/s (1.31× the asyncio.gather harness, 3.3× aiohttp).
- **Typed JSON:** `client.get(url, model=User)` (bytes validated straight into the model by pydantic-core) is **6.3×** requests, **4.1×** httpx, **1.7×** aiohttp doing `User.model_validate(response.json())`.
- **SSE:** the Rust parser delivers 2,963,915 events/s as ready-made `SSEEvent` objects - **3.3×** requests, **3.6×** httpx, **15.1×** aiohttp with a hand-written Python parser.
- **Large downloads:** for 1 MiB bodies, where memory copies rather than per-request overhead dominate, nextreq is **4.3×** requests, **3.1×** httpx, **1.2×** aiohttp.
- **Memory:** nextreq's peak RSS stays between 44 and 55 MB across scenarios (interpreter included); see each table for the per-library comparison.
<!-- BENCHMARKS:END -->

Reproduce with `maturin develop --release && python benches/run_benchmarks.py`.
Loopback benchmarks flatter fast clients: against a real network the round trip
dominates and the gaps narrow. What stays is the lower CPU cost per request.

## How it compares

| | nextreq | requests | httpx | aiohttp |
|---|:---:|:---:|:---:|:---:|
| Sync API | ✅ | ✅ | ✅ | — |
| Async API | ✅ | — | ✅ | ✅ |
| HTTP/2 | ✅ default | — | ✅ opt-in extra | — |
| HTTP/3 | opt-in build | — | — | — |
| Timeouts by default | ✅ 5/30/30/10 s | — none | ✅ 5 s | ✅ 300 s total |
| Built-in retries + `Retry-After` | ✅ | via urllib3 adapter | connect only | — |
| Typed responses (`model=`) | ✅ | — | — | — |
| Fully typed API + stubs | ✅ | third-party stubs | ✅ | ✅ |
| Server-Sent Events | ✅ built in | — | extra package | extra package |
| WebSockets | ✅ sync + async | — | extra package | ✅ async |
| Bounded-concurrency `gather()` | ✅ | — | — | — |
| Client-side rate limiting | ✅ | — | — | — |
| Pagination | ✅ `paginate()` | `links` only | `links` only | `links` only |
| Resumable downloads to a file | ✅ | — | — | — |
| Unix domain sockets | ✅ | extra package | ✅ | ✅ |
| Mock transport built in | ✅ | extra package | ✅ basic | extra package |
| In-process ASGI / WSGI | ✅ / ✅ | — | ✅ / ✅ | — |
| Per-phase timings | ✅ | total only | total only | via tracing hooks |
| OpenTelemetry hooks | ✅ built in | separate instrumentation | separate instrumentation | separate instrumentation |
| gzip / deflate / br / zstd | ✅ all built in | gzip, deflate (+extras) | gzip, deflate (+extras) | gzip, deflate (+extras) |
| Runtime dependencies | none | 4 | 4+ | 6+ |
| Engine | Rust (Tokio, hyper, rustls) | Python (urllib3) | Python (httpcore, h11) | Python + C parser |

### The same task, side by side

Fetch 100 items concurrently (at most 20 at a time), retry transient failures,
and parse each into a model.

```python
# nextreq
async with nextreq.AsyncClient(base_url=API, retries=3) as client:
    responses = await client.gather([f"/items/{n}" for n in range(100)], max_concurrency=20)
    items = [r.json(model=Item) for r in responses]
```

```python
# httpx (+ tenacity for status retries)
transport = httpx.AsyncHTTPTransport(retries=3)               # connection errors only
async with httpx.AsyncClient(base_url=API, transport=transport) as client:
    semaphore = asyncio.Semaphore(20)

    @tenacity.retry(stop=tenacity.stop_after_attempt(4), wait=tenacity.wait_exponential(0.5))
    async def fetch(n):
        async with semaphore:
            response = await client.get(f"/items/{n}")
            response.raise_for_status()
            return Item.model_validate(response.json())

    items = await asyncio.gather(*(fetch(n) for n in range(100)))
```

```python
# requests (threads + urllib3 Retry)
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=20))

def fetch(n):
    response = session.get(f"{API}/items/{n}", timeout=(5, 30))   # no timeout by default!
    response.raise_for_status()
    return Item.model_validate(response.json())

with ThreadPoolExecutor(max_workers=20) as pool:
    items = list(pool.map(fetch, range(100)))
```

### Migrating

| requests / httpx | nextreq |
|---|---|
| `requests.get(url, timeout=(5, 30))` | `nextreq.get(url)` - those are the defaults |
| `requests.Session()` / `httpx.Client()` | `nextreq.Client()` |
| `httpx.AsyncClient()` / `aiohttp.ClientSession()` | `nextreq.AsyncClient()` |
| `r.status_code`, `r.headers`, `r.text`, `r.content`, `r.json()`, `r.ok`, `r.elapsed` | identical |
| `r.raise_for_status()` | identical (returns the response, so it chains) |
| `allow_redirects=False` (requests) | `follow_redirects=False` |
| `httpx.Client(follow_redirects=True)` | default in nextreq |
| `proxies={"https": url}` (requests) | `proxy=url` or `proxy={"https://": url}` |
| `verify="ca.pem"`, `cert=("c.pem", "k.pem")` | identical |
| `HTTPAdapter(max_retries=Retry(...))` | `retries=3` or `retries=RetryPolicy(...)` |
| `r.links["next"]["url"]` | identical (URLs are made absolute); or `client.paginate(url)` |
| `aiolimiter` / `ratelimit` around each call | `rate_limit=10` |
| `httpx.HTTPTransport(uds=path)` / `aiohttp.UnixConnector(path)` | `nextreq.Client(uds=path)` |
| `httpx.MockTransport(handler)` / `responses` / `respx` | `nextreq.MockTransport()` |
| `httpx.ASGITransport(app=app)` | `nextreq.AsyncClient(app=app)` |
| `requests.exceptions.ConnectionError`, `Timeout` | `nextreq.ConnectError` (also builtin `ConnectionError`), `nextreq.TimeoutException` (also builtin `TimeoutError`) |

## Architecture

```
            your code
                │
   ┌────────────▼─────────────────────────────────────────────┐
   │ python/nextreq   Client · AsyncClient · gather()          │  HTTP semantics:
   │                  redirects · cookies · auth · retries     │  identical for real
   │                  hooks · typed parsing · otel             │  and mocked transports
   └───────┬───────────────────────────┬──────────────────────┘
           │ Rust transport            │ MockTransport · ASGI · WSGI
   ┌───────▼──────────────────────────────────────────────────┐
   │ _nextreq (PyO3)                                           │  bytes on the wire,
   │   HttpClient      reqwest + hyper (HTTP/1.1, HTTP/2)      │  GIL released
   │   Timeout / RetryPolicy / TLS (rustls, native + webpki)   │
   │   SSEParser · SSEEvent · WebSocket (yawc, deflate)        │
   │   shared multi-threaded Tokio runtime                     │
   └──────────────────────────────────────────────────────────┘
```

- **Sync calls** park the calling thread in `Runtime::block_on` with the interpreter
  detached, waking every 100 ms only to let Python deliver signals (Ctrl-C works).
- **Async calls** run on the Tokio runtime. Finished requests, stream reads and
  WebSocket operations are queued without touching the interpreter and delivered to
  asyncio in batches by a single `call_soon_threadsafe`, so I/O threads never fight
  the event loop for the GIL.
- **Uploads** flow through a small bounded queue: the sync client pulls the next
  chunk from your iterator on the calling thread, the async client from the event
  loop (files: from a worker thread), and only when the connection has taken the
  previous ones.
- **Timeouts** are enforced per request in Rust: pool (semaphore), connect (a
  connector layer covering DNS + TCP + TLS), write (upload progress) and read
  (headers, then each body chunk).
- **Free-threading:** the extension declares itself safe without the GIL; every
  exposed class is frozen and guards its state with Rust synchronisation. Wheels
  target abi3 (CPython 3.10+) and, from 3.15, abi3t (PEP 803).
- **Forking:** the runtime's threads do not survive `os.fork()`, so nextreq starts a
  new runtime and new connection pools in the child. Clients created before the fork
  (gunicorn `--preload`, `multiprocessing`) keep working there.

## Limitations

Known gaps, in rough priority order:

- **HTTP/3 is opt-in** and requires a custom build
  (`RUSTFLAGS="--cfg reqwest_unstable" maturin build --features http3`), because the
  underlying support is still marked unstable upstream. The published wheels do not
  include it, and it is only compile-checked here, not tested against a server.
- **No NTLM, Negotiate or Kerberos auth.** Write an `auth_flow` for a scheme that is
  not built in.
- **`max_connections` counts connections in use**, not idle ones. With one host the
  socket count stays at the limit; idle keep-alive connections to *other* hosts come
  on top until they expire (`keepalive_expiry`, `max_keepalive_connections`).
- **After `fork()`**, responses and WebSockets that were open in the parent cannot be
  used in the child, and forking while another thread is in the middle of a request
  is not supported (CPython itself warns about forking a multi-threaded process).
- **The Public Suffix List is compiled in**, so it is as fresh as the release.
- **Unix domain sockets are not available on Windows**; `uds=` raises
  `UnsupportedProtocol` there.

## Development

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
maturin develop --release
ruff check python tests benches
mypy --strict python/nextreq tests/test_typing.py
pytest -v
python benches/run_benchmarks.py
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the project layout and conventions, and
[SECURITY.md](SECURITY.md) for the security policy.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
