# nextreq

**A next-generation, Rust-powered, batteries-included HTTP client for Python.**

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
  for async fan-out (3.4× with the built-in `gather()`). [Benchmarks](#benchmarks)
- **Sync and async, one API.** `Client` and `AsyncClient` share every option and
  every feature, on one connection pool implementation.
- **Typed end to end.** `client.get(url, model=User)` returns a `User` - statically
  (mypy/pyright) and at runtime (Pydantic v2, dataclasses or msgspec).
- **Safe by default.** Timeouts on every phase, TLS verification, bounded
  redirects, credentials dropped on cross-origin redirects.
- **Resilient.** Built-in retries with exponential backoff, jitter and `Retry-After`.
- **Realtime.** WHATWG-compliant SSE parser in Rust, WebSockets, and `gather()` for
  bounded-concurrency fan-out.
- **Testable.** `MockTransport`, plus in-memory transports for ASGI (FastAPI) and
  WSGI (Flask) apps - no sockets, no monkeypatching.
- **Observable.** Request/response/error hooks, per-phase timings, optional
  OpenTelemetry spans with W3C `traceparent` propagation.
- **Modern protocols.** HTTP/1.1 and HTTP/2 out of the box, gzip / deflate / brotli /
  zstd decoding, proxies, optional HTTP/3.
- **Zero mandatory dependencies.** Python 3.10+, one abi3 wheel per platform.

> **Status:** `0.1.0` release candidate. The API is small and tested (310 Python tests,
> 40 Rust tests; the suite also passes on Python 3.10 against the abi3 wheel, and a
> threaded smoke test passes on free-threaded CPython 3.14t) but young - see
> [Limitations](#limitations).

## Installation

```bash
pip install nextreq                # once published to PyPI
pip install "nextreq[pydantic]"    # optional extras: pydantic, msgspec, otel
```

From source (needs a Rust toolchain):

```bash
git clone https://github.com/nextreq/nextreq && cd nextreq
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
    auth=("user", "pass"),            # or nextreq.BearerAuth("token"), or any callable
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
`raise_for_status`, `retries`, `verify`, `cert`, `proxy`, `trust_env`, `http2`,
`max_connections` and `keepalive_expiry`. Every request method accepts per-request
overrides. The module-level helpers (`nextreq.get`, `post`, `put`, `patch`, `delete`,
`head`, `options`, `request`, `stream`) share a keep-alive pool but never share cookies.

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

### WebSockets

```python
async with aclient.websocket("wss://example.com/ws", subprotocols=["json"]) as ws:
    await ws.send_json({"subscribe": "ticker"})
    async for message in ws:                 # str or bytes; stops when the peer closes
        ...
```

`send_text` / `send_bytes` / `send_json`, `receive` / `receive_text` / `receive_bytes` /
`receive_json` (optionally with `timeout=`), `ping` and `close(code, reason)` are
available; a closed connection raises `WebSocketDisconnect(code, reason)`.

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

with nextreq.Client(app=flask_app) as client:                 # WSGI
    assert client.get("/health").json() == {"ok": True}
```

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
_Apple M2 Max · CPython 3.14.6 · nextreq 0.1.0 vs requests 2.34.2, httpx 0.28.1, aiohttp 3.14.5 · 2026-11-20 04:19 UTC. "nextreq speedup" is nextreq's throughput divided by that library's._

**1. Sequential sync GETs** - 500 GET requests for a small JSON document, one after another, over a single keep-alive connection.

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq** | 22,100 | baseline | 0.044 | 0.055 | 0.090 | 40.3 |
| aiohttp (awaited sequentially) | 12,487 | 1.77× | 0.079 | 0.088 | 0.096 | 53.8 |
| httpx | 4,900 | 4.51× | 0.200 | 0.229 | 0.420 | 67.1 |
| requests | 2,682 | 8.24× | 0.369 | 0.405 | 0.441 | 49.2 |

**2. High-concurrency async fan-out** - 1000 GET requests with at most 50 in flight.

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq (client.gather)** | 80,106 | — | 0.258 | 0.429 | 0.536 | 46.8 |
| **nextreq (asyncio.gather + Semaphore)** | 60,253 | baseline | 0.506 | 0.827 | 1.42 | 46.5 |
| aiohttp | 23,663 | 2.55× | 1.25 | 1.94 | 2.25 | 55.9 |
| requests (thread pool) | 3,242 | 18.58× | 14.0 | 25.7 | 33.0 | 53.7 |
| httpx | 599 | 100.53× | 56.0 | 227 | 328 | 71.6 |

**3. Typed JSON deserialization** - 500 GET requests for a ~1.3 kB JSON document, each validated into a Pydantic v2 model (nested models, a list of 10 sub-objects, a datetime).

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq** | 16,683 | baseline | 0.058 | 0.071 | 0.078 | 54.2 |
| aiohttp (awaited sequentially) | 8,387 | 1.99× | 0.114 | 0.154 | 0.197 | 64.2 |
| httpx | 3,414 | 4.89× | 0.268 | 0.422 | 0.667 | 77.5 |
| requests | 2,528 | 6.60× | 0.393 | 0.416 | 0.433 | 60.4 |

**4. SSE streaming throughput** - A stream of 10000 Server-Sent Events, consumed into event objects (event, data, id, retry).

| Library | Events/sec | nextreq speedup | Time per stream (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|
| **nextreq** | 2,829,154 | baseline | 3.53 | 41.7 |
| requests + hand-written parser | 846,400 | 3.34× | 11.8 | 49.7 |
| httpx + hand-written parser | 784,547 | 3.61× | 12.7 | 68.5 |
| aiohttp + hand-written parser | 195,832 | 14.45× | 51.1 | 54.0 |

**5. Large payload downloads (bonus)** - 100 sequential GET requests, each downloading a 1 MiB body.

| Library | Requests/sec | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **nextreq** | 5,426 | baseline | 0.180 | 0.233 | 0.264 | 46.8 |
| aiohttp (awaited sequentially) | 4,736 | 1.15× | 0.210 | 0.223 | 0.247 | 57.0 |
| httpx | 1,618 | 3.35× | 0.614 | 0.670 | 0.999 | 141.4 |
| requests | 1,208 | 4.49× | 0.819 | 0.894 | 1.03 | 53.0 |

**Takeaways**

- **Sequential sync GETs:** nextreq handles 22,100 req/s at a p50 of 0.044 ms - **8.2×** requests, **4.5×** httpx, **1.8×** aiohttp.
- **Async fan-out:** with the same `asyncio.gather` + `Semaphore` harness nextreq reaches 60,253 req/s - **2.5×** aiohttp, **100.5×** httpx, **18.6×** requests. The built-in `client.gather()` reaches 80,106 req/s (1.33× the asyncio.gather harness, 3.4× aiohttp).
- **Typed JSON:** `client.get(url, model=User)` (bytes validated straight into the model by pydantic-core) is **6.6×** requests, **4.9×** httpx, **2.0×** aiohttp doing `User.model_validate(response.json())`.
- **SSE:** the Rust parser delivers 2,829,154 events/s as ready-made `SSEEvent` objects - **3.3×** requests, **3.6×** httpx, **14.4×** aiohttp with a hand-written Python parser.
- **Large downloads:** for 1 MiB bodies, where memory copies rather than per-request overhead dominate, nextreq is **4.5×** requests, **3.4×** httpx, **1.1×** aiohttp.
- **Memory:** nextreq's peak RSS stays between 40 and 54 MB across scenarios (interpreter included); see each table for the per-library comparison.
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
| WebSockets | ✅ async | — | extra package | ✅ |
| Bounded-concurrency `gather()` | ✅ | — | — | — |
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
   │   SSEParser · SSEEvent · WebSocket (tokio-tungstenite)    │
   │   shared multi-threaded Tokio runtime                     │
   └──────────────────────────────────────────────────────────┘
```

- **Sync calls** park the calling thread in `Runtime::block_on` with the interpreter
  detached, waking every 100 ms only to let Python deliver signals (Ctrl-C works).
- **Async calls** run on the Tokio runtime. Finished requests are queued without
  touching the interpreter and delivered to asyncio in batches by a single
  `call_soon_threadsafe`, so I/O threads never fight the event loop for the GIL.
  WebSockets and stream reads use `pyo3-async-runtimes`.
- **Timeouts** are enforced per request in Rust: pool (semaphore), connect (a
  connector layer covering DNS + TCP + TLS), write (upload progress) and read
  (headers, then each body chunk).
- **Free-threading:** the extension declares itself safe without the GIL; every
  exposed class is frozen and guards its state with Rust synchronisation. Wheels
  target abi3 (CPython 3.10+) and, from 3.15, abi3t (PEP 803).

## Limitations

Known gaps in `0.1.0`, in rough priority order:

- **Uploads are buffered.** `content=` accepts an iterable of bytes but joins it in
  memory first; multipart bodies are built in memory too.
- **WebSockets are async-only**, do not go through proxies, and do not negotiate
  `permessage-deflate`.
- **ASGI / WSGI transports buffer the response** and do not run ASGI lifespan events.
- **No Digest/NTLM auth and no auth flows** that need to see a response (use an
  `on_response` hook or a custom `auth` callable).
- **`max_connections` bounds in-flight requests**, not sockets: during a burst the
  pool can briefly open one extra connection per waiting request.
- **The cookie jar does not use the Public Suffix List.**
- **HTTP/3 is opt-in** and requires a custom build
  (`RUSTFLAGS="--cfg reqwest_unstable" maturin build --features http3`), because the
  underlying support is still marked unstable upstream.
- **`fork()`**: like any library with background threads, create clients after
  forking, not before.

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
