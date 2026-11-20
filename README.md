# ⚡ nextreq

**The Rust-powered, batteries-included HTTP client for high-concurrency Python and AI workloads.**

[![PyPI version](https://img.shields.io/pypi/v/nextreq.svg)](https://pypi.org/project/nextreq/)
[![Python versions](https://img.shields.io/pypi/pyversions/nextreq.svg)](https://pypi.org/project/nextreq/)
[![License](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Free-Threaded Ready](https://img.shields.io/badge/python-free--threaded%20ready-brightgreen.svg)]()

Stop stitching together 5 different libraries just to make a resilient, typed, observable HTTP call.

`nextreq` pairs the human-friendly ergonomics of `requests` with a compiled **Rust core** (`hyper` + `reqwest` + `rustls` + `quinn`)—giving you HTTP/3, native Server-Sent Events (SSE), `Retry-After`-aware backoff, typed Pydantic/dataclass parsing, and built-in mocking in a single zero-config package.

---

## Why `nextreq`?

* **🦀 Rust Core, Pythonic API:** Connection pooling, TLS, HTTP/2, HTTP/3 (QUIC), and Brotli/Zstd decompression run outside the GIL—delivering 5–10x lower latency and memory overhead, ready for free-threaded Python (`3.13t`+).
* **🛡️ Safe by Default:** Built-in granular timeouts (`connect=5s`, `read=30s`), optional client-wide `raise_for_status=True`, and automatic `Retry-After` exponential backoff. No more hanging workers.
* **🤖 Built for AI & Realtime:** First-class `iter_sse()` for streaming LLM responses, native WebSockets, and bounded `client.gather(urls, max_concurrency=20)` for parallel agent tool calls.
* **🎯 Native Typed Parsing:** Deserialize and validate JSON directly into Pydantic models, dataclasses, or `msgspec` structs (`client.get(url, model=User)`) with full `mypy --strict` and `pyright` inference.
* **🧪 Zero-Boilerplate Testing & Tracing:** Built-in `MockTransport`, in-memory ASGI/WSGI testing, and optional OpenTelemetry instrumentation out of the box.

---

## Installation

```bash
pip install nextreq
# or with uv
uv add nextreq
```

Pre-built binary wheels are published for Linux (`manylinux`/`musllinux`), macOS (Apple Silicon & Intel), and Windows—**no Rust toolchain required**.

Optional extras:
```bash
pip install "nextreq[otel]"      # OpenTelemetry distributed tracing
```

---

## Quickstart

### 1. Typed JSON, Retries & HTTP/3 in One Client

```python
import nextreq
from pydantic import BaseModel

class Repo(BaseModel):
    name: str
    stargazers_count: int

async with nextreq.AsyncClient(
    base_url="[https://api.github.com](https://api.github.com)",
    http3=True,
    retries=3,                  # Exponential backoff + Retry-After aware
    timeout=10.0,               # Safe default even if omitted
    raise_for_status=True,      # Never forget r.raise_for_status() again
) as client:
    # Validated, statically typed return value (repo: Repo)
    repo = await client.get("/repos/nextreq/nextreq", model=Repo)
    print(f"{repo.name}: {repo.stargazers_count} stars")
```

### 2. First-Class SSE Streaming (AI / LLM Workloads)

```python
async with nextreq.AsyncClient(retries=3) as client:
    async with client.stream(
        "POST",
        "[https://api.example.com/v1/chat/completions](https://api.example.com/v1/chat/completions)",
        json={"model": "gpt-5", "stream": True, "messages": [{"role": "user", "content": "Hello"}]},
    ) as response:
        async for event in response.aiter_sse():
            if event.data == "[DONE]":
                break
            print(event.data, end="", flush=True)
```

### 3. Bounded High-Concurrency Fan-Out

```python
urls = [f"[https://api.example.com/items/](https://api.example.com/items/){i}" for i in range(500)]

async with nextreq.AsyncClient(http3=True, retries=2) as client:
    # Reuses the Rust connection pool with a strict concurrency cap
    responses = await client.gather(
        [client.build_request("GET", url) for url in urls],
        max_concurrency=25,
    )
```

---

## Before & After: Ending the "5-Library Tax"

Making a resilient, typed, streaming API call in Python today usually means wiring together `httpx` + `tenacity` + `httpx-sse` + manual Pydantic validation.

### ❌ The Old Way (4 packages, ~35 lines of glue)

```python
import httpx
from httpx_sse import aconnect_sse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
)
async def fetch_user(client: httpx.AsyncClient, user_id: int) -> User:
    resp = await client.get(f"[https://api.example.com/users/](https://api.example.com/users/){user_id}")
    resp.raise_for_status()
    return User.model_validate(resp.json())

async with httpx.AsyncClient(http2=True, timeout=10.0) as client:
    user = await fetch_user(client, 42)
    async with aconnect_sse(client, "GET", "[https://api.example.com/events](https://api.example.com/events)") as source:
        async for sse in source.aiter_sse():
            print(sse.data)
```

### ✅ The `nextreq` Way (1 package, 10 lines)

```python
import nextreq
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str

async with nextreq.AsyncClient(
    base_url="[https://api.example.com](https://api.example.com)",
    retries=3,
    raise_for_status=True,
) as client:
    user = await client.get("/users/42", model=User)
    async with client.stream("GET", "/events") as r:
        async for sse in r.aiter_sse():
            print(sse.data)
```

---

## How `nextreq` Compares

| Feature | `nextreq` | `requests` | `httpx` / `httpx2` | `aiohttp` |
|---|---|---|---|---|
| **Core Engine** | **Rust (`hyper` / `rustls`)** | Pure Python (`urllib3`) | Pure Python (`httpcore`) | Python + Cython |
| **Sync & Async** | Unified | Sync only | Unified | Async only |
| **HTTP/2 & HTTP/3** | **Default (built-in)** | HTTP/1.1 only | HTTP/2 optional | HTTP/1.1 only |
| **Safe Default Timeouts** | **Yes** | No (hangs forever) | Yes | Yes |
| **Built-in Retries (`Retry-After`)** | **Yes** | Manual adapter | External (`tenacity`) | External |
| **Native Typed JSON Parsing** | **Yes** | No | No | No |
| **Native SSE & WebSockets** | **Yes** | No | WS optional / No SSE | WS only |
| **Native Mocking & ASGI/WSGI** | **Yes** | External (`responses`) | ASGI only (`respx` ext.) | Test server |

---

## Built-in Testing & Mocking

No external monkey-patching libraries required. Mock routes declaratively or mount FastAPI/Starlette/Flask apps directly in memory:

```python
import nextreq

def test_user_fetch():
    mock = nextreq.MockTransport()
    mock.get("[https://api.example.com/users/1](https://api.example.com/users/1)").respond(
        status_code=200,
        json={"id": 1, "name": "Ada"},
    )

    with nextreq.Client(transport=mock) as client:
        r = client.get("[https://api.example.com/users/1](https://api.example.com/users/1)")
        assert r.json()["name"] == "Ada"
        mock.assert_called_once()
```

Testing a FastAPI app directly in memory:

```python
from myapp import app
import nextreq

async def test_fastapi_endpoint():
    async with nextreq.AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/health")
        assert r.status_code == 200
```

---

## Contributing

Contributions are welcome across both the Rust core (`src/`) and Python API
