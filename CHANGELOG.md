# Changelog

## 0.2.0 - 2026-11-20

This release removes most of the limitations listed for 0.1.

### Added

- **Streamed uploads.** `content=` accepts generators, async generators and open
  files, and `files=` streams open files. Chunks are pulled as the connection takes
  them, so memory use no longer grows with the size of the upload. Seekable files
  and re-iterable bodies are replayed for retries and `307`/`308` redirects.
- **Blocking WebSockets:** `Client.websocket()` returns a `SyncWebSocket`.
- **WebSocket compression:** `compression=True` offers permessage-deflate (RFC 7692).
- **WebSockets through proxies:** `http://` (CONNECT) and `socks5://` proxies, the
  proxy environment variables and `NO_PROXY`.
- **`DigestAuth`** (RFC 7616) and response-aware auth flows: subclass `Auth` and
  implement `auth_flow`.
- **In-process transports stream.** `ASGITransport` and `WSGITransport` deliver the
  application's response as it is produced, and `ASGITransport` runs lifespan events
  (`lifespan="auto" | True | False`).
- **Fork safety.** After `os.fork()` the child starts its own I/O runtime and
  connection pools, so clients created before the fork keep working.
- **Public Suffix List** in the cookie jar: a site can no longer set a cookie for a
  whole suffix such as `co.uk` or `github.io`.
- `Request.stream`, `Request.read()` / `aread()` and the `RequestStream` type.

### Changed

- A generator passed as `content=` is now sent with chunked transfer encoding
  instead of being joined in memory. Pass `bytes` (or a `list` of chunks) to keep a
  `Content-Length`, or set the header yourself.
- A request with a one-shot streamed body is not retried, and a redirect that would
  need the body again raises `StreamConsumed`.
- `ASGITransport` runs the application's lifespan by default; pass `lifespan=False`
  for the old behaviour. A bare ASGI application is offered a `lifespan` scope
  before its first request, as a real server would do.
- The default limit for a received WebSocket message is 64 MiB.
- The WebSocket implementation moved from `tokio-tungstenite` to `yawc`, and the
  extension no longer depends on `pyo3-async-runtimes`: every awaitable is resolved
  through the batching loop bridge.

### Still not supported

See "Limitations" in the README: HTTP/3 needs a custom build, there is no
NTLM/Negotiate auth, and `max_connections` counts connections in use.

## 0.1.1

- Package metadata: author, licence expression, provenance attestations on PyPI.

## 0.1.0

- First release.
