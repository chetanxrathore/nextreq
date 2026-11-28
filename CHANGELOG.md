# Changelog

## 0.4.0 - 2026-11-28

This release adds what a program needs around the request itself when it works with a
real API: rate limits, pagination, JSON Lines and downloads.

### Added

- **Client-side rate limiting.** `rate_limit=10` (requests per second) or
  `rate_limit=RateLimit(600, per=60, burst=5)` on either client. Requests beyond the
  limit wait for their turn, evenly spaced. One `RateLimit` can be shared between
  clients and threads.
- **`paginate()`** on both clients: follows `Link: rel="next"` by default, or a
  `next_page` function that returns a URL, query parameters, or a `Request`.
- **`Response.links`**: the parsed `Link` header, keyed by relation, with absolute
  URLs.
- **JSON Lines:** `Response.iter_json_lines()` and `aiter_json_lines()`, with an
  optional `model=` for typed items.
- **`download()`** on both clients and as `nextreq.download()`. The body is streamed to
  a `.part` file and renamed when complete. With `retries`, a connection that breaks
  mid-body is continued with `Range`; with `resume=True`, a `.part` file from an
  earlier run is continued. `If-Range` guards against a file that changed.
- **Unix domain sockets:** `Client(uds="/var/run/docker.sock")`, for HTTP requests and
  WebSockets (not on Windows).

### Fixed

- The 0.3.0 heading in this file had a doubled `##`.

## 0.3.0 - 2026-11-25

This release closes the gaps that were left in the test transports, in WebSockets and
in Digest authentication.

### Added

- **Streamed request bodies reach in-process applications as a stream.** With
  `ASGITransport` the application receives one `http.request` message per chunk, and
  with `WSGITransport` it reads them from `wsgi.input`, both only as fast as it asks.
- **WSGI applications are streamed through `AsyncClient`.** The application runs in a
  worker thread of its own for as long as the response is open, instead of being
  buffered.
- **WebSockets through `https://` proxies**: TLS to the proxy, `CONNECT` inside it,
  and for `wss://` a second TLS session to the server inside the tunnel.
- **Authentication flows on the WebSocket handshake.** `DigestAuth` and custom
  `auth_flow` classes now answer a challenge on `client.websocket(...)`.
- **`WebSocketError.response`**: the server's answer (status, headers, body) when it
  refuses a handshake.
- **Digest `qop=auth-int`**, which also signs the request body.
- Custom transports can set `streams_requests = True` to receive streamed request
  bodies as a stream.

### Changed

- `AsyncClient` reads upload files in a worker thread, so a slow disk no longer
  stalls the event loop.
- A one-shot streamed body (a generator) sent to an ASGI or WSGI application is no
  longer retried and cannot follow a `307`/`308` redirect, exactly as on the network.
  Before, the test transports read the body into memory first, which hid this.
- With `ASGITransport` and `WSGITransport`, `request.content` is empty for a streamed
  body; read `request.stream` instead. `MockTransport` still reads the body into
  `request.content`.
- A WebSocket handshake that is refused while a `DigestAuth` (or another flow-based
  auth) is configured used to be sent without credentials; it is now authenticated.
- CI: the Windows job is required, and the workflows use Node 24 versions of the
  GitHub actions.

### Still not supported

See "Limitations" in the README: HTTP/3 needs a custom build, there is no
NTLM/Negotiate auth, and `max_connections` counts connections in use.

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
