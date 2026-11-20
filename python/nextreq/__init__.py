"""nextreq - a next-generation, Rust-powered, batteries-included HTTP client.

::

    import nextreq

    r = nextreq.get("https://httpbin.org/get", params={"q": "rust"})
    r.raise_for_status()
    print(r.json())

    with nextreq.Client(base_url="https://api.example.com", retries=3) as client:
        user = client.get("/users/1", model=User)        # typed: `User`

    async with nextreq.AsyncClient() as client:
        responses = await client.gather(urls, max_concurrency=20)
"""

from __future__ import annotations

import os as _os

from . import _nextreq
from ._nextreq import HTTP3_ENABLED, RetryPolicy, Timeout, parse_retry_after
from .client import (
    AsyncClient,
    AsyncTransport,
    Auth,
    BasicAuth,
    BearerAuth,
    Client,
    DigestAuth,
    SyncWebSocket,
    Transport,
    WebSocket,
    delete,
    gather,
    get,
    head,
    options,
    patch,
    post,
    put,
    request,
    stream,
)
from .exceptions import (
    ClientClosed,
    ConnectError,
    ConnectTimeout,
    DecodeError,
    DecodingError,
    HTTPStatusError,
    InvalidURL,
    LocalProtocolError,
    NetworkError,
    NextReqError,
    PoolTimeout,
    ProtocolError,
    ProxyError,
    ReadError,
    ReadTimeout,
    RemoteProtocolError,
    ResponseNotRead,
    StreamClosed,
    StreamConsumed,
    StreamError,
    TimeoutException,
    TooManyRedirects,
    TransportError,
    UnsupportedProtocol,
    WebSocketDisconnect,
    WebSocketError,
    WriteError,
    WriteTimeout,
)
from .models import URL, Cookies, Elapsed, Headers, Request, RequestStream, Response, SSEEvent
from .testing import ASGITransport, MockRouteNotFound, MockTransport, WSGITransport

__version__: str = _nextreq.__version__

# The I/O runtime's threads do not survive os.fork(). Tell the extension when
# this process is a forked child, so it starts a fresh runtime and rebuilds
# connection pools on first use instead of waiting on threads that are gone.
if hasattr(_os, "register_at_fork"):
    _os.register_at_fork(after_in_child=_nextreq.after_fork_in_child)

__all__ = [
    "HTTP3_ENABLED",
    "URL",
    "ASGITransport",
    "AsyncClient",
    "AsyncTransport",
    "Auth",
    "BasicAuth",
    "BearerAuth",
    "Client",
    "ClientClosed",
    "ConnectError",
    "ConnectTimeout",
    "Cookies",
    "DecodeError",
    "DecodingError",
    "DigestAuth",
    "Elapsed",
    "HTTPStatusError",
    "Headers",
    "InvalidURL",
    "LocalProtocolError",
    "MockRouteNotFound",
    "MockTransport",
    "NetworkError",
    "NextReqError",
    "PoolTimeout",
    "ProtocolError",
    "ProxyError",
    "ReadError",
    "ReadTimeout",
    "RemoteProtocolError",
    "Request",
    "RequestStream",
    "Response",
    "ResponseNotRead",
    "RetryPolicy",
    "SSEEvent",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
    "SyncWebSocket",
    "Timeout",
    "TimeoutException",
    "TooManyRedirects",
    "Transport",
    "TransportError",
    "UnsupportedProtocol",
    "WSGITransport",
    "WebSocket",
    "WebSocketDisconnect",
    "WebSocketError",
    "WriteError",
    "WriteTimeout",
    "__version__",
    "delete",
    "gather",
    "get",
    "head",
    "options",
    "parse_retry_after",
    "patch",
    "post",
    "put",
    "request",
    "stream",
]
