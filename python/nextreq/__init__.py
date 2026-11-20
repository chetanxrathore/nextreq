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

from . import _nextreq
from ._nextreq import HTTP3_ENABLED, RetryPolicy, Timeout, parse_retry_after
from .client import (
    AsyncClient,
    AsyncTransport,
    Auth,
    BasicAuth,
    BearerAuth,
    Client,
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
from .models import URL, Cookies, Elapsed, Headers, Request, Response, SSEEvent
from .testing import ASGITransport, MockRouteNotFound, MockTransport, WSGITransport

__version__: str = _nextreq.__version__

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
    "Response",
    "ResponseNotRead",
    "RetryPolicy",
    "SSEEvent",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
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
