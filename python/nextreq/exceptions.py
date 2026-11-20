"""The nextreq exception hierarchy.

::

    NextReqError
    ├── TransportError                  (no usable response was received)
    │   ├── TimeoutException            (also a builtin ``TimeoutError``)
    │   │   ├── ConnectTimeout
    │   │   ├── ReadTimeout
    │   │   ├── WriteTimeout
    │   │   └── PoolTimeout
    │   ├── NetworkError
    │   │   ├── ConnectError            (also a builtin ``ConnectionError``)
    │   │   ├── ReadError
    │   │   └── WriteError
    │   ├── ProxyError
    │   ├── ProtocolError
    │   │   ├── LocalProtocolError
    │   │   └── RemoteProtocolError
    │   ├── UnsupportedProtocol
    │   ├── DecodingError               (corrupt gzip/br/zstd/deflate body)
    │   ├── TooManyRedirects
    │   └── WebSocketError
    │       └── WebSocketDisconnect
    ├── HTTPStatusError                 (4xx / 5xx with ``raise_for_status``)
    ├── DecodeError                     (typed JSON parsing / validation failed)
    ├── InvalidURL
    ├── StreamError
    │   ├── StreamConsumed
    │   ├── StreamClosed
    │   └── ResponseNotRead
    └── ClientClosed

Transport-level exceptions are raised by the Rust core (see ``src/errors.rs``)
and carry the originating :class:`~nextreq.Request` as ``.request`` when known.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import Request, Response

__all__ = [
    "ClientClosed",
    "ConnectError",
    "ConnectTimeout",
    "DecodeError",
    "DecodingError",
    "HTTPStatusError",
    "InvalidURL",
    "LocalProtocolError",
    "NetworkError",
    "NextReqError",
    "PoolTimeout",
    "ProtocolError",
    "ProxyError",
    "ReadError",
    "ReadTimeout",
    "RemoteProtocolError",
    "ResponseNotRead",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
    "TimeoutException",
    "TooManyRedirects",
    "TransportError",
    "UnsupportedProtocol",
    "WebSocketDisconnect",
    "WebSocketError",
    "WriteError",
    "WriteTimeout",
]


class NextReqError(Exception):
    """Base class for every exception raised by nextreq."""

    def __init__(self, message: str = "", *, request: Request | None = None) -> None:
        super().__init__(message)
        self._request = request

    @property
    def request(self) -> Request | None:
        """The request that was being processed, if known."""
        return self._request

    @request.setter
    def request(self, request: Request | None) -> None:
        self._request = request


# -- transport ---------------------------------------------------------------


class TransportError(NextReqError):
    """The request could not be completed: no usable response was received."""


class TimeoutException(TransportError, TimeoutError):
    """Base class for all timeouts. Also catchable as builtin ``TimeoutError``."""


class ConnectTimeout(TimeoutException):
    """DNS resolution, TCP connect or TLS handshake exceeded the connect timeout."""


class ReadTimeout(TimeoutException):
    """The server sent no data for longer than the read timeout."""


class WriteTimeout(TimeoutException):
    """Uploading the request body made no progress for longer than the write timeout."""


class PoolTimeout(TimeoutException):
    """No connection slot became free within the pool timeout."""


class NetworkError(TransportError):
    """A network-level failure while talking to the server."""


class ConnectError(NetworkError, ConnectionError):
    """A connection could not be established (DNS, refused, TLS failure...)."""


class ReadError(NetworkError):
    """The connection failed while receiving data."""


class WriteError(NetworkError):
    """The connection failed while sending data."""


class ProxyError(TransportError):
    """Connecting through the configured proxy failed."""


class ProtocolError(TransportError):
    """The HTTP protocol was violated."""


class LocalProtocolError(ProtocolError):
    """The request we were asked to send is not valid (bad header, bad TLS config...)."""


class RemoteProtocolError(ProtocolError):
    """The server sent a malformed or truncated response."""


class UnsupportedProtocol(TransportError):
    """The URL scheme is not supported."""


class DecodingError(TransportError):
    """The response body could not be decompressed."""


class TooManyRedirects(TransportError):
    """The redirect limit was exceeded."""


class WebSocketError(TransportError):
    """A WebSocket handshake or protocol failure."""


class WebSocketDisconnect(WebSocketError):
    """The WebSocket connection was closed by the peer."""

    def __init__(self, code: int = 1006, reason: str = "") -> None:
        detail = f"WebSocket closed with code {code}"
        if reason:
            detail += f": {reason}"
        super().__init__(detail)
        self.code = code
        self.reason = reason


# -- HTTP semantics ----------------------------------------------------------


class HTTPStatusError(NextReqError):
    """A 4xx or 5xx response, raised by ``Response.raise_for_status()``."""

    def __init__(self, message: str, *, request: Request | None, response: Response) -> None:
        super().__init__(message, request=request)
        self.response = response

    @property
    def status_code(self) -> int:
        return self.response.status_code


class DecodeError(NextReqError, ValueError):
    """The response body could not be parsed into the requested type.

    Attributes:
        status_code: HTTP status of the response that failed to parse.
        url: URL of the response.
        model: the type that was requested.
        errors: structured validation errors, when the underlying library
            provides them (e.g. Pydantic's ``ValidationError.errors()``).
        response: the full response, for inspection of the raw body.
    """

    def __init__(
        self,
        message: str,
        *,
        response: Response | None = None,
        model: Any = None,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        request = response.request if response is not None else None
        super().__init__(message, request=request)
        self.response = response
        self.model = model
        self.errors: list[dict[str, Any]] = errors or []
        self.status_code: int | None = response.status_code if response is not None else None
        self.url: str | None = str(response.url) if response is not None else None


class InvalidURL(NextReqError, ValueError):
    """The URL is malformed."""


# -- streams / lifecycle -----------------------------------------------------


class StreamError(NextReqError, RuntimeError):
    """A streaming response was used incorrectly."""


class StreamConsumed(StreamError):
    """The response stream has already been iterated."""

    def __init__(self, message: str = "") -> None:
        super().__init__(
            message
            or "The response stream has already been consumed. "
            "Streamed bodies can only be iterated once; call .read() first to buffer it."
        )


class StreamClosed(StreamError):
    """The response stream was closed before it was fully read."""

    def __init__(self, message: str = "") -> None:
        super().__init__(message or "The response stream has been closed.")


class ResponseNotRead(StreamError):
    """Body was accessed on a streaming response before calling ``read()``."""

    def __init__(self, message: str = "") -> None:
        super().__init__(
            message
            or "The response body has not been read yet. "
            "Call .read() (or `await .aread()`) on a streaming response first."
        )


class ClientClosed(NextReqError, RuntimeError):
    """The client was used after being closed."""
