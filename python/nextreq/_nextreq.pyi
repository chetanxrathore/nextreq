"""Type stubs for the compiled Rust extension ``nextreq._nextreq``."""

from collections.abc import Awaitable, Iterable
from typing import Final, final

__version__: Final[str]
HTTP3_ENABLED: Final[bool]
DEFAULT_CONNECT_TIMEOUT: Final[float]
DEFAULT_READ_TIMEOUT: Final[float]
DEFAULT_WRITE_TIMEOUT: Final[float]
DEFAULT_POOL_TIMEOUT: Final[float]

# (event, data, id, retry)
SSETuple = tuple[str, str, str | None, int | None]
# (total, pool, connect, ttfb, download) in seconds
RawTimings = tuple[float, float, float | None, float, float | None]
# (status, http_version, headers, body, timings)
RawResponse = tuple[int, str, list[tuple[str, str]], bytes | ResponseStream, RawTimings]
# (kind, payload, close_code): kind 0 = text, 1 = binary, 2 = closed
WebSocketMessage = tuple[int, str | bytes, int]

@final
class Timeout:
    """Granular request timeouts, in seconds (``None`` disables a phase)."""

    def __init__(
        self,
        timeout: float | Timeout | None = ...,
        *,
        connect: float | None = ...,
        read: float | None = ...,
        write: float | None = ...,
        pool: float | None = ...,
    ) -> None: ...
    @property
    def connect(self) -> float | None: ...
    @property
    def read(self) -> float | None: ...
    @property
    def write(self) -> float | None: ...
    @property
    def pool(self) -> float | None: ...
    def as_tuple(self) -> tuple[float | None, float | None, float | None, float | None]: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __repr__(self) -> str: ...

@final
class RetryPolicy:
    """Declarative retry policy; ``max_attempts`` counts retries after the first try."""

    def __init__(
        self,
        max_attempts: int = 3,
        *,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        jitter: bool | float | None = None,
        retry_statuses: Iterable[int] | None = None,
        retry_methods: Iterable[str] | None = None,
        respect_retry_after: bool = True,
        max_retry_after: float = 120.0,
    ) -> None: ...
    @property
    def max_attempts(self) -> int: ...
    @property
    def backoff_factor(self) -> float: ...
    @property
    def max_backoff(self) -> float: ...
    @property
    def jitter(self) -> float: ...
    @property
    def retry_statuses(self) -> frozenset[int]: ...
    @property
    def retry_methods(self) -> frozenset[str]: ...
    @property
    def respect_retry_after(self) -> bool: ...
    @property
    def max_retry_after(self) -> float: ...
    def allows_method(self, method: str) -> bool: ...
    def allows_status(self, status: int) -> bool: ...
    def delay(self, attempt: int, retry_after: str | None = None) -> float: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...

def parse_retry_after(value: str) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds."""

@final
class SSEParser:
    """Incremental WHATWG Server-Sent Events parser."""

    def __init__(self) -> None: ...
    def feed(self, data: bytes) -> list[SSETuple]: ...
    def finish(self) -> None: ...
    @property
    def last_event_id(self) -> str | None: ...
    @property
    def retry(self) -> int | None: ...

@final
class ResponseStream:
    """Handle to a response body that is still being received."""

    def read(self) -> bytes | None: ...
    def aread(self) -> Awaitable[bytes | None]: ...
    def read_all(self) -> bytes: ...
    def aread_all(self) -> Awaitable[bytes]: ...
    def read_events(self) -> list[SSETuple] | None: ...
    def aread_events(self) -> Awaitable[list[SSETuple] | None]: ...
    def close(self) -> None: ...
    @property
    def is_closed(self) -> bool: ...

@final
class WebSocketConnection:
    """An open WebSocket connection."""

    def send_text(self, text: str) -> Awaitable[None]: ...
    def send_bytes(self, data: bytes) -> Awaitable[None]: ...
    def ping(self, data: bytes = b"") -> Awaitable[None]: ...
    def receive(self) -> Awaitable[WebSocketMessage]: ...
    def close(self, code: int = 1000, reason: str = "") -> Awaitable[None]: ...
    @property
    def subprotocol(self) -> str | None: ...
    @property
    def response_headers(self) -> list[tuple[str, str]]: ...

@final
class HttpClient:
    """The Rust HTTP engine: one exchange per call, no redirects/cookies/retries."""

    def __init__(
        self,
        *,
        verify: bool = True,
        ca_bundle: str | None = None,
        cert: str | None = None,
        key: str | None = None,
        proxies: list[tuple[str, str]] | None = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = True,
        http3: bool = False,
        max_connections: int | None = 100,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = 30.0,
    ) -> None: ...
    def send(
        self,
        method: str,
        url: str,
        headers: Iterable[tuple[str, str]],
        body: bytes | None = None,
        timeout: Timeout | None = None,
        stream: bool = False,
    ) -> RawResponse: ...
    def send_async(
        self,
        method: str,
        url: str,
        headers: Iterable[tuple[str, str]],
        body: bytes | None = None,
        timeout: Timeout | None = None,
        stream: bool = False,
    ) -> Awaitable[RawResponse]: ...
    def ws_connect(
        self,
        url: str,
        headers: list[tuple[str, str]],
        subprotocols: list[str] | None = None,
        timeout: Timeout | None = None,
        max_message_size: int | None = None,
    ) -> Awaitable[WebSocketConnection]: ...
    def close(self) -> None: ...
    @property
    def is_closed(self) -> bool: ...
