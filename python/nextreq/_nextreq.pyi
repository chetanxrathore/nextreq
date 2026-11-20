"""Type stubs for the compiled Rust extension ``nextreq._nextreq``."""

from asyncio import AbstractEventLoop, Future
from collections.abc import Awaitable, Iterable, Iterator
from typing import Any, Final, final

__version__: Final[str]
HTTP3_ENABLED: Final[bool]
DEFAULT_CONNECT_TIMEOUT: Final[float]
DEFAULT_READ_TIMEOUT: Final[float]
DEFAULT_WRITE_TIMEOUT: Final[float]
DEFAULT_POOL_TIMEOUT: Final[float]

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
class SSEEvent:
    """One Server-Sent Event (immutable).

    Attributes:
        event: event type; ``"message"`` when the stream did not name one.
        data: payload; multiple ``data:`` lines are joined with ``\\n``.
        id: the last event ID seen on the stream (persists across events).
        retry: reconnection time in milliseconds, if this event set one.
    """

    __match_args__: Final = ("event", "data", "id", "retry")

    def __init__(
        self,
        event: str | None = None,
        data: str | None = None,
        id: str | None = None,
        retry: int | None = None,
    ) -> None: ...
    @property
    def event(self) -> str: ...
    @property
    def data(self) -> str: ...
    @property
    def id(self) -> str | None: ...
    @property
    def retry(self) -> int | None: ...
    def json(self) -> Any:
        """Parse ``data`` as JSON."""

    def encode(self) -> bytes:
        """Serialise to the ``text/event-stream`` wire format."""

    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __repr__(self) -> str: ...

@final
class SSEParser:
    """Incremental WHATWG Server-Sent Events parser."""

    def __init__(self) -> None: ...
    def feed(self, data: bytes) -> list[SSEEvent]: ...
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
    def read_events(self) -> list[SSEEvent] | None: ...
    def aread_events(self) -> Awaitable[list[SSEEvent] | None]: ...
    def close(self) -> None: ...
    @property
    def is_closed(self) -> bool: ...

@final
class LoopBridge:
    """Batches results from the Tokio runtime onto one asyncio event loop."""

    def drain(self) -> None: ...
    @property
    def pending(self) -> int: ...

def loop_bridge(event_loop: AbstractEventLoop) -> LoopBridge:
    """The bridge of ``event_loop`` (one per loop, created on first use)."""

def request_target(url: str) -> str:
    """The request target (``/path?query``) as sent on the wire for ``url``."""

def after_fork_in_child() -> None:
    """Abandon the parent's I/O runtime in a forked child (see ``os.register_at_fork``)."""

def is_public_suffix(domain: str) -> bool:
    """Whether ``domain`` is a public suffix (``com``, ``co.uk``, ``github.io``)."""

@final
class UploadStream:
    """The producing side of a streamed request body (``AsyncClient``)."""

    def __init__(self) -> None: ...
    def try_send(self, chunk: bytes) -> int:
        """0 = queued, 1 = queue full (use ``send``), 2 = the body is no longer wanted."""
    def send(self, chunk: bytes) -> Awaitable[bool]: ...
    def finish(self) -> None: ...
    def fail(self) -> None: ...

@final
class TaskHandle:
    """Handle to an in-flight asynchronous request."""

    def abort(self) -> None: ...
    def abort_if_cancelled(self, future: Future[Any]) -> None: ...
    @property
    def is_finished(self) -> bool: ...

@final
class WebSocketConnection:
    """An open WebSocket connection."""

    def send_text(self, text: str) -> Awaitable[None]: ...
    def send_text_blocking(self, text: str) -> None: ...
    def send_bytes(self, data: bytes) -> Awaitable[None]: ...
    def send_bytes_blocking(self, data: bytes) -> None: ...
    def ping(self, data: bytes = b"") -> Awaitable[None]: ...
    def ping_blocking(self, data: bytes = b"") -> None: ...
    def receive(self) -> Awaitable[WebSocketMessage]: ...
    def receive_blocking(self, timeout: float | None = None) -> WebSocketMessage: ...
    def close(self, code: int = 1000, reason: str = "") -> Awaitable[None]: ...
    def close_blocking(self, code: int = 1000, reason: str = "") -> None: ...
    @property
    def subprotocol(self) -> str | None: ...
    @property
    def response_headers(self) -> list[tuple[str, str]]: ...
    @property
    def compressed(self) -> bool: ...

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
        body: bytes | Iterator[bytes] | None = None,
        timeout: Timeout | None = None,
        stream: bool = False,
    ) -> RawResponse: ...
    def send_async(
        self,
        bridge: LoopBridge,
        future: Future[RawResponse],
        method: str,
        url: str,
        headers: Iterable[tuple[str, str]],
        body: bytes | UploadStream | None = None,
        timeout: Timeout | None = None,
        stream: bool = False,
    ) -> TaskHandle:
        """Run the request on the Tokio runtime and resolve ``future`` with the result."""
    def ws_connect(
        self,
        url: str,
        headers: list[tuple[str, str]],
        subprotocols: list[str] | None = None,
        timeout: Timeout | None = None,
        max_message_size: int | None = None,
        compression: bool = False,
    ) -> Awaitable[WebSocketConnection]: ...
    def ws_connect_blocking(
        self,
        url: str,
        headers: list[tuple[str, str]],
        subprotocols: list[str] | None = None,
        timeout: Timeout | None = None,
        max_message_size: int | None = None,
        compression: bool = False,
    ) -> WebSocketConnection: ...
    def close(self) -> None: ...
    @property
    def is_closed(self) -> bool: ...
