"""Optional OpenTelemetry integration.

Nothing here is imported unless you use it, and ``opentelemetry-api`` is only
required for span recording::

    import nextreq
    from nextreq.otel import instrument

    client = instrument(nextreq.Client())      # spans + W3C traceparent injection

Each HTTP exchange (including every redirect hop and retry) becomes one
``SpanKind.CLIENT`` span named after the HTTP method, following the stable
OpenTelemetry HTTP semantic conventions.

If you only want trace-context propagation and no SDK, use
:func:`traceparent_hook`, which has no dependencies at all::

    client = nextreq.Client(on_request=nextreq.otel.traceparent_hook)
"""

from __future__ import annotations

import importlib
import os
import re
from typing import TYPE_CHECKING, Any, TypeVar

from .models import Request, Response

if TYPE_CHECKING:
    from .client import AsyncClient, Client

__all__ = [
    "OpenTelemetryHooks",
    "generate_traceparent",
    "instrument",
    "is_available",
    "parse_traceparent",
    "traceparent_hook",
]

ClientT = TypeVar("ClientT", "Client", "AsyncClient")

_SPAN_KEY = "nextreq.otel.span"
_TRACEPARENT_RE = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
_HTTP_VERSIONS = {"HTTP/1.0": "1.0", "HTTP/1.1": "1.1", "HTTP/2": "2", "HTTP/3": "3"}


def is_available() -> bool:
    """Whether ``opentelemetry-api`` is importable."""
    try:
        importlib.import_module("opentelemetry.trace")
    except ImportError:
        return False
    return True


# -- dependency-free W3C trace context ----------------------------------------


def generate_traceparent(trace_id: str | None = None, *, sampled: bool = True) -> str:
    """Create a W3C ``traceparent`` header value.

    Args:
        trace_id: 32 lowercase hex characters to continue an existing trace;
            a random trace ID is generated when omitted.
        sampled: sets the ``sampled`` trace flag.
    """
    if trace_id is None:
        trace_id = os.urandom(16).hex()
    elif not re.fullmatch(r"[0-9a-f]{32}", trace_id) or trace_id == "0" * 32:
        raise ValueError("trace_id must be 32 lowercase hex characters and not all zeros")
    span_id = os.urandom(8).hex()
    return f"00-{trace_id}-{span_id}-{'01' if sampled else '00'}"


def parse_traceparent(value: str) -> tuple[str, str, bool] | None:
    """Parse a ``traceparent`` header into ``(trace_id, parent_id, sampled)``."""
    match = _TRACEPARENT_RE.match(value.strip().lower())
    if match is None:
        return None
    version, trace_id, parent_id, flags = match.groups()
    if version == "ff" or trace_id == "0" * 32 or parent_id == "0" * 16:
        return None
    return trace_id, parent_id, bool(int(flags, 16) & 1)


def traceparent_hook(request: Request) -> None:
    """An ``on_request`` hook that adds a W3C ``traceparent`` header.

    If OpenTelemetry is installed and a span is active, its context is
    propagated; otherwise a fresh trace ID is generated. An existing
    ``traceparent`` header is never overwritten.
    """
    if "traceparent" in request.headers:
        return
    try:
        propagate = importlib.import_module("opentelemetry.propagate")
    except ImportError:
        propagate = None
    if propagate is not None:
        propagate.inject(request.headers)
        if "traceparent" in request.headers:
            return
    request.headers["traceparent"] = generate_traceparent()


# -- span recording -----------------------------------------------------------


def _redact_url(request: Request) -> str:
    """``url.full`` must not contain credentials."""
    return str(request.url.without_userinfo())


class OpenTelemetryHooks:
    """Request / response / error hooks that record client spans.

    Usually installed with :func:`instrument`, but the three methods can also
    be passed straight to a client::

        hooks = OpenTelemetryHooks()
        client = nextreq.Client(
            on_request=hooks.on_request,
            on_response=hooks.on_response,
            on_error=hooks.on_error,
        )
    """

    def __init__(self, *, tracer_provider: Any = None, tracer: Any = None) -> None:
        try:
            self._trace: Any = importlib.import_module("opentelemetry.trace")
            self._propagate: Any = importlib.import_module("opentelemetry.propagate")
        except ImportError as exc:
            raise ImportError(
                "OpenTelemetry tracing requires the `opentelemetry-api` package. "
                "Install it with `pip install 'nextreq[otel]'`."
            ) from exc
        if tracer is None:
            from . import __version__

            tracer = self._trace.get_tracer("nextreq", __version__, tracer_provider=tracer_provider)
        self._tracer: Any = tracer

    def on_request(self, request: Request) -> None:
        """Start a client span and inject its context into the request headers."""
        url = request.url
        attributes: dict[str, Any] = {
            "http.request.method": request.method,
            "url.full": _redact_url(request),
            "server.address": url.host,
        }
        port = url.effective_port
        if port is not None:
            attributes["server.port"] = port
        if url.scheme:
            attributes["url.scheme"] = url.scheme
        span = self._tracer.start_span(
            request.method, kind=self._trace.SpanKind.CLIENT, attributes=attributes
        )
        request.extensions[_SPAN_KEY] = span
        # Propagate *this* span's context (overwriting any stale header from a
        # previous hop, so redirects and retries get correct parent IDs).
        self._propagate.inject(request.headers, context=self._trace.set_span_in_context(span))

    def on_response(self, response: Response) -> None:
        """Record the status and end the span."""
        request = response.request
        if request is None:
            return
        span = request.extensions.pop(_SPAN_KEY, None)
        if span is None:
            return
        status = response.status_code
        span.set_attribute("http.response.status_code", status)
        version = _HTTP_VERSIONS.get(response.http_version)
        if version is not None:
            span.set_attribute("network.protocol.version", version)
        if status >= 400:
            span.set_attribute("error.type", str(status))
            span.set_status(self._trace.Status(self._trace.StatusCode.ERROR))
        span.end()

    def on_error(self, request: Request, exc: BaseException) -> None:
        """Record a transport failure and end the span."""
        span = request.extensions.pop(_SPAN_KEY, None)
        if span is None:
            return
        span.set_attribute("error.type", type(exc).__qualname__)
        span.record_exception(exc)
        span.set_status(self._trace.Status(self._trace.StatusCode.ERROR, str(exc)))
        span.end()


def instrument(client: ClientT, *, tracer_provider: Any = None, tracer: Any = None) -> ClientT:
    """Add OpenTelemetry tracing to a :class:`~nextreq.Client` or
    :class:`~nextreq.AsyncClient` and return it.

    Raises:
        ImportError: ``opentelemetry-api`` is not installed.
    """
    hooks = OpenTelemetryHooks(tracer_provider=tracer_provider, tracer=tracer)
    client.add_request_hook(hooks.on_request)
    client.add_response_hook(hooks.on_response)
    client.add_error_hook(hooks.on_error)
    return client
