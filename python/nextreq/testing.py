"""Built-in test doubles: ``MockTransport`` plus in-memory ASGI and WSGI transports.

::

    mock = nextreq.MockTransport()
    mock.get("https://api.example.com/users/1").respond(json={"id": 1, "name": "Ada"})

    with nextreq.Client(transport=mock) as client:
        assert client.get("https://api.example.com/users/1").json()["name"] == "Ada"
    mock.assert_called_once()

No network, no monkeypatching, and the full client pipeline (redirects,
cookies, retries, hooks, typed parsing) still runs.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import io
import json as _json
import re
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Literal, Union
from urllib.parse import parse_qsl, unquote, unquote_to_bytes

from .exceptions import NextReqError
from .models import UPLOAD_CHUNK_SIZE, Headers, HeaderTypes, Request, Response, SSEEvent

if TYPE_CHECKING:
    from typing_extensions import Self

__all__ = [
    "ASGITransport",
    "MockCall",
    "MockRoute",
    "MockRouteNotFound",
    "MockTransport",
    "WSGITransport",
    "encode_sse",
]

MockHandler = Callable[[Request], Response | Awaitable[Response]]
URLPattern = Union[str, "re.Pattern[str]", Callable[[Request], bool], None]
SSEItem = Union[SSEEvent, str, Mapping[str, Any]]
_Reply = Union[Response, BaseException, type[BaseException], MockHandler]


class MockRouteNotFound(NextReqError, AssertionError):
    """A request reached a :class:`MockTransport` but matched none of its routes."""


def encode_sse(events: Iterable[SSEItem]) -> list[bytes]:
    """Encode events to ``text/event-stream`` chunks (one chunk per event).

    Items may be :class:`SSEEvent` objects, plain strings (the ``data``), or
    mappings (JSON-encoded into ``data``).
    """
    chunks: list[bytes] = []
    for item in events:
        if isinstance(item, SSEEvent):
            event = item
        elif isinstance(item, str):
            event = SSEEvent(data=item)
        else:
            event = SSEEvent(data=_json.dumps(dict(item), separators=(",", ":")))
        chunks.append(event.encode())
    return chunks


@dataclasses.dataclass
class MockCall:
    """One request seen by a :class:`MockTransport`."""

    request: Request
    response: Response | None = None
    exception: BaseException | None = None
    route: MockRoute | None = None


class _CallAssertions:
    """Call-recording helpers shared by the transport and its routes."""

    calls: list[MockCall]
    _label: str

    @property
    def called(self) -> bool:
        return bool(self.calls)

    @property
    def call_count(self) -> int:
        return len(self.calls)

    @property
    def last_request(self) -> Request | None:
        return self.calls[-1].request if self.calls else None

    def _describe_calls(self) -> str:
        if not self.calls:
            return "no calls"
        return ", ".join(f"{c.request.method} {c.request.url}" for c in self.calls)

    def assert_called(self) -> None:
        if not self.calls:
            raise AssertionError(f"Expected {self._label} to have been called, but it was not.")

    def assert_called_once(self) -> None:
        if len(self.calls) != 1:
            raise AssertionError(
                f"Expected {self._label} to have been called once, "
                f"but it was called {len(self.calls)} times ({self._describe_calls()})."
            )

    def assert_not_called(self) -> None:
        if self.calls:
            raise AssertionError(
                f"Expected {self._label} not to have been called, "
                f"but it was called {len(self.calls)} times ({self._describe_calls()})."
            )

    def assert_call_count(self, expected: int) -> None:
        if len(self.calls) != expected:
            raise AssertionError(
                f"Expected {self._label} to have been called {expected} times, "
                f"but it was called {len(self.calls)} times ({self._describe_calls()})."
            )


class MockRoute(_CallAssertions):
    """A request matcher plus the response(s) it produces."""

    def __init__(
        self,
        method: str | None,
        url: URLPattern,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        content: bytes | str | None = None,
    ) -> None:
        self.method = method.upper() if method else None
        self.url = url
        self._params = dict(params) if params else None
        self._headers = {k.lower(): v for k, v in headers.items()} if headers else None
        self._json = json
        self._content = content.encode("utf-8") if isinstance(content, str) else content
        self._replies: list[_Reply] = []
        self._cursor = 0
        self.calls: list[MockCall] = []
        self._label = f"route {self!r}"

    # -- matching ---------------------------------------------------------

    def matches(self, request: Request) -> bool:
        if self.method is not None and request.method != self.method:
            return False
        if not _match_url(self.url, request):
            return False
        if self._params is not None:
            query = dict(parse_qsl(request.url.query, keep_blank_values=True))
            if any(query.get(key) != value for key, value in self._params.items()):
                return False
        if self._headers is not None and any(
            request.headers.get(key) != value for key, value in self._headers.items()
        ):
            return False
        if self._content is not None and request.content != self._content:
            return False
        if self._json is not None:
            try:
                if _json.loads(request.content) != self._json:
                    return False
            except ValueError:
                return False
        return True

    # -- configuring replies ----------------------------------------------

    def respond(
        self,
        status_code: int = 200,
        *,
        json: Any = None,
        text: str | None = None,
        content: bytes | str | None = None,
        headers: HeaderTypes | None = None,
        sse: Iterable[SSEItem] | None = None,
        stream: Iterable[bytes] | None = None,
    ) -> Self:
        """Reply with a static response (rebuilt fresh for every call).

        ``sse=[...]`` replies with a ``text/event-stream`` body delivered one
        event per chunk, so ``iter_sse()`` / ``aiter_sse()`` can be exercised.
        """
        chunks: list[bytes] | None = None
        if sse is not None:
            chunks = encode_sse(sse)
        elif stream is not None:
            chunks = list(stream)

        def build(_request: Request) -> Response:
            merged = Headers(headers)
            if chunks is not None:
                if sse is not None and "content-type" not in merged:
                    merged["content-type"] = "text/event-stream"
                return Response(status_code, headers=merged, stream=iter(list(chunks)))
            return Response(status_code, headers=merged, json=json, text=text, content=content)

        self._replies = [build]
        self._cursor = 0
        return self

    def reply(self, handler: MockHandler) -> Self:
        """Reply dynamically: ``handler(request)`` returns the response.

        The handler may be a coroutine function when used with ``AsyncClient``.
        """
        self._replies = [handler]
        self._cursor = 0
        return self

    def raises(self, exception: BaseException | type[BaseException]) -> Self:
        """Raise ``exception`` instead of responding (e.g. ``ConnectError``)."""
        self._replies = [exception]
        self._cursor = 0
        return self

    def sequence(self, *replies: _Reply) -> Self:
        """Reply with each item in turn; the last one repeats.

        Items may be :class:`Response` objects, exceptions to raise, or
        handler callables - handy for testing retries::

            route.sequence(Response(503), Response(503), Response(200, json={"ok": True}))
        """
        if not replies:
            raise ValueError("sequence() needs at least one reply")
        self._replies = list(replies)
        self._cursor = 0
        return self

    def _next_reply(self) -> _Reply:
        if not self._replies:
            return _default_reply
        index = min(self._cursor, len(self._replies) - 1)
        self._cursor += 1
        return self._replies[index]

    def __repr__(self) -> str:
        url = self.url.pattern if isinstance(self.url, re.Pattern) else self.url
        return f"<MockRoute {self.method or 'ANY'} {url if url is not None else '*'}>"


def _default_reply(_request: Request) -> Response:
    return Response(200)


def _match_url(pattern: URLPattern, request: Request) -> bool:
    if pattern is None:
        return True
    if isinstance(pattern, re.Pattern):
        return pattern.search(str(request.url)) is not None
    if callable(pattern):
        return bool(pattern(request))
    url = request.url
    full = str(url)
    if pattern.startswith("/"):
        return pattern in (url.path, url.raw_path)
    without_query = full.split("?", 1)[0].split("#", 1)[0]
    candidates = {full, without_query, without_query.rstrip("/")}
    return pattern in candidates or pattern.rstrip("/") in candidates


def _clone_response(response: Response) -> Response:
    """Static ``Response`` objects are reused across calls, so hand out copies."""
    if response._content is None:
        return response  # a one-shot stream; cannot be replayed
    return Response(
        response.status_code,
        headers=response.headers,
        content=response._content,
        http_version=response.http_version,
    )


class MockTransport(_CallAssertions):
    # A streamed request body is read into ``request.content`` before matching,
    # so that matchers, handlers and ``last_request`` can look at it.
    """An in-memory transport for tests.

    Register routes with :meth:`route` (or the ``get`` / ``post`` / ...
    shortcuts) and configure each with ``respond`` / ``reply`` / ``raises`` /
    ``sequence``. Routes are matched in registration order. A request matching
    no route falls back to ``handler`` if one was given, otherwise raises
    :class:`MockRouteNotFound`.

    Works with both ``Client(transport=mock)`` and ``AsyncClient(transport=mock)``.
    """

    def __init__(self, handler: MockHandler | None = None) -> None:
        self._handler = handler
        self.routes: list[MockRoute] = []
        self.calls: list[MockCall] = []
        self._label = "the mock transport"

    # -- routing ----------------------------------------------------------

    def route(
        self,
        method: str | None = None,
        url: URLPattern = None,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        content: bytes | str | None = None,
    ) -> MockRoute:
        """Add a route.

        Args:
            method: HTTP method, or ``None`` for any.
            url: an absolute URL (query string optional), a path starting with
                ``/``, a compiled regex searched against the full URL, a
                predicate ``(request) -> bool``, or ``None`` for any URL.
            params: query parameters that must be present with these values.
            headers: request headers that must be present with these values.
            json: the request body must be JSON equal to this value.
            content: the request body must equal these bytes.
        """
        route = MockRoute(method, url, params=params, headers=headers, json=json, content=content)
        self.routes.append(route)
        return route

    def get(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("GET", url, **matchers)

    def post(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("POST", url, **matchers)

    def put(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("PUT", url, **matchers)

    def patch(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("PATCH", url, **matchers)

    def delete(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("DELETE", url, **matchers)

    def head(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("HEAD", url, **matchers)

    def options(self, url: URLPattern = None, **matchers: Any) -> MockRoute:
        return self.route("OPTIONS", url, **matchers)

    def reset(self) -> None:
        """Forget recorded calls and rewind every route's reply sequence."""
        self.calls.clear()
        for route in self.routes:
            route.calls.clear()
            route._cursor = 0

    def assert_all_called(self) -> None:
        """Assert that every registered route was hit at least once."""
        unused = [route for route in self.routes if not route.called]
        if unused:
            raise AssertionError(f"These mock routes were never called: {unused!r}")

    # -- dispatch ---------------------------------------------------------

    def _resolve(self, request: Request) -> tuple[MockCall, _Reply]:
        call = MockCall(request)
        self.calls.append(call)
        for route in self.routes:
            if route.matches(request):
                call.route = route
                route.calls.append(call)
                return call, route._next_reply()
        if self._handler is not None:
            return call, self._handler
        error = MockRouteNotFound(
            f"No mock route matched {request.method} {request.url}. "
            f"Registered routes: {self.routes!r}",
            request=request,
        )
        call.exception = error
        raise error

    @staticmethod
    def _finish(call: MockCall, result: Any) -> Response:
        if not isinstance(result, Response):
            raise TypeError(
                f"Mock handlers must return a nextreq.Response, got {type(result).__name__}"
            )
        response = _clone_response(result)
        call.response = response
        return response

    @staticmethod
    def _as_exception(reply: _Reply) -> BaseException | None:
        if isinstance(reply, BaseException):
            return reply
        if isinstance(reply, type) and issubclass(reply, BaseException):
            return reply("mocked error")
        return None

    def handle_request(self, request: Request) -> Response:
        call, reply = self._resolve(request)
        error = self._as_exception(reply)
        if error is not None:
            call.exception = error
            raise error
        if isinstance(reply, Response):
            return self._finish(call, reply)
        try:
            result = reply(request)  # type: ignore[operator]
        except BaseException as exc:
            call.exception = exc
            raise
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise TypeError(
                "This mock handler is asynchronous; use it with AsyncClient, "
                "or make the handler a plain function."
            )
        return self._finish(call, result)

    async def handle_async_request(self, request: Request) -> Response:
        call, reply = self._resolve(request)
        error = self._as_exception(reply)
        if error is not None:
            call.exception = error
            raise error
        if isinstance(reply, Response):
            return self._finish(call, reply)
        try:
            result = reply(request)  # type: ignore[operator]
            if inspect.isawaitable(result):
                result = await result
        except BaseException as exc:
            call.exception = exc
            raise
        return self._finish(call, result)

    def close(self) -> None:
        """Nothing to release; present for transport symmetry."""

    async def aclose(self) -> None:
        """Nothing to release; present for transport symmetry."""


# -- WSGI ---------------------------------------------------------------------


class WSGITransport:
    """Call a WSGI application (Flask, Django, ...) in-process.

    ::

        client = nextreq.Client(app=flask_app)          # shorthand
        client = nextreq.Client(transport=WSGITransport(flask_app))

    With ``client.stream(...)`` the application's response is consumed as it
    is produced, so streaming and Server-Sent Events endpoints can be tested
    chunk by chunk; closing the response closes the application's iterable.

    A streamed request body (a generator or an open file) reaches the
    application as a stream too: ``wsgi.input`` pulls it chunk by chunk.

    With ``AsyncClient`` the application runs in a worker thread of its own
    for as long as the response is open, and both bodies are still streamed.
    """

    #: Streamed request bodies are passed on as a stream (see ``Transport``).
    streams_requests = True

    def __init__(
        self,
        app: Callable[..., Any],
        *,
        raise_app_exceptions: bool = True,
        script_name: str = "",
        remote_addr: str = "127.0.0.1",
    ) -> None:
        if _is_asgi_app(app):
            raise TypeError(
                "This looks like an ASGI application. Use `AsyncClient(app=...)` "
                "(or `ASGITransport`) for ASGI apps; `Client(app=...)` is for WSGI."
            )
        self.app = app
        self.raise_app_exceptions = raise_app_exceptions
        self.script_name = script_name
        self.remote_addr = remote_addr

    def _environ(self, request: Request, body: Iterator[bytes] | None) -> dict[str, Any]:
        url = request.url
        scheme = url.scheme or "http"
        port = url.effective_port or (443 if scheme == "https" else 80)
        environ: dict[str, Any] = {
            "REQUEST_METHOD": request.method,
            "SCRIPT_NAME": self.script_name,
            "PATH_INFO": unquote_to_bytes(url.path).decode("latin-1"),
            "QUERY_STRING": url.query,
            "SERVER_NAME": url.host or "testserver",
            "SERVER_PORT": str(port),
            "SERVER_PROTOCOL": "HTTP/1.1",
            "REMOTE_ADDR": self.remote_addr,
            "wsgi.version": (1, 0),
            "wsgi.url_scheme": scheme,
            "wsgi.input": (
                io.BytesIO(request.content)
                if body is None
                else io.BufferedReader(_ChunkReader(body), UPLOAD_CHUNK_SIZE)
            ),
            "wsgi.errors": sys.stderr,
            "wsgi.multithread": True,
            "wsgi.multiprocess": False,
            "wsgi.run_once": False,
        }
        if request.content:
            environ["CONTENT_LENGTH"] = str(len(request.content))
        for name in request.headers:
            value = request.headers[name]
            if name == "content-type":
                environ["CONTENT_TYPE"] = value
            elif name == "content-length":
                environ["CONTENT_LENGTH"] = value
            else:
                environ["HTTP_" + name.upper().replace("-", "_")] = value
        environ.setdefault("HTTP_HOST", url.netloc or "testserver")
        if body is not None and "CONTENT_LENGTH" not in environ:
            # A body of unknown length ends where the stream ends (the flag
            # servers set after decoding chunked transfer encoding).
            environ["wsgi.input_terminated"] = True
        return environ

    def handle_request(self, request: Request) -> Response:
        stream = request.stream
        started = self._start(request, None if stream is None else stream.open())
        if started is None:
            return Response(500, text="Internal Server Error", request=request)
        status_code, headers, body = started
        return Response(status_code, headers=headers, stream=body, request=request)

    def _start(
        self, request: Request, request_body: Iterator[bytes] | None
    ) -> tuple[int, list[tuple[str, str]], _WSGIBody] | None:
        """Call the application up to its first chunk; ``None`` for a hidden failure."""
        upload_errors: list[Exception] = []
        if request_body is not None:
            request_body = _recording(request_body, upload_errors)
        environ = self._environ(request, request_body)
        captured: dict[str, Any] = {}
        written: list[bytes] = []

        def start_response(
            status: str,
            headers: list[tuple[str, str]],
            exc_info: Any = None,
        ) -> Callable[[bytes], Any]:
            if exc_info is not None and self.raise_app_exceptions:
                raise exc_info[1].with_traceback(exc_info[2])
            captured["status"] = status
            captured["headers"] = headers
            return written.append

        result: Any = None
        try:
            result = self.app(environ, start_response)
            chunks = iter(result)
            # An application may call start_response() as late as its first
            # chunk, so produce that one before reading the status.
            first = next(chunks, None)
        except Exception:
            _close_quietly(result)
            if upload_errors:
                # The body could not be produced. A real client would drop the
                # connection here, and the caller gets this error.
                raise upload_errors[0] from None
            if self.raise_app_exceptions:
                raise
            return None
        if upload_errors:
            _close_quietly(result)
            raise upload_errors[0]
        if "status" not in captured:
            _close_quietly(result)
            raise RuntimeError("The WSGI application did not call start_response().")
        status_code = int(str(captured["status"]).split(" ", 1)[0])
        # Legacy write() output comes first, then the iterable.
        ready = [*written, first] if first is not None else list(written)
        body = _WSGIBody(result, chunks, ready, written, self.raise_app_exceptions)
        return status_code, captured["headers"], body

    async def handle_async_request(self, request: Request) -> Response:
        """Run the (blocking) WSGI app in a worker thread of its own.

        The thread lives as long as the response is open: the application is
        called there, and each response chunk is produced there when the
        client asks for it. The request body is produced on the event loop
        and handed over chunk by chunk.
        """
        loop = asyncio.get_running_loop()
        worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nextreq-wsgi")
        stream = request.stream
        request_body = None if stream is None else _iterate_from_thread(stream.aopen(), loop)
        try:
            started = await loop.run_in_executor(worker, self._start, request, request_body)
        except BaseException:
            worker.shutdown(wait=False)
            raise
        if started is None:
            worker.shutdown(wait=False)
            return Response(500, text="Internal Server Error", request=request)
        status_code, headers, body = started
        return Response(
            status_code, headers=headers, stream=_ThreadedBody(body, worker), request=request
        )

    def close(self) -> None:
        """Nothing to release."""


def _recording(chunks: Iterator[bytes], errors: list[Exception]) -> Iterator[bytes]:
    """Pass ``chunks`` through, noting the exception that ends them (if any)."""
    try:
        yield from chunks
    except Exception as exc:
        errors.append(exc)
        raise


class _ChunkReader(io.RawIOBase):
    """``wsgi.input`` for a streamed request body: pulls chunks as they are read."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._pending = b""

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        while not self._pending:
            try:
                self._pending = next(self._chunks)
            except StopIteration:
                return 0
        size = min(len(buffer), len(self._pending))
        buffer[:size] = self._pending[:size]
        self._pending = self._pending[size:]
        return size


def _iterate_from_thread(
    chunks: AsyncIterator[bytes], loop: asyncio.AbstractEventLoop
) -> Iterator[bytes]:
    """Iterate, from a worker thread, over chunks produced on the event loop."""

    async def pull() -> bytes | None:
        try:
            return await chunks.__anext__()
        except StopAsyncIteration:
            return None

    while True:
        chunk = asyncio.run_coroutine_threadsafe(pull(), loop).result()
        if chunk is None:
            return
        yield chunk


class _ThreadedBody:
    """A WSGI response read from the event loop.

    Every step runs on the one worker thread that called the application, so
    the application sees a single thread for the whole request.
    """

    def __init__(self, body: _WSGIBody, worker: ThreadPoolExecutor) -> None:
        self._body = body
        self._chunks = iter(body)
        self._worker = worker
        self._done = False

    def __aiter__(self) -> _ThreadedBody:
        return self

    async def __anext__(self) -> bytes:
        if self._done:
            raise StopAsyncIteration
        loop = asyncio.get_running_loop()
        try:
            chunk = await loop.run_in_executor(self._worker, next, self._chunks, None)
        except BaseException:
            await self.aclose()
            raise
        if chunk is None:
            self._done = True
            self._worker.shutdown(wait=False)
            raise StopAsyncIteration
        return chunk

    def _close(self) -> None:
        self._chunks.close()  # type: ignore[attr-defined]
        self._body.close()

    async def aclose(self) -> None:
        if self._done:
            return
        self._done = True
        loop = asyncio.get_running_loop()
        try:
            # Queued behind a chunk that is still being produced, if any.
            await loop.run_in_executor(self._worker, self._close)
        finally:
            self._worker.shutdown(wait=False)


def _close_quietly(result: Any) -> None:
    closer = getattr(result, "close", None)
    if callable(closer):
        with contextlib.suppress(Exception):
            closer()


class _WSGIBody:
    """The body of a WSGI response, produced as the application yields it."""

    def __init__(
        self,
        result: Any,
        chunks: Iterator[bytes],
        ready: list[bytes],
        written: list[bytes],
        raise_app_exceptions: bool,
    ) -> None:
        self._result = result
        self._chunks = chunks
        self._ready = ready
        self._written = written
        self._seen_writes = len(written)
        self._raise = raise_app_exceptions
        self._closed = False

    def __iter__(self) -> Iterator[bytes]:
        try:
            for chunk in self._ready:
                if chunk:
                    yield chunk
            self._ready = []
            while True:
                try:
                    chunk = next(self._chunks)
                except StopIteration:
                    break
                except Exception:
                    if self._raise:
                        raise
                    break  # the response is cut short, as a real server would do
                # write() calls made while producing this chunk precede it.
                late = self._written[self._seen_writes :]
                self._seen_writes = len(self._written)
                for piece in (*late, chunk):
                    if piece:
                        yield piece
        finally:
            self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _close_quietly(self._result)


# -- ASGI ---------------------------------------------------------------------


def _is_asgi_app(app: Any) -> bool:
    if inspect.iscoroutinefunction(app):
        return True
    call = getattr(app, "__call__", None)  # noqa: B004
    return call is not None and inspect.iscoroutinefunction(call)


#: Response chunks an ASGI application may produce ahead of the client.
_ASGI_QUEUE_CHUNKS = 8
#: How long a closed response waits for the application to notice the
#: disconnect before its task is cancelled.
_ASGI_DISCONNECT_GRACE = 0.2


class ASGITransport:
    """Call an ASGI 3.0 application (FastAPI, Starlette, ...) in-process.

    ::

        async with nextreq.AsyncClient(app=fastapi_app) as client:   # shorthand
            ...
        client = nextreq.AsyncClient(transport=ASGITransport(fastapi_app))

    **Streaming.** With ``client.stream(...)`` the response is delivered as the
    application sends it, with backpressure, so streaming and Server-Sent
    Events endpoints can be tested chunk by chunk. Closing the response tells
    the application that the client disconnected. A streamed request body (a
    generator or an open file) is streamed too: the application receives one
    ``http.request`` message per chunk, as it asks for them.

    **Lifespan.** The application's startup handlers run before the first
    request and its shutdown handlers when the client is closed, so use the
    client as a context manager. ``lifespan`` selects the behaviour:

    * ``"auto"`` (default): run lifespan events if the application supports them;
    * ``True``: run them and raise if the application does not support them;
    * ``False``: never run them.
    """

    #: Streamed request bodies are passed on as a stream (see ``AsyncTransport``).
    streams_requests = True

    def __init__(
        self,
        app: Callable[..., Any],
        *,
        raise_app_exceptions: bool = True,
        root_path: str = "",
        client: tuple[str, int] = ("127.0.0.1", 50000),
        lifespan: bool | Literal["auto"] = "auto",
    ) -> None:
        if lifespan not in (True, False, "auto"):
            raise ValueError("lifespan must be True, False or 'auto'")
        self.app = app
        self.raise_app_exceptions = raise_app_exceptions
        self.root_path = root_path
        self.client = client
        self.lifespan = lifespan
        #: State shared by the application's lifespan and its requests.
        self.state: dict[str, Any] = {}
        self._lifespan: _Lifespan | None = None

    def _scope(self, request: Request) -> dict[str, Any]:
        url = request.url
        scheme = url.scheme or "http"
        port = url.effective_port or (443 if scheme == "https" else 80)
        headers = [
            (name.encode("ascii"), _encode_header_value(value))
            for name, value in request.headers.multi_items()
        ]
        if "host" not in request.headers:
            headers.insert(0, (b"host", (url.netloc or "testserver").encode("ascii")))
        return {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": request.method,
            "scheme": scheme,
            "path": unquote(url.path),
            "raw_path": url.path.encode("ascii", "ignore"),
            "query_string": url.query.encode("ascii", "ignore"),
            "root_path": self.root_path,
            "headers": headers,
            "server": (url.host or "testserver", port),
            "client": self.client,
            "extensions": {},
            "state": self.state.copy(),
        }

    async def _ensure_started(self) -> None:
        """Run the application's startup handlers once per event loop."""
        if self.lifespan is False:
            return
        loop = asyncio.get_running_loop()
        current = self._lifespan
        if current is not None and current.loop is loop:
            return
        # First request, or the client moved to a new event loop (the old
        # lifespan task died with its loop).
        self._lifespan = lifespan = _Lifespan(self.app, self.state, loop)
        await lifespan.startup(required=self.lifespan is True)

    async def handle_async_request(self, request: Request) -> Response:
        await self._ensure_started()
        scope = self._scope(request)
        loop = asyncio.get_running_loop()
        chunks: asyncio.Queue[bytes | BaseException | None] = asyncio.Queue(_ASGI_QUEUE_CHUNKS)
        disconnected = asyncio.Event()
        started: asyncio.Future[None] = loop.create_future()
        head: dict[str, Any] = {}
        request_delivered = False
        finished = False
        upload = None if request.stream is None else request.stream.aopen()
        upload_errors: list[Exception] = []

        async def receive() -> dict[str, Any]:
            nonlocal request_delivered
            if not request_delivered:
                if upload is None:
                    request_delivered = True
                    return {"type": "http.request", "body": request.content, "more_body": False}
                try:
                    chunk = await upload.__anext__()
                except StopAsyncIteration:
                    request_delivered = True
                    return {"type": "http.request", "body": b"", "more_body": False}
                except Exception as exc:
                    # The body could not be produced. A real client would drop
                    # the connection here, and the caller gets this error.
                    request_delivered = True
                    upload_errors.append(exc)
                    disconnected.set()
                    return {"type": "http.disconnect"}
                return {"type": "http.request", "body": chunk, "more_body": True}
            await disconnected.wait()
            return {"type": "http.disconnect"}

        async def send(message: Mapping[str, Any]) -> None:
            nonlocal finished
            kind = message["type"]
            if kind == "http.response.start":
                head["status"] = int(message["status"])
                head["headers"] = [
                    (bytes(name).decode("latin-1").lower(), bytes(value).decode("latin-1"))
                    for name, value in message.get("headers", [])
                ]
                if not started.done():
                    started.set_result(None)
            elif kind == "http.response.body" and not finished:
                if disconnected.is_set():
                    # Nobody is reading any more. Still give the event loop a
                    # turn, so an application that keeps sending can be told
                    # about the disconnect (or cancelled) instead of spinning.
                    await asyncio.sleep(0)
                    return
                chunk = message.get("body", b"")
                if chunk and request.method != "HEAD":
                    await chunks.put(bytes(chunk))  # waits while the client is behind
                if not message.get("more_body", False):
                    finished = True
                    await chunks.put(None)

        async def run_app() -> None:
            try:
                await self.app(scope, receive, send)
            except Exception as exc:
                if not started.done():
                    started.set_exception(exc)
                elif not finished:
                    await chunks.put(exc)
                else:
                    raise  # after a complete response: reported when the body ends
            else:
                if not started.done():
                    started.set_exception(
                        RuntimeError("The ASGI application returned without starting a response.")
                    )
                elif not finished:
                    await chunks.put(None)  # the app ended the response by returning
            finally:
                if upload is not None:
                    with contextlib.suppress(Exception):
                        await upload.aclose()

        task = loop.create_task(run_app())
        try:
            await started
        except RuntimeError:
            if upload_errors:
                raise upload_errors[0] from None
            raise
        except Exception:
            if upload_errors:
                raise upload_errors[0] from None
            if self.raise_app_exceptions:
                raise
            return Response(500, text="Internal Server Error", request=request)
        except BaseException:
            task.cancel()
            raise
        if upload_errors:
            task.cancel()
            raise upload_errors[0]
        body = _ASGIBody(task, chunks, disconnected, self.raise_app_exceptions)
        return Response(head["status"], headers=head["headers"], stream=body, request=request)

    async def aclose(self) -> None:
        """Run the application's shutdown handlers (if startup ran)."""
        lifespan, self._lifespan = self._lifespan, None
        if lifespan is not None:
            await lifespan.shutdown()


class _ASGIBody:
    """The body of an ASGI response, delivered as the application sends it."""

    def __init__(
        self,
        task: asyncio.Task[None],
        chunks: asyncio.Queue[bytes | BaseException | None],
        disconnected: asyncio.Event,
        raise_app_exceptions: bool,
    ) -> None:
        self._task = task
        self._chunks = chunks
        self._disconnected = disconnected
        self._raise = raise_app_exceptions
        self._done = False

    def __aiter__(self) -> _ASGIBody:
        return self

    async def __anext__(self) -> bytes:
        if self._done:
            raise StopAsyncIteration
        item = await self._chunks.get()
        if isinstance(item, bytes):
            return item
        self._done = True
        self._disconnected.set()
        if isinstance(item, BaseException):
            if self._raise:
                raise item
            raise StopAsyncIteration
        # The response is complete. Let the application finish (it may still
        # run background work), so its side effects are visible afterwards.
        try:
            await self._task
        except Exception:
            if self._raise:
                raise
        raise StopAsyncIteration

    async def aclose(self) -> None:
        """The client is no longer interested: tell the application."""
        if self._done:
            return
        self._done = True
        self._disconnected.set()
        while not self._chunks.empty():  # make room for a send() that is waiting
            self._chunks.get_nowait()
        if not self._task.done():
            done, _ = await asyncio.wait({self._task}, timeout=_ASGI_DISCONNECT_GRACE)
            if not done:
                self._task.cancel()
        with contextlib.suppress(BaseException):
            await self._task


class _Lifespan:
    """Drives the ASGI lifespan protocol for one application on one loop."""

    def __init__(
        self, app: Callable[..., Any], state: dict[str, Any], loop: asyncio.AbstractEventLoop
    ) -> None:
        self.app = app
        self.state = state
        self.loop = loop
        self._events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._started = asyncio.Event()
        self._stopped = asyncio.Event()
        self._failure: str | None = None
        self._error: BaseException | None = None
        self._task: asyncio.Task[None] | None = None
        self._active = False

    async def _run(self) -> None:
        async def receive() -> dict[str, Any]:
            return await self._events.get()

        async def send(message: Mapping[str, Any]) -> None:
            kind = message["type"]
            if kind == "lifespan.startup.complete":
                self._started.set()
            elif kind == "lifespan.startup.failed":
                self._failure = str(message.get("message", "")) or "startup failed"
                self._started.set()
            elif kind == "lifespan.shutdown.complete":
                self._stopped.set()
            elif kind == "lifespan.shutdown.failed":
                self._failure = str(message.get("message", "")) or "shutdown failed"
                self._stopped.set()

        scope = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": self.state,
        }
        try:
            await self.app(scope, receive, send)
        except BaseException as exc:
            self._error = exc
        finally:
            self._started.set()
            self._stopped.set()

    async def startup(self, *, required: bool) -> None:
        self._task = self.loop.create_task(self._run())
        await self._events.put({"type": "lifespan.startup"})
        await self._started.wait()
        if self._failure is not None:
            raise RuntimeError(f"ASGI lifespan startup failed: {self._failure}")
        if self._task.done():
            # The application returned or raised instead of running the
            # lifespan protocol: it does not support it.
            if required:
                raise RuntimeError(
                    "The ASGI application does not support the lifespan protocol."
                ) from self._error
            return
        self._active = True

    async def shutdown(self) -> None:
        task = self._task
        if not self._active or task is None or task.done():
            return
        if self.loop is not asyncio.get_running_loop():
            return  # its event loop is gone; nothing can be run on it
        self._active = False
        self._failure = None
        await self._events.put({"type": "lifespan.shutdown"})
        await self._stopped.wait()
        with contextlib.suppress(BaseException):
            await task
        if self._failure is not None:
            raise RuntimeError(f"ASGI lifespan shutdown failed: {self._failure}")


def _encode_header_value(value: str) -> bytes:
    try:
        return value.encode("latin-1")
    except UnicodeEncodeError:
        return value.encode("utf-8")
