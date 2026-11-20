"""``Client``, ``AsyncClient``, the module-level request helpers and ``gather()``.

The Rust engine (``_nextreq.HttpClient``) performs exactly one HTTP exchange per
call. Everything with HTTP *semantics* lives here so it behaves identically
for the real network and for the mock / ASGI / WSGI transports: redirects,
the cookie jar, auth, retries with ``Retry-After``, hooks and typed parsing.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import inspect
import os
import threading
import time
from collections.abc import AsyncIterator, Callable, Generator, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, TypedDict, TypeVar, Union, overload

from . import _nextreq
from ._nextreq import HttpClient, RetryPolicy, Timeout, WebSocketConnection
from .exceptions import (
    ClientClosed,
    DecodingError,
    LocalProtocolError,
    NextReqError,
    ReadTimeout,
    TooManyRedirects,
    TransportError,
    UnsupportedProtocol,
    WebSocketDisconnect,
    WebSocketError,
)
from .models import (
    UNSET,
    URL,
    Cookies,
    CookieTypes,
    Headers,
    HeaderTypes,
    QueryParamTypes,
    Request,
    RequestContent,
    RequestData,
    RequestFiles,
    Response,
    TimeoutTypes,
    URLTypes,
    _Unset,
    coerce_timeout,
    encode_body,
    encode_cookie_pairs,
    json_dumps,
    merge_query,
)
from .parsing import parse_response

if TYPE_CHECKING:
    from typing_extensions import Self, Unpack

__all__ = [
    "AsyncClient",
    "AsyncTransport",
    "Auth",
    "BasicAuth",
    "BearerAuth",
    "Client",
    "Transport",
    "WebSocket",
    "delete",
    "gather",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "request",
    "stream",
]

T = TypeVar("T")

USER_AGENT: Final = f"nextreq/{_nextreq.__version__}"
DEFAULT_MAX_REDIRECTS: Final = 10
_REDIRECT_STATUSES: Final = frozenset({301, 302, 303, 307, 308})
_BODY_HEADERS: Final = ("content-length", "content-type", "content-encoding", "transfer-encoding")
_NON_RETRYABLE: Final = (
    TooManyRedirects,
    UnsupportedProtocol,
    LocalProtocolError,
    DecodingError,
    WebSocketError,
)

# -- types --------------------------------------------------------------------

RequestHook = Callable[[Request], Any]
ResponseHook = Callable[[Response], Any]
ErrorHook = Callable[[Request, BaseException], Any]
AuthTypes = Union[tuple[str, str], Callable[[Request], Any], None]
RetryTypes = Union[int, RetryPolicy, None]
ProxyTypes = Union[str, Mapping[str, str], None]
CertTypes = Union[str, "os.PathLike[str]", tuple[str, str], None]
VerifyTypes = Union[bool, str, "os.PathLike[str]"]
GatherItem = Union[Request, str, URL, tuple[str, str], tuple[str, str, Mapping[str, Any]]]


class RequestOptions(TypedDict, total=False):
    """Per-request keyword arguments accepted by every request method."""

    params: QueryParamTypes | None
    headers: HeaderTypes | None
    cookies: CookieTypes | None
    content: RequestContent | None
    data: RequestData | None
    json: Any
    files: RequestFiles | None
    auth: AuthTypes | _Unset
    timeout: TimeoutTypes | _Unset
    follow_redirects: bool | None
    raise_for_status: bool | None
    retries: RetryTypes | _Unset
    extensions: dict[str, Any] | None


class Transport(Protocol):
    """A synchronous transport: turns a :class:`Request` into a :class:`Response`."""

    def handle_request(self, request: Request) -> Response: ...


class AsyncTransport(Protocol):
    """An asynchronous transport."""

    async def handle_async_request(self, request: Request) -> Response: ...


# -- auth ---------------------------------------------------------------------


class Auth:
    """Base class for request authentication. Subclasses mutate the request."""

    def __call__(self, request: Request) -> Request | None:
        raise NotImplementedError


class BasicAuth(Auth):
    """HTTP Basic authentication."""

    def __init__(self, username: str | bytes, password: str | bytes) -> None:
        user = username.encode("utf-8") if isinstance(username, str) else username
        secret = password.encode("utf-8") if isinstance(password, str) else password
        self._header = "Basic " + base64.b64encode(user + b":" + secret).decode("ascii")

    def __call__(self, request: Request) -> Request:
        request.headers["authorization"] = self._header
        return request


class BearerAuth(Auth):
    """``Authorization: Bearer <token>``."""

    def __init__(self, token: str) -> None:
        self._header = f"Bearer {token}"

    def __call__(self, request: Request) -> Request:
        request.headers["authorization"] = self._header
        return request


def _coerce_auth(auth: AuthTypes) -> Callable[[Request], Any] | None:
    if auth is None:
        return None
    if isinstance(auth, tuple):
        return BasicAuth(auth[0], auth[1])
    if callable(auth):
        return auth
    raise TypeError(
        f"auth must be a (username, password) tuple or a callable, not {type(auth).__name__}"
    )


def _coerce_retry(retries: RetryTypes) -> RetryPolicy | None:
    if retries is None:
        return None
    if isinstance(retries, RetryPolicy):
        return retries if retries.max_attempts > 0 else None
    if isinstance(retries, bool) or not isinstance(retries, int):
        raise TypeError(f"retries must be an int or RetryPolicy, not {type(retries).__name__}")
    if retries < 0:
        raise ValueError("retries must be >= 0")
    return RetryPolicy(max_attempts=retries) if retries > 0 else None


def _as_hook_list(hooks: Any) -> list[Any]:
    if hooks is None:
        return []
    if callable(hooks):
        return [hooks]
    return list(hooks)


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


async def _asleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


# -- engine configuration -----------------------------------------------------

_PROXY_SCOPES: Final = {
    "all": "all",
    "all://": "all",
    "http": "http",
    "http://": "http",
    "https": "https",
    "https://": "https",
}


def _engine_kwargs(
    *,
    verify: VerifyTypes,
    cert: CertTypes,
    proxy: ProxyTypes,
    trust_env: bool,
    http1: bool,
    http2: bool,
    http3: bool,
    max_connections: int | None,
    max_keepalive_connections: int | None,
    keepalive_expiry: float | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "trust_env": trust_env,
        "http1": http1,
        "http2": http2,
        "http3": http3,
        "max_connections": max_connections,
        "max_keepalive_connections": max_keepalive_connections,
        "keepalive_expiry": keepalive_expiry,
    }
    if isinstance(verify, bool):
        kwargs["verify"] = verify
    else:
        kwargs["verify"] = True
        kwargs["ca_bundle"] = os.fspath(verify)
    if cert is not None:
        if isinstance(cert, tuple):
            kwargs["cert"], kwargs["key"] = os.fspath(cert[0]), os.fspath(cert[1])
        else:
            kwargs["cert"] = os.fspath(cert)
    if proxy:
        if isinstance(proxy, str):
            kwargs["proxies"] = [("all", proxy)]
        else:
            proxies = []
            for scope, url in proxy.items():
                try:
                    proxies.append((_PROXY_SCOPES[scope.lower()], url))
                except KeyError:
                    raise ValueError(
                        f"unknown proxy scope {scope!r}; use 'http://', 'https://' or 'all://'"
                    ) from None
            kwargs["proxies"] = proxies
    return kwargs


_DEFAULT_ENGINE_LOCK = threading.Lock()
_default_engine: HttpClient | None = None


def _shared_engine() -> HttpClient:
    """Process-wide engine used by the module-level helpers (keeps connections warm)."""
    global _default_engine
    engine = _default_engine
    if engine is None or engine.is_closed:
        with _DEFAULT_ENGINE_LOCK:
            engine = _default_engine
            if engine is None or engine.is_closed:
                engine = _default_engine = HttpClient()
    return engine


# -- shared client logic ------------------------------------------------------


class _BaseClient:
    """Configuration and transport-independent logic shared by both clients."""

    def __init__(
        self,
        *,
        base_url: URLTypes,
        headers: HeaderTypes | None,
        params: QueryParamTypes | None,
        cookies: CookieTypes | None,
        auth: AuthTypes,
        timeout: TimeoutTypes | _Unset,
        follow_redirects: bool,
        max_redirects: int,
        raise_for_status: bool,
        retries: RetryTypes,
        on_request: Any,
        on_response: Any,
        on_error: Any,
    ) -> None:
        self._base_url = str(base_url).rstrip("/")
        self._headers = Headers(
            {
                "accept": "*/*",
                "accept-encoding": "gzip, deflate, br, zstd",
                "user-agent": USER_AGENT,
            }
        )
        if headers:
            for key, value in Headers(headers).multi_items():
                self._headers[key] = value
        self._params = params
        self._cookies = Cookies(cookies)
        self._auth = _coerce_auth(auth)
        self._timeout: Timeout | None = (
            Timeout() if isinstance(timeout, _Unset) else coerce_timeout(timeout)
        )
        self._follow_redirects = follow_redirects
        self._max_redirects = max_redirects
        self._raise_for_status = raise_for_status
        self._retry = _coerce_retry(retries)
        self._on_request: list[Any] = _as_hook_list(on_request)
        self._on_response: list[Any] = _as_hook_list(on_response)
        self._on_error: list[Any] = _as_hook_list(on_error)
        self._raw: HttpClient | None = None
        self._owns_raw = True
        self._closed = False

    # -- configuration accessors -----------------------------------------

    @property
    def base_url(self) -> URL:
        return URL(self._base_url)

    @property
    def headers(self) -> Headers:
        """Default headers sent with every request (mutable)."""
        return self._headers

    @property
    def cookies(self) -> Cookies:
        """The persistent cookie jar (mutable)."""
        return self._cookies

    @property
    def timeout(self) -> Timeout | None:
        return self._timeout

    @timeout.setter
    def timeout(self, value: TimeoutTypes) -> None:
        self._timeout = coerce_timeout(value)

    @property
    def retries(self) -> RetryPolicy | None:
        return self._retry

    @property
    def is_closed(self) -> bool:
        return self._closed

    def add_request_hook(self, hook: RequestHook) -> None:
        self._on_request.append(hook)

    def add_response_hook(self, hook: ResponseHook) -> None:
        self._on_response.append(hook)

    def add_error_hook(self, hook: ErrorHook) -> None:
        self._on_error.append(hook)

    # -- request construction --------------------------------------------

    def _merge_url(self, url: URLTypes) -> str:
        target = url if isinstance(url, str) else str(url)
        if target.startswith(("http://", "https://")) or "://" in target.split("?", 1)[0]:
            return target
        base = self._base_url
        if not base:
            return target
        if not target:
            return base
        return f"{base}/{target.lstrip('/')}"

    def build_request(
        self,
        method: str,
        url: URLTypes,
        *,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        cookies: CookieTypes | None = None,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        json: Any = None,
        files: RequestFiles | None = None,
        auth: AuthTypes | _Unset = UNSET,
        timeout: TimeoutTypes | _Unset = UNSET,
        extensions: dict[str, Any] | None = None,
    ) -> Request:
        """Build a :class:`Request` with this client's defaults applied."""
        target = self._merge_url(url)
        if self._params:
            target = merge_query(target, self._params)
        if params:
            target = merge_query(target, params)

        header_list = list(self._headers._list)
        if headers:
            extra = Headers(headers)._list
            overridden = {key for key, _ in extra}
            header_list = [item for item in header_list if item[0] not in overridden]
            header_list.extend(extra)
        merged = Headers._from_raw(header_list)

        body = b""
        if content is not None or data is not None or json is not None or files:
            body, content_type = encode_body(content, data, json, files)
            if content_type is not None and "content-type" not in merged:
                header_list.append(("content-type", content_type))
            if body and "content-length" not in merged:
                header_list.append(("content-length", str(len(body))))
            merged._map = None

        cookie_base: str | None = None
        if cookies or (headers and "cookie" in merged):
            explicit = merged.get("cookie")
            if explicit is not None:
                del merged["cookie"]
            pairs = encode_cookie_pairs(cookies)
            cookie_base = "; ".join(part for part in (explicit, pairs) if part) or None

        target_url = URL(target)
        if "@" in target and target_url.username:
            if "authorization" not in merged:
                BasicAuth(target_url.username, target_url.password)(
                    Request._build("GET", target_url, merged, b"", None, None)
                )
            target_url = target_url.without_userinfo()

        request = Request._build(
            method.upper(),
            target_url,
            merged,
            body,
            self._timeout if isinstance(timeout, _Unset) else coerce_timeout(timeout),
            cookie_base,
        )
        if extensions:
            request._extensions = dict(extensions)
        authenticator = self._auth if isinstance(auth, _Unset) else _coerce_auth(auth)
        if authenticator is not None:
            replaced = authenticator(request)
            if isinstance(replaced, Request):
                replaced._prepared = True
                request = replaced
        return request

    def _prepare(self, request: Request) -> Request:
        """Apply client defaults to a user-constructed :class:`Request`."""
        target = self._merge_url(request.url)
        if self._params:
            target = merge_query(target, self._params)
        explicit = {key for key, _ in request.headers._list}
        header_list = [item for item in self._headers._list if item[0] not in explicit]
        header_list.extend(request.headers._list)
        prepared = Request._build(
            request.method,
            URL(target),
            Headers._from_raw(header_list),
            request.content,
            self._timeout if isinstance(request.timeout, _Unset) else request.timeout,
            request._cookie_base,
        )
        prepared._extensions = request._extensions
        if self._auth is not None and "authorization" not in explicit:
            replaced = self._auth(prepared)
            if isinstance(replaced, Request):
                replaced._prepared = True
                prepared = replaced
        return prepared

    def _coerce_gather_item(self, item: GatherItem) -> Request:
        if isinstance(item, Request):
            return item if item._prepared else self._prepare(item)
        if isinstance(item, (str, URL)):
            return self.build_request("GET", item)
        if isinstance(item, tuple) and len(item) in (2, 3):
            options: Mapping[str, Any] = item[2] if len(item) == 3 else {}
            return self.build_request(item[0], item[1], **options)
        raise TypeError(
            "gather() items must be a Request, a URL string, or a (method, url[, options]) tuple; "
            f"got {type(item).__name__}"
        )

    # -- cookies ----------------------------------------------------------

    def _apply_cookies(self, request: Request) -> None:
        jar_header = self._cookies.header_for(request.url)
        base = request._cookie_base
        if jar_header and base:
            request.headers["cookie"] = f"{base}; {jar_header}"
        elif jar_header or base:
            request.headers["cookie"] = jar_header or base or ""
        elif "cookie" in request.headers:
            del request.headers["cookie"]

    def _store_cookies(self, request: Request, response: Response) -> None:
        for item in response.headers._list:
            if item[0] == "set-cookie":
                self._cookies.extract(request.url, response.headers.get_list("set-cookie"))
                return

    # -- redirects --------------------------------------------------------

    def _build_redirect(self, request: Request, response: Response) -> Request:
        location = response.headers["location"]
        try:
            url = request.url.join(location)
        except ValueError as exc:
            raise TransportError(f"invalid redirect location {location!r}: {exc}") from None
        if url.scheme not in ("http", "https"):
            raise UnsupportedProtocol(
                f"redirect to unsupported URL scheme {url.scheme!r}: {location!r}"
            )
        if not url.fragment and request.url.fragment:
            url = URL(f"{url}#{request.url.fragment}")

        method = request.method
        status = response.status_code
        if (
            (status == 303 and method != "HEAD")
            or (status == 302 and method != "HEAD")
            or (status == 301 and method == "POST")
        ):
            method = "GET"

        headers = request.headers.copy()
        content = request.content
        if method != request.method and method == "GET":
            content = b""
            for name in _BODY_HEADERS:
                if name in headers:
                    del headers[name]
        cookie_base = request._cookie_base
        if url.origin != request.url.origin:
            same_host_upgrade = (
                url.host == request.url.host
                and request.url.scheme == "http"
                and url.scheme == "https"
                and request.url.effective_port == 80
                and url.effective_port == 443
            )
            if not same_host_upgrade:
                # Never leak credentials or request-scoped cookies cross-origin.
                if "authorization" in headers:
                    del headers["authorization"]
                cookie_base = None
        for name in ("host", "cookie"):
            if name in headers:
                del headers[name]
        redirected = Request._build(
            method, url, headers, content, _prepared_timeout(request), cookie_base
        )
        redirected._extensions = request._extensions
        return redirected

    def _check_open(self) -> None:
        if self._closed:
            raise ClientClosed("Cannot send a request: the client has been closed.")

    def _resolve_policy(self, retries: RetryTypes | _Unset, method: str) -> RetryPolicy | None:
        policy = self._retry if isinstance(retries, _Unset) else _coerce_retry(retries)
        if policy is not None and policy.allows_method(method):
            return policy
        return None

    def __repr__(self) -> str:
        base = f" base_url={self._base_url!r}" if self._base_url else ""
        return f"<{type(self).__name__}{base}>"


def _prepared_timeout(request: Request) -> Timeout | None:
    timeout = request.timeout
    return None if isinstance(timeout, _Unset) else timeout


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, TransportError) and not isinstance(exc, _NON_RETRYABLE)


def _too_many_redirects(request: Request, limit: int) -> TooManyRedirects:
    return TooManyRedirects(
        f"Exceeded the maximum of {limit} redirects (last URL: {request.url})", request=request
    )


# -- synchronous client -------------------------------------------------------


class Client(_BaseClient):
    """A synchronous HTTP client with connection pooling.

    ::

        with nextreq.Client(base_url="https://api.example.com", retries=3) as client:
            user = client.get("/users/1", model=User)

    Args:
        base_url: prefix for relative request URLs.
        headers / params / cookies / auth: defaults merged into every request.
        timeout: a number of seconds, a :class:`Timeout`, or ``None`` to
            disable. Defaults to connect=5, read=30, write=30, pool=10.
        follow_redirects: follow up to ``max_redirects`` (10) redirects.
        raise_for_status: raise :class:`HTTPStatusError` on 4xx / 5xx.
        retries: number of retries, or a :class:`RetryPolicy`. Default 0.
        verify: ``False`` to skip TLS verification, or a CA bundle path.
        cert: client certificate PEM path, or ``(cert, key)``.
        proxy: proxy URL, or ``{"http://": ..., "https://": ...}``.
        trust_env: honour ``HTTP_PROXY`` / ``HTTPS_PROXY`` / ``NO_PROXY``.
        http1 / http2 / http3: protocol selection (HTTP/3 needs a custom build).
        max_connections: cap on concurrent in-flight requests (and therefore
            on busy connections). ``None`` removes the cap.
        max_keepalive_connections: idle connections kept per host. ``None``
            (default) keeps every connection, so bursts never churn sockets.
        keepalive_expiry: seconds an idle connection is kept. ``None`` keeps
            idle connections until the server closes them.
        on_request / on_response / on_error: hooks (a callable or a list).
        transport: a custom :class:`Transport` (e.g. ``MockTransport``).
        app: a WSGI application to call in-process instead of the network.
    """

    def __init__(
        self,
        *,
        base_url: URLTypes = "",
        headers: HeaderTypes | None = None,
        params: QueryParamTypes | None = None,
        cookies: CookieTypes | None = None,
        auth: AuthTypes = None,
        timeout: TimeoutTypes | _Unset = UNSET,
        follow_redirects: bool = True,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        raise_for_status: bool = False,
        retries: RetryTypes = 0,
        verify: VerifyTypes = True,
        cert: CertTypes = None,
        proxy: ProxyTypes = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = True,
        http3: bool = False,
        max_connections: int | None = 100,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = 30.0,
        on_request: RequestHook | Iterable[RequestHook] | None = None,
        on_response: ResponseHook | Iterable[ResponseHook] | None = None,
        on_error: ErrorHook | Iterable[ErrorHook] | None = None,
        transport: Transport | None = None,
        app: Callable[..., Any] | None = None,
        _engine: HttpClient | None = None,
    ) -> None:
        if app is not None and transport is not None:
            raise TypeError("Pass either `app` or `transport`, not both.")
        if app is not None:
            from .testing import WSGITransport

            transport = WSGITransport(app)
            base_url = base_url or "http://testserver"
        super().__init__(
            base_url=base_url,
            headers=headers,
            params=params,
            cookies=cookies,
            auth=auth,
            timeout=timeout,
            follow_redirects=follow_redirects,
            max_redirects=max_redirects,
            raise_for_status=raise_for_status,
            retries=retries,
            on_request=on_request,
            on_response=on_response,
            on_error=on_error,
        )
        self._transport: Transport | None = transport
        if transport is None:
            if _engine is not None:
                self._raw = _engine
                self._owns_raw = False
            else:
                self._raw = HttpClient(
                    **_engine_kwargs(
                        verify=verify,
                        cert=cert,
                        proxy=proxy,
                        trust_env=trust_env,
                        http1=http1,
                        http2=http2,
                        http3=http3,
                        max_connections=max_connections,
                        max_keepalive_connections=max_keepalive_connections,
                        keepalive_expiry=keepalive_expiry,
                    )
                )

    # -- sending ----------------------------------------------------------

    def _exchange(self, request: Request, stream: bool) -> Response:
        """One request/response exchange, with cookies and hooks applied."""
        if self._cookies._jar or request._cookie_base is not None:
            self._apply_cookies(request)
        if self._on_request:
            for hook in self._on_request:
                hook(request)
        try:
            raw = self._raw
            if raw is not None:
                response = Response._from_raw(
                    raw.send(
                        request.method,
                        request.url._url,
                        request.headers._list,
                        request.content or None,
                        request.timeout,  # type: ignore[arg-type]
                        stream,
                    ),
                    request,
                )
            else:
                assert self._transport is not None
                started = time.perf_counter()
                response = self._transport.handle_request(request)
                if response.request is None:
                    response.request = request
                if not stream:
                    response.read()
                if response._timings is None:
                    response._timings = (time.perf_counter() - started, None, None, None, None)
        except Exception as exc:
            if isinstance(exc, NextReqError) and exc.request is None:
                exc.request = request
            for error_hook in self._on_error:
                error_hook(request, exc)
            raise
        self._store_cookies(request, response)
        if self._on_response:
            for response_hook in self._on_response:
                response_hook(response)
        return response

    def _send_following(self, request: Request, stream: bool, follow: bool) -> Response:
        response = self._exchange(request, stream)
        if (
            not follow
            or response.status_code not in _REDIRECT_STATUSES
            or "location" not in response.headers
        ):
            return response
        history: list[Response] = []
        while True:
            if len(history) >= self._max_redirects:
                response.close()
                raise _too_many_redirects(request, self._max_redirects)
            try:
                request = self._build_redirect(request, response)
            finally:
                response.close()
            history.append(response)
            response = self._exchange(request, stream)
            if response.status_code not in _REDIRECT_STATUSES or "location" not in response.headers:
                break
        response.history = history
        return response

    def _send_retrying(
        self, request: Request, stream: bool, follow: bool, policy: RetryPolicy
    ) -> Response:
        attempt = 0
        limit = policy.max_attempts
        while True:
            try:
                response = self._send_following(request, stream, follow)
            except TransportError as exc:
                if attempt >= limit or not _is_retryable(exc):
                    exc.retry_count = attempt  # type: ignore[attr-defined]
                    raise
                delay = policy.delay(attempt + 1)
            else:
                if attempt >= limit or not policy.allows_status(response.status_code):
                    response.retry_count = attempt
                    return response
                delay = policy.delay(attempt + 1, response.headers.get("retry-after"))
                response.close()
            attempt += 1
            if delay > 0:
                _sleep(delay)

    def send(
        self,
        request: Request,
        *,
        stream: bool = False,
        follow_redirects: bool | None = None,
        raise_for_status: bool | None = None,
        retries: RetryTypes | _Unset = UNSET,
    ) -> Response:
        """Send a :class:`Request`, applying redirects, retries and hooks."""
        if self._closed:
            self._check_open()
        if not request._prepared:
            request = self._prepare(request)
        follow = self._follow_redirects if follow_redirects is None else follow_redirects
        policy = self._resolve_policy(retries, request.method)
        if policy is None:
            response = self._send_following(request, stream, follow)
        else:
            response = self._send_retrying(request, stream, follow, policy)
        if (
            self._raise_for_status if raise_for_status is None else raise_for_status
        ) and response.status_code >= 400:
            if stream:
                response.read()
            response.raise_for_status()
        return response

    def _request(
        self,
        method: str,
        url: URLTypes,
        model: Any,
        *,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        cookies: CookieTypes | None = None,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        json: Any = None,
        files: RequestFiles | None = None,
        auth: AuthTypes | _Unset = UNSET,
        timeout: TimeoutTypes | _Unset = UNSET,
        follow_redirects: bool | None = None,
        raise_for_status: bool | None = None,
        retries: RetryTypes | _Unset = UNSET,
        extensions: dict[str, Any] | None = None,
    ) -> Any:
        request = self.build_request(
            method,
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            content=content,
            data=data,
            json=json,
            files=files,
            auth=auth,
            timeout=timeout,
            extensions=extensions,
        )
        if model is None:
            return self.send(
                request,
                follow_redirects=follow_redirects,
                raise_for_status=raise_for_status,
                retries=retries,
            )
        # Parsing an error body into a model would only produce a confusing
        # validation error, so typed requests check the status first unless
        # the caller explicitly opted out.
        response = self.send(
            request,
            follow_redirects=follow_redirects,
            raise_for_status=True if raise_for_status is None else raise_for_status,
            retries=retries,
        )
        return parse_response(response, model)

    @overload
    def request(
        self, method: str, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    def request(
        self, method: str, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    def request(
        self, method: str, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a request. With ``model=`` the JSON body is parsed into that type."""
        return self._request(method, url, model, **kwargs)

    @overload
    def get(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    def get(self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]) -> T: ...
    def get(self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]) -> Any:
        """Send a ``GET`` request."""
        return self._request("GET", url, model, **kwargs)

    @overload
    def post(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    def post(self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]) -> T: ...
    def post(self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]) -> Any:
        """Send a ``POST`` request."""
        return self._request("POST", url, model, **kwargs)

    @overload
    def put(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    def put(self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]) -> T: ...
    def put(self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]) -> Any:
        """Send a ``PUT`` request."""
        return self._request("PUT", url, model, **kwargs)

    @overload
    def patch(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    def patch(self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]) -> T: ...
    def patch(self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]) -> Any:
        """Send a ``PATCH`` request."""
        return self._request("PATCH", url, model, **kwargs)

    @overload
    def delete(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    def delete(self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]) -> T: ...
    def delete(self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]) -> Any:
        """Send a ``DELETE`` request."""
        return self._request("DELETE", url, model, **kwargs)

    def head(self, url: URLTypes, **kwargs: Unpack[RequestOptions]) -> Response:
        """Send a ``HEAD`` request."""
        response: Response = self._request("HEAD", url, None, **kwargs)
        return response

    def options(self, url: URLTypes, **kwargs: Unpack[RequestOptions]) -> Response:
        """Send an ``OPTIONS`` request."""
        response: Response = self._request("OPTIONS", url, None, **kwargs)
        return response

    @contextlib.contextmanager
    def stream(
        self, method: str, url: URLTypes, **kwargs: Unpack[RequestOptions]
    ) -> Iterator[Response]:
        """Send a request and stream the response body.

        ::

            with client.stream("GET", "/events") as response:
                for event in response.iter_sse():
                    ...
        """
        build_options, send_options = _split_options(kwargs)
        request = self.build_request(method, url, **build_options)
        response = self.send(request, stream=True, **send_options)
        try:
            yield response
        finally:
            response.close()

    def close(self) -> None:
        """Close the client and its connection pool."""
        if self._closed:
            return
        self._closed = True
        if self._raw is not None and self._owns_raw:
            self._raw.close()
        closer = getattr(self._transport, "close", None)
        if callable(closer):
            closer()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _split_options(kwargs: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split request keyword arguments into ``build_request()`` and ``send()`` options."""
    build: dict[str, Any] = {}
    send: dict[str, Any] = {}
    for name, value in kwargs.items():
        if name in ("follow_redirects", "raise_for_status", "retries"):
            send[name] = value
        else:
            build[name] = value
    return build, send


# -- asynchronous client ------------------------------------------------------


class AsyncClient(_BaseClient):
    """An asynchronous HTTP client with connection pooling.

    ::

        async with nextreq.AsyncClient(base_url="https://api.example.com") as client:
            response = await client.get("/users/1")
            pages = await client.gather([f"/items?page={n}" for n in range(100)])

    Accepts the same arguments as :class:`Client`; ``app`` takes an ASGI
    application, and hooks may be plain functions or coroutine functions.
    """

    def __init__(
        self,
        *,
        base_url: URLTypes = "",
        headers: HeaderTypes | None = None,
        params: QueryParamTypes | None = None,
        cookies: CookieTypes | None = None,
        auth: AuthTypes = None,
        timeout: TimeoutTypes | _Unset = UNSET,
        follow_redirects: bool = True,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        raise_for_status: bool = False,
        retries: RetryTypes = 0,
        verify: VerifyTypes = True,
        cert: CertTypes = None,
        proxy: ProxyTypes = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = True,
        http3: bool = False,
        max_connections: int | None = 100,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = 30.0,
        on_request: RequestHook | Iterable[RequestHook] | None = None,
        on_response: ResponseHook | Iterable[ResponseHook] | None = None,
        on_error: ErrorHook | Iterable[ErrorHook] | None = None,
        transport: AsyncTransport | None = None,
        app: Callable[..., Any] | None = None,
        _engine: HttpClient | None = None,
    ) -> None:
        if app is not None and transport is not None:
            raise TypeError("Pass either `app` or `transport`, not both.")
        if app is not None:
            from .testing import ASGITransport

            transport = ASGITransport(app)
            base_url = base_url or "http://testserver"
        super().__init__(
            base_url=base_url,
            headers=headers,
            params=params,
            cookies=cookies,
            auth=auth,
            timeout=timeout,
            follow_redirects=follow_redirects,
            max_redirects=max_redirects,
            raise_for_status=raise_for_status,
            retries=retries,
            on_request=on_request,
            on_response=on_response,
            on_error=on_error,
        )
        self._transport: AsyncTransport | None = transport
        if transport is None:
            if _engine is not None:
                self._raw = _engine
                self._owns_raw = False
            else:
                self._raw = HttpClient(
                    **_engine_kwargs(
                        verify=verify,
                        cert=cert,
                        proxy=proxy,
                        trust_env=trust_env,
                        http1=http1,
                        http2=http2,
                        http3=http3,
                        max_connections=max_connections,
                        max_keepalive_connections=max_keepalive_connections,
                        keepalive_expiry=keepalive_expiry,
                    )
                )

    # -- sending ----------------------------------------------------------

    async def _exchange(self, request: Request, stream: bool) -> Response:
        """One request/response exchange, with cookies and hooks applied."""
        if self._cookies._jar or request._cookie_base is not None:
            self._apply_cookies(request)
        if self._on_request:
            for hook in self._on_request:
                pending = hook(request)
                if inspect.isawaitable(pending):
                    await pending
        try:
            raw = self._raw
            if raw is not None:
                response = Response._from_raw(
                    await raw.send_async(
                        request.method,
                        request.url._url,
                        request.headers._list,
                        request.content or None,
                        request.timeout,  # type: ignore[arg-type]
                        stream,
                    ),
                    request,
                )
            else:
                assert self._transport is not None
                started = time.perf_counter()
                response = await self._transport.handle_async_request(request)
                if response.request is None:
                    response.request = request
                if not stream:
                    await response.aread()
                if response._timings is None:
                    response._timings = (time.perf_counter() - started, None, None, None, None)
        except Exception as exc:
            if isinstance(exc, NextReqError) and exc.request is None:
                exc.request = request
            for error_hook in self._on_error:
                pending = error_hook(request, exc)
                if inspect.isawaitable(pending):
                    await pending
            raise
        self._store_cookies(request, response)
        if self._on_response:
            for response_hook in self._on_response:
                pending = response_hook(response)
                if inspect.isawaitable(pending):
                    await pending
        return response

    async def _send_following(self, request: Request, stream: bool, follow: bool) -> Response:
        response = await self._exchange(request, stream)
        if (
            not follow
            or response.status_code not in _REDIRECT_STATUSES
            or "location" not in response.headers
        ):
            return response
        history: list[Response] = []
        while True:
            if len(history) >= self._max_redirects:
                await response.aclose()
                raise _too_many_redirects(request, self._max_redirects)
            try:
                request = self._build_redirect(request, response)
            finally:
                await response.aclose()
            history.append(response)
            response = await self._exchange(request, stream)
            if response.status_code not in _REDIRECT_STATUSES or "location" not in response.headers:
                break
        response.history = history
        return response

    async def _send_retrying(
        self, request: Request, stream: bool, follow: bool, policy: RetryPolicy
    ) -> Response:
        attempt = 0
        limit = policy.max_attempts
        while True:
            try:
                response = await self._send_following(request, stream, follow)
            except TransportError as exc:
                if attempt >= limit or not _is_retryable(exc):
                    exc.retry_count = attempt  # type: ignore[attr-defined]
                    raise
                delay = policy.delay(attempt + 1)
            else:
                if attempt >= limit or not policy.allows_status(response.status_code):
                    response.retry_count = attempt
                    return response
                delay = policy.delay(attempt + 1, response.headers.get("retry-after"))
                await response.aclose()
            attempt += 1
            if delay > 0:
                await _asleep(delay)

    async def send(
        self,
        request: Request,
        *,
        stream: bool = False,
        follow_redirects: bool | None = None,
        raise_for_status: bool | None = None,
        retries: RetryTypes | _Unset = UNSET,
    ) -> Response:
        """Send a :class:`Request`, applying redirects, retries and hooks."""
        if self._closed:
            self._check_open()
        if not request._prepared:
            request = self._prepare(request)
        follow = self._follow_redirects if follow_redirects is None else follow_redirects
        policy = self._resolve_policy(retries, request.method)
        if policy is None:
            response = await self._send_following(request, stream, follow)
        else:
            response = await self._send_retrying(request, stream, follow, policy)
        if (
            self._raise_for_status if raise_for_status is None else raise_for_status
        ) and response.status_code >= 400:
            if stream:
                await response.aread()
            response.raise_for_status()
        return response

    async def _request(
        self,
        method: str,
        url: URLTypes,
        model: Any,
        *,
        params: QueryParamTypes | None = None,
        headers: HeaderTypes | None = None,
        cookies: CookieTypes | None = None,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        json: Any = None,
        files: RequestFiles | None = None,
        auth: AuthTypes | _Unset = UNSET,
        timeout: TimeoutTypes | _Unset = UNSET,
        follow_redirects: bool | None = None,
        raise_for_status: bool | None = None,
        retries: RetryTypes | _Unset = UNSET,
        extensions: dict[str, Any] | None = None,
    ) -> Any:
        request = self.build_request(
            method,
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            content=content,
            data=data,
            json=json,
            files=files,
            auth=auth,
            timeout=timeout,
            extensions=extensions,
        )
        if model is None:
            return await self.send(
                request,
                follow_redirects=follow_redirects,
                raise_for_status=raise_for_status,
                retries=retries,
            )
        response = await self.send(
            request,
            follow_redirects=follow_redirects,
            raise_for_status=True if raise_for_status is None else raise_for_status,
            retries=retries,
        )
        return parse_response(response, model)

    @overload
    async def request(
        self, method: str, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    async def request(
        self, method: str, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    async def request(
        self, method: str, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a request. With ``model=`` the JSON body is parsed into that type."""
        return await self._request(method, url, model, **kwargs)

    @overload
    async def get(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    async def get(
        self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    async def get(
        self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a ``GET`` request."""
        return await self._request("GET", url, model, **kwargs)

    @overload
    async def post(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    async def post(
        self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    async def post(
        self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a ``POST`` request."""
        return await self._request("POST", url, model, **kwargs)

    @overload
    async def put(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    async def put(
        self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    async def put(
        self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a ``PUT`` request."""
        return await self._request("PUT", url, model, **kwargs)

    @overload
    async def patch(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    async def patch(
        self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    async def patch(
        self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a ``PATCH`` request."""
        return await self._request("PATCH", url, model, **kwargs)

    @overload
    async def delete(
        self, url: URLTypes, *, model: None = None, **kwargs: Unpack[RequestOptions]
    ) -> Response: ...
    @overload
    async def delete(
        self, url: URLTypes, *, model: type[T], **kwargs: Unpack[RequestOptions]
    ) -> T: ...
    async def delete(
        self, url: URLTypes, *, model: Any = None, **kwargs: Unpack[RequestOptions]
    ) -> Any:
        """Send a ``DELETE`` request."""
        return await self._request("DELETE", url, model, **kwargs)

    async def head(self, url: URLTypes, **kwargs: Unpack[RequestOptions]) -> Response:
        """Send a ``HEAD`` request."""
        response: Response = await self._request("HEAD", url, None, **kwargs)
        return response

    async def options(self, url: URLTypes, **kwargs: Unpack[RequestOptions]) -> Response:
        """Send an ``OPTIONS`` request."""
        response: Response = await self._request("OPTIONS", url, None, **kwargs)
        return response

    @contextlib.asynccontextmanager
    async def stream(
        self, method: str, url: URLTypes, **kwargs: Unpack[RequestOptions]
    ) -> AsyncIterator[Response]:
        """Send a request and stream the response body.

        ::

            async with client.stream("GET", "/events") as response:
                async for event in response.aiter_sse():
                    ...
        """
        build_options, send_options = _split_options(kwargs)
        request = self.build_request(method, url, **build_options)
        response = await self.send(request, stream=True, **send_options)
        try:
            yield response
        finally:
            await response.aclose()

    # -- concurrency ------------------------------------------------------

    @overload
    async def gather(
        self,
        requests: Iterable[GatherItem],
        *,
        max_concurrency: int = 20,
        return_exceptions: Literal[False] = False,
    ) -> list[Response]: ...
    @overload
    async def gather(
        self,
        requests: Iterable[GatherItem],
        *,
        max_concurrency: int = 20,
        return_exceptions: Literal[True],
    ) -> list[Response | BaseException]: ...
    async def gather(
        self,
        requests: Iterable[GatherItem],
        *,
        max_concurrency: int = 20,
        return_exceptions: bool = False,
    ) -> list[Response] | list[Response | BaseException]:
        """Send many requests concurrently and return responses in input order.

        Args:
            requests: :class:`Request` objects, URL strings (sent as ``GET``),
                or ``(method, url)`` / ``(method, url, options)`` tuples.
            max_concurrency: maximum number of requests in flight at once.
            return_exceptions: when true, failures are returned in place
                instead of raised; otherwise the first failure cancels the
                remaining requests and propagates.
        """
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        prepared = [self._coerce_gather_item(item) for item in requests]
        results: list[Any] = [None] * len(prepared)
        if not prepared:
            return results
        queue = iter(enumerate(prepared))

        async def worker() -> None:
            for index, request in queue:
                try:
                    results[index] = await self.send(request)
                except Exception as exc:
                    if not return_exceptions:
                        raise
                    results[index] = exc

        workers = [
            asyncio.ensure_future(worker()) for _ in range(min(max_concurrency, len(prepared)))
        ]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        return results

    # -- websockets -------------------------------------------------------

    def websocket(
        self,
        url: URLTypes,
        *,
        headers: HeaderTypes | None = None,
        subprotocols: Iterable[str] | None = None,
        auth: AuthTypes | _Unset = UNSET,
        timeout: TimeoutTypes | _Unset = UNSET,
        max_message_size: int | None = None,
    ) -> _WebSocketConnector:
        """Open a WebSocket connection.

        ::

            async with client.websocket("wss://example.com/ws") as ws:
                await ws.send_json({"hello": "world"})
                reply = await ws.receive_json()
        """
        self._check_open()
        if self._raw is None:
            raise WebSocketError("WebSockets are not supported with a custom transport or `app`.")
        request = self.build_request("GET", url, headers=headers, auth=auth, timeout=timeout)
        target = str(request.url)
        if target.startswith("http://"):
            target = "ws://" + target[7:]
        elif target.startswith("https://"):
            target = "wss://" + target[8:]
        if self._cookies._jar or request._cookie_base is not None:
            request.url = URL(target)
            self._apply_cookies(request)
        handshake_headers = [
            item for item in request.headers._list if item[0] not in ("accept", "accept-encoding")
        ]
        return _WebSocketConnector(
            self._raw,
            target,
            handshake_headers,
            list(subprotocols) if subprotocols else None,
            _prepared_timeout(request),
            max_message_size,
        )

    async def aclose(self) -> None:
        """Close the client and its connection pool."""
        if self._closed:
            return
        self._closed = True
        if self._raw is not None and self._owns_raw:
            self._raw.close()
        closer = getattr(self._transport, "aclose", None)
        if callable(closer):
            await closer()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()


# -- websockets ---------------------------------------------------------------


class WebSocket:
    """An open WebSocket connection (see :meth:`AsyncClient.websocket`)."""

    def __init__(self, connection: WebSocketConnection) -> None:
        self._conn = connection
        self._closed = False
        self.close_code: int | None = None
        self.close_reason: str | None = None

    @property
    def subprotocol(self) -> str | None:
        """The subprotocol selected by the server, if any."""
        return self._conn.subprotocol

    @property
    def response_headers(self) -> Headers:
        """Headers of the handshake response."""
        return Headers(self._conn.response_headers)

    @property
    def closed(self) -> bool:
        return self._closed

    async def send(self, data: str | bytes) -> None:
        """Send a text (``str``) or binary (``bytes``) message."""
        if isinstance(data, str):
            await self._conn.send_text(data)
        else:
            await self._conn.send_bytes(bytes(data))

    async def send_text(self, text: str) -> None:
        await self._conn.send_text(text)

    async def send_bytes(self, data: bytes) -> None:
        await self._conn.send_bytes(bytes(data))

    async def send_json(self, obj: Any, *, mode: Literal["text", "binary"] = "text") -> None:
        """Serialise ``obj`` to JSON and send it as a text (or binary) message."""
        payload = json_dumps(obj)
        if mode == "binary":
            await self._conn.send_bytes(payload)
        else:
            await self._conn.send_text(payload.decode("utf-8"))

    async def ping(self, data: bytes = b"") -> None:
        await self._conn.ping(data)

    async def receive(self, *, timeout: float | None = None) -> str | bytes:
        """Receive the next message.

        Raises:
            WebSocketDisconnect: the peer closed the connection.
            ReadTimeout: no message arrived within ``timeout`` seconds.
        """
        if self._closed:
            raise WebSocketDisconnect(self.close_code or 1006, self.close_reason or "")
        pending = self._conn.receive()
        if timeout is not None:
            try:
                kind, payload, code = await asyncio.wait_for(pending, timeout)
            except asyncio.TimeoutError:
                raise ReadTimeout(
                    f"timed out after {timeout:.3f}s waiting for a WebSocket message"
                ) from None
        else:
            kind, payload, code = await pending
        if kind == 2:
            self._closed = True
            self.close_code = code
            self.close_reason = str(payload)
            raise WebSocketDisconnect(code, str(payload))
        return payload

    async def receive_text(self, *, timeout: float | None = None) -> str:
        message = await self.receive(timeout=timeout)
        return message if isinstance(message, str) else message.decode("utf-8")

    async def receive_bytes(self, *, timeout: float | None = None) -> bytes:
        message = await self.receive(timeout=timeout)
        return message.encode("utf-8") if isinstance(message, str) else message

    async def receive_json(self, *, timeout: float | None = None) -> Any:
        import json

        return json.loads(await self.receive(timeout=timeout))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Perform the closing handshake. Safe to call repeatedly."""
        if not self._closed:
            self._closed = True
            self.close_code = code
            self.close_reason = reason
        await self._conn.close(code, reason)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> str | bytes:
        try:
            return await self.receive()
        except WebSocketDisconnect:
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()


class _WebSocketConnector:
    """Awaitable / async context manager returned by ``AsyncClient.websocket()``."""

    def __init__(
        self,
        engine: HttpClient,
        url: str,
        headers: list[tuple[str, str]],
        subprotocols: list[str] | None,
        timeout: Timeout | None,
        max_message_size: int | None,
    ) -> None:
        self._engine = engine
        self._url = url
        self._headers = headers
        self._subprotocols = subprotocols
        self._timeout = timeout
        self._max_message_size = max_message_size
        self._socket: WebSocket | None = None

    async def _connect(self) -> WebSocket:
        connection = await self._engine.ws_connect(
            self._url, self._headers, self._subprotocols, self._timeout, self._max_message_size
        )
        self._socket = WebSocket(connection)
        return self._socket

    def __await__(self) -> Generator[Any, None, WebSocket]:
        return self._connect().__await__()

    async def __aenter__(self) -> WebSocket:
        return await self._connect()

    async def __aexit__(self, *exc_info: object) -> None:
        if self._socket is not None:
            await self._socket.close()


# -- module-level helpers -----------------------------------------------------


def _one_shot_client(
    verify: VerifyTypes, cert: CertTypes, proxy: ProxyTypes, trust_env: bool
) -> Client:
    """A throwaway client. Default settings reuse the shared engine so
    consecutive ``nextreq.get()`` calls keep their connections alive, while
    cookies stay isolated per call."""
    if verify is True and cert is None and proxy is None and trust_env:
        return Client(_engine=_shared_engine())
    return Client(verify=verify, cert=cert, proxy=proxy, trust_env=trust_env)


@overload
def request(
    method: str,
    url: URLTypes,
    *,
    model: None = None,
    verify: VerifyTypes = True,
    cert: CertTypes = None,
    proxy: ProxyTypes = None,
    trust_env: bool = True,
    **kwargs: Unpack[RequestOptions],
) -> Response: ...
@overload
def request(
    method: str,
    url: URLTypes,
    *,
    model: type[T],
    verify: VerifyTypes = True,
    cert: CertTypes = None,
    proxy: ProxyTypes = None,
    trust_env: bool = True,
    **kwargs: Unpack[RequestOptions],
) -> T: ...
def request(
    method: str,
    url: URLTypes,
    *,
    model: Any = None,
    verify: VerifyTypes = True,
    cert: CertTypes = None,
    proxy: ProxyTypes = None,
    trust_env: bool = True,
    **kwargs: Unpack[RequestOptions],
) -> Any:
    """Send a single request without managing a client.

    Connections are pooled across calls; cookies are not shared between calls.
    """
    with _one_shot_client(verify, cert, proxy, trust_env) as client:
        return client._request(method, url, model, **kwargs)


class _TopLevelOptions(RequestOptions, total=False):
    verify: VerifyTypes
    cert: CertTypes
    proxy: ProxyTypes
    trust_env: bool


@overload
def get(url: URLTypes, *, model: None = None, **kwargs: Unpack[_TopLevelOptions]) -> Response: ...
@overload
def get(url: URLTypes, *, model: type[T], **kwargs: Unpack[_TopLevelOptions]) -> T: ...
def get(url: URLTypes, *, model: Any = None, **kwargs: Unpack[_TopLevelOptions]) -> Any:
    """Send a ``GET`` request."""
    return request("GET", url, model=model, **kwargs)


@overload
def post(url: URLTypes, *, model: None = None, **kwargs: Unpack[_TopLevelOptions]) -> Response: ...
@overload
def post(url: URLTypes, *, model: type[T], **kwargs: Unpack[_TopLevelOptions]) -> T: ...
def post(url: URLTypes, *, model: Any = None, **kwargs: Unpack[_TopLevelOptions]) -> Any:
    """Send a ``POST`` request."""
    return request("POST", url, model=model, **kwargs)


@overload
def put(url: URLTypes, *, model: None = None, **kwargs: Unpack[_TopLevelOptions]) -> Response: ...
@overload
def put(url: URLTypes, *, model: type[T], **kwargs: Unpack[_TopLevelOptions]) -> T: ...
def put(url: URLTypes, *, model: Any = None, **kwargs: Unpack[_TopLevelOptions]) -> Any:
    """Send a ``PUT`` request."""
    return request("PUT", url, model=model, **kwargs)


@overload
def patch(url: URLTypes, *, model: None = None, **kwargs: Unpack[_TopLevelOptions]) -> Response: ...
@overload
def patch(url: URLTypes, *, model: type[T], **kwargs: Unpack[_TopLevelOptions]) -> T: ...
def patch(url: URLTypes, *, model: Any = None, **kwargs: Unpack[_TopLevelOptions]) -> Any:
    """Send a ``PATCH`` request."""
    return request("PATCH", url, model=model, **kwargs)


@overload
def delete(
    url: URLTypes, *, model: None = None, **kwargs: Unpack[_TopLevelOptions]
) -> Response: ...
@overload
def delete(url: URLTypes, *, model: type[T], **kwargs: Unpack[_TopLevelOptions]) -> T: ...
def delete(url: URLTypes, *, model: Any = None, **kwargs: Unpack[_TopLevelOptions]) -> Any:
    """Send a ``DELETE`` request."""
    return request("DELETE", url, model=model, **kwargs)


def head(url: URLTypes, **kwargs: Unpack[_TopLevelOptions]) -> Response:
    """Send a ``HEAD`` request."""
    return request("HEAD", url, **kwargs)


def options(url: URLTypes, **kwargs: Unpack[_TopLevelOptions]) -> Response:
    """Send an ``OPTIONS`` request."""
    return request("OPTIONS", url, **kwargs)


@contextlib.contextmanager
def stream(
    method: str,
    url: URLTypes,
    *,
    verify: VerifyTypes = True,
    cert: CertTypes = None,
    proxy: ProxyTypes = None,
    trust_env: bool = True,
    **kwargs: Unpack[RequestOptions],
) -> Iterator[Response]:
    """Send a single streaming request without managing a client."""
    with (
        _one_shot_client(verify, cert, proxy, trust_env) as client,
        client.stream(method, url, **kwargs) as response,
    ):
        yield response


@overload
async def gather(
    requests: Iterable[GatherItem],
    *,
    max_concurrency: int = 20,
    return_exceptions: Literal[False] = False,
    client: AsyncClient | None = None,
) -> list[Response]: ...
@overload
async def gather(
    requests: Iterable[GatherItem],
    *,
    max_concurrency: int = 20,
    return_exceptions: Literal[True],
    client: AsyncClient | None = None,
) -> list[Response | BaseException]: ...
async def gather(
    requests: Iterable[GatherItem],
    *,
    max_concurrency: int = 20,
    return_exceptions: bool = False,
    client: AsyncClient | None = None,
) -> list[Response] | list[Response | BaseException]:
    """Send many requests concurrently, preserving input order.

    Uses ``client`` if given, otherwise a temporary :class:`AsyncClient`
    backed by the shared connection pool.
    """
    if client is not None:
        if return_exceptions:
            return await client.gather(
                requests, max_concurrency=max_concurrency, return_exceptions=True
            )
        return await client.gather(requests, max_concurrency=max_concurrency)
    async with AsyncClient(_engine=_shared_engine()) as temporary:
        if return_exceptions:
            return await temporary.gather(
                requests, max_concurrency=max_concurrency, return_exceptions=True
            )
        return await temporary.gather(requests, max_concurrency=max_concurrency)
