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
import hashlib
import inspect
import os
import pathlib
import re
import threading
import time
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
)
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, TypedDict, TypeVar, Union, overload
from urllib.parse import unquote_plus

from . import _nextreq
from ._nextreq import (
    HttpClient,
    LoopBridge,
    RetryPolicy,
    Timeout,
    UploadStream,
    WebSocketConnection,
    loop_bridge,
    request_target,
)
from .downloads import WRITE_BATCH, Download, ProgressCallback
from .exceptions import (
    ClientClosed,
    DecodingError,
    LocalProtocolError,
    NextReqError,
    ReadTimeout,
    StreamConsumed,
    TooManyRedirects,
    TransportError,
    UnsupportedProtocol,
    WebSocketDisconnect,
    WebSocketError,
)
from .limits import RateLimit, RateLimitTypes, coerce_rate_limit
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
    RequestStream,
    Response,
    TimeoutTypes,
    URLTypes,
    _Unset,
    body_headers,
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
    "DigestAuth",
    "SyncWebSocket",
    "Transport",
    "WebSocket",
    "delete",
    "download",
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
NextPage = Callable[[Response], Any]
"""Given a page, returns the next one: a URL, a mapping of query parameters to
change, a :class:`Request`, or ``None`` when there are no more pages."""


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
    """A synchronous transport: turns a :class:`Request` into a :class:`Response`.

    A request whose body is streamed (``request.stream`` is set) is read into
    ``request.content`` before the transport sees it. A transport that wants
    the stream itself sets the class attribute ``streams_requests = True`` and
    consumes ``request.stream.open()``.
    """

    def handle_request(self, request: Request) -> Response: ...


class AsyncTransport(Protocol):
    """An asynchronous transport.

    As for :class:`Transport`, set ``streams_requests = True`` to receive
    streamed request bodies as a stream (``request.stream.aopen()``).
    """

    async def handle_async_request(self, request: Request) -> Response: ...


# -- auth ---------------------------------------------------------------------


class Auth:
    """Base class for request authentication.

    Simple schemes implement ``__call__`` and set a header on the request.

    Schemes that depend on the server's answer (challenge / response, token
    refresh) override :meth:`auth_flow` instead: a generator that yields the
    request to send and receives the response to it, as often as it needs::

        class RefreshingAuth(nextreq.Auth):
            def auth_flow(self, request):
                request.headers["authorization"] = f"Bearer {self.token}"
                response = yield request
                if response.status_code == 401:
                    self.token = fetch_new_token()
                    request.headers["authorization"] = f"Bearer {self.token}"
                    yield request

    The same generator drives ``Client`` and ``AsyncClient``, so it must not
    block. Set ``requires_response_body = True`` if the flow reads the body of
    the responses it receives.
    """

    requires_response_body: bool = False

    def __call__(self, request: Request) -> Request | None:
        raise NotImplementedError

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        replaced = self(request)
        yield replaced if isinstance(replaced, Request) else request


def _has_flow(auth: object) -> bool:
    """Whether ``auth`` needs to see responses (it overrides ``auth_flow``)."""
    return isinstance(auth, Auth) and type(auth).auth_flow is not Auth.auth_flow


def _attach_auth(request: Request, auth: Callable[[Request], Any]) -> Request:
    """Apply ``auth`` now, or - for a response-aware flow - at send time."""
    if _has_flow(auth):
        request._auth = auth
        return request
    replaced = auth(request)
    if isinstance(replaced, Request):
        replaced._prepared = True
        return replaced
    return request


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


_DIGEST_ALGORITHMS: dict[str, str] = {
    "MD5": "md5",
    "SHA": "sha1",
    "SHA-256": "sha256",
    "SHA-512": "sha512",
    "SHA-512-256": "sha512_256",
}
_AUTH_PARAM = re.compile(r'\s*,?\s*([\w-]+)\s*=\s*("(?:[^"\\]|\\.)*"|[^\s,]*)')


def _parse_digest_challenge(header: str) -> dict[str, str] | None:
    """The parameters of the ``Digest`` challenge in a ``WWW-Authenticate`` value.

    The value may carry several challenges (``Basic realm="a", Digest ...``);
    only the parameters that belong to ``Digest`` are returned.
    """
    found = re.search(r"(?:^|,)\s*digest\s+", header, re.IGNORECASE)
    if found is None:
        return None
    params: dict[str, str] = {}
    position = found.end()
    while (pair := _AUTH_PARAM.match(header, position)) is not None:
        value = pair.group(2)
        if value.startswith('"'):
            value = re.sub(r"\\(.)", r"\1", value[1:-1])
        params[pair.group(1).lower()] = value
        position = pair.end()
    return params if "nonce" in params else None


def _quote_auth_value(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class DigestAuth(Auth):
    """HTTP Digest authentication (RFC 7616, compatible with RFC 2617 / 2069).

    The first request is sent without credentials; when the server answers
    ``401`` with a ``Digest`` challenge the request is repeated with the
    response to it. The challenge is remembered, so later requests
    authenticate straight away.

    Supports the ``MD5``, ``SHA``, ``SHA-256``, ``SHA-512`` and ``SHA-512-256``
    algorithms and their ``-sess`` variants, with ``qop=auth`` and
    ``qop=auth-int``.

    ``auth-int`` also signs the request body, and is used only when the server
    offers nothing else. The body is hashed before it is sent, so it has to be
    in memory, a seekable file or a re-iterable object (not a generator).
    """

    def __init__(self, username: str | bytes, password: str | bytes) -> None:
        self._username = username.encode("utf-8") if isinstance(username, str) else username
        self._password = password.encode("utf-8") if isinstance(password, str) else password
        self._challenge: dict[str, str] | None = None
        self._nonce_count = 0
        self._lock = threading.Lock()

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        challenge = self._challenge
        if challenge is not None:
            request.headers["authorization"] = self._authorization(request, challenge)
        response = yield request
        if response.status_code != 401:
            return
        challenge = _parse_digest_challenge(
            ", ".join(response.headers.get_list("www-authenticate"))
        )
        if challenge is None:
            return
        answered = "authorization" in request.headers
        if (
            answered
            and challenge.get("stale", "").lower() != "true"
            and self._same_nonce(challenge)
        ):
            return  # the credentials themselves were rejected; do not loop
        with self._lock:
            self._challenge = challenge
            self._nonce_count = 0
        request.headers["authorization"] = self._authorization(request, challenge)
        yield request

    def _same_nonce(self, challenge: dict[str, str]) -> bool:
        current = self._challenge
        return current is not None and current.get("nonce") == challenge.get("nonce")

    def _cnonce(self) -> str:
        return os.urandom(16).hex()

    def _authorization(self, request: Request, challenge: dict[str, str]) -> str:
        algorithm = challenge.get("algorithm", "MD5")
        name = algorithm.upper()
        session = name.endswith("-SESS")
        hash_name = _DIGEST_ALGORITHMS.get(name[:-5] if session else name)
        if hash_name is None or hash_name not in hashlib.algorithms_available:
            raise LocalProtocolError(f"unsupported Digest auth algorithm {algorithm!r}")

        def digest(data: bytes) -> str:
            return hashlib.new(hash_name, data).hexdigest()

        qop_options = [option.strip().lower() for option in challenge.get("qop", "").split(",")]
        qop_options = [option for option in qop_options if option]
        qop: str | None = None
        if "auth" in qop_options:
            qop = "auth"
        elif "auth-int" in qop_options:
            qop = "auth-int"
        elif qop_options:
            raise LocalProtocolError(
                f"unsupported Digest auth quality of protection {challenge.get('qop')!r} "
                '(expected "auth" or "auth-int")'
            )
        realm = challenge.get("realm", "")
        nonce = challenge["nonce"]
        uri = request_target(request.url._url)  # exactly as sent on the wire
        cnonce = self._cnonce()
        with self._lock:
            self._nonce_count += 1
            nonce_count = f"{self._nonce_count:08x}"

        ha1 = digest(b":".join((self._username, realm.encode("utf-8"), self._password)))
        if session:
            ha1 = digest(f"{ha1}:{nonce}:{cnonce}".encode())
        if qop == "auth-int":
            body_hash = self._body_hash(request, hash_name)
            ha2 = digest(f"{request.method}:{uri}:{body_hash}".encode())
        else:
            ha2 = digest(f"{request.method}:{uri}".encode())
        if qop is not None:
            response = digest(f"{ha1}:{nonce}:{nonce_count}:{cnonce}:{qop}:{ha2}".encode())
        else:  # RFC 2069
            response = digest(f"{ha1}:{nonce}:{ha2}".encode())

        fields = [
            f"username={_quote_auth_value(self._username.decode('utf-8', 'replace'))}",
            f"realm={_quote_auth_value(realm)}",
            f"nonce={_quote_auth_value(nonce)}",
            f"uri={_quote_auth_value(uri)}",
            f'response="{response}"',
        ]
        if "algorithm" in challenge:
            fields.append(f"algorithm={algorithm}")
        if "opaque" in challenge:
            fields.append(f"opaque={_quote_auth_value(challenge['opaque'])}")
        if qop is not None:
            fields.extend((f"qop={qop}", f"nc={nonce_count}", f'cnonce="{cnonce}"'))
        return "Digest " + ", ".join(fields)

    @staticmethod
    def _body_hash(request: Request, hash_name: str) -> str:
        """The hash of the request body, which ``qop=auth-int`` signs."""
        hasher = hashlib.new(hash_name)
        stream = request.stream
        if stream is None:
            hasher.update(request.content)
        elif stream.is_async or not stream.replayable:
            raise LocalProtocolError(
                "Digest auth with qop=auth-int signs the request body, so the body is "
                "needed before it is sent, and this one is a one-shot or async stream. "
                "Pass bytes, a seekable file or a re-iterable object instead."
            )
        else:
            for chunk in stream.open():  # read once here, replayed when it is sent
                hasher.update(chunk)
        return hasher.hexdigest()


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


_get_running_loop = asyncio.get_running_loop


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
    uds: str | os.PathLike[str] | None,
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
    if uds is not None:
        if proxy:
            raise ValueError("`uds` cannot be combined with `proxy`")
        kwargs["uds"] = os.fspath(uds)
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
        rate_limit: RateLimitTypes,
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
        self._rate_limit = coerce_rate_limit(rate_limit)
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
    def rate_limit(self) -> RateLimit | None:
        """The :class:`RateLimit` this client observes, if any."""
        return self._rate_limit

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
        return self._build_for(
            method,
            target,
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

    def _build_for(
        self,
        method: str,
        target: str,
        *,
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
        """Build a request for ``target``, a complete URL that is used as it is."""
        header_list = list(self._headers._list)
        if headers:
            extra = Headers(headers)._list
            overridden = {key for key, _ in extra}
            header_list = [item for item in header_list if item[0] not in overridden]
            header_list.extend(extra)
        merged = Headers._from_raw(header_list)

        body: bytes | RequestStream = b""
        if content is not None or data is not None or json is not None or files:
            body, content_type = encode_body(content, data, json, files)
            header_list.extend(body_headers(body, content_type, merged))
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
            body if isinstance(body, bytes) else b"",
            self._timeout if isinstance(timeout, _Unset) else coerce_timeout(timeout),
            cookie_base,
        )
        if not isinstance(body, bytes):
            request.stream = body
        if extensions:
            request._extensions = dict(extensions)
        authenticator = self._auth if isinstance(auth, _Unset) else _coerce_auth(auth)
        if authenticator is not None:
            request = _attach_auth(request, authenticator)
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
        prepared.stream = request.stream
        prepared._extensions = request._extensions
        if self._auth is not None and "authorization" not in explicit:
            prepared = _attach_auth(prepared, self._auth)
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
        body_stream = request.stream
        if method != request.method and method == "GET":
            content = b""
            body_stream = None
            for name in _BODY_HEADERS:
                if name in headers:
                    del headers[name]
        elif body_stream is not None and not body_stream.replayable:
            raise StreamConsumed(
                f"Cannot follow the {status} redirect to {url}: it requires sending the "
                "request body again, but the body is a one-shot stream. "
                "Pass bytes, a seekable file or a re-iterable object instead."
            )
        cookie_base = request._cookie_base
        flow_auth = request._auth
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
                flow_auth = None
        for name in ("host", "cookie"):
            if name in headers:
                del headers[name]
        redirected = Request._build(
            method, url, headers, content, _prepared_timeout(request), cookie_base
        )
        redirected.stream = body_stream
        redirected._extensions = request._extensions
        redirected._auth = flow_auth
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


_WebSocketRefusal = tuple[int, list[tuple[str, str]], bytes]


def _websocket_handshake(
    client: _BaseClient,
    url: URLTypes,
    headers: HeaderTypes | None,
    auth: AuthTypes | _Unset,
    timeout: TimeoutTypes | _Unset,
) -> Request:
    """The handshake of a WebSocket connection: a GET for a ``ws(s)://`` URL."""
    request = client.build_request("GET", url, headers=headers, auth=auth, timeout=timeout)
    target = str(request.url)
    if target.startswith("http://"):
        request.url = URL("ws://" + target[7:])
    elif target.startswith("https://"):
        request.url = URL("wss://" + target[8:])
    return request


def _handshake_headers(client: _BaseClient, request: Request) -> list[tuple[str, str]]:
    """The headers to send with a handshake, read again for every attempt."""
    if client._cookies._jar or request._cookie_base is not None:
        client._apply_cookies(request)
    return [item for item in request.headers._list if item[0] not in ("accept", "accept-encoding")]


def _handshake_refused(request: Request, refusal: _WebSocketRefusal) -> Response:
    status, headers, body = refusal
    return Response(status, headers=headers, content=body, request=request)


def _handshake_error(response: Response) -> WebSocketError:
    return WebSocketError(
        f"WebSocket handshake failed: server responded with HTTP {response.status_code}",
        request=response.request,
        response=response,
    )


def _flow_ended_early() -> WebSocketError:
    return WebSocketError("The auth flow ended without sending the WebSocket handshake.")


def _prepared_timeout(request: Request) -> Timeout | None:
    timeout = request.timeout
    return None if isinstance(timeout, _Unset) else timeout


def _can_resend(request: Request) -> bool:
    """Whether the request body can be produced again (for a retry)."""
    return request.stream is None or request.stream.replayable


async def _pump_upload(stream: RequestStream, upload: UploadStream) -> None:
    """Feed a streamed request body to the engine as the connection takes it."""
    try:
        async for chunk in stream.aopen():
            status = upload.try_send(chunk)
            if status == 1:  # queue full: wait for the connection to catch up
                status = 0 if await upload.send(chunk) else 2
            if status == 2:  # the request ended without needing the rest
                return
    except BaseException:
        upload.fail()
        raise
    upload.finish()


def _retrieve_exception(task: asyncio.Future[Any]) -> None:
    """Done-callback for a detached task, so its failure is not logged as unretrieved."""
    if not task.cancelled():
        task.exception()


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, TransportError) and not isinstance(exc, _NON_RETRYABLE)


def _can_continue(
    policy: RetryPolicy | None, attempt: int, request: Request, exc: TransportError
) -> bool:
    """Whether a download whose connection broke may be requested again."""
    return (
        policy is not None
        and attempt < policy.max_attempts
        and _is_retryable(exc)
        and _can_resend(request)
    )


def _too_many_redirects(request: Request, limit: int) -> TooManyRedirects:
    return TooManyRedirects(
        f"Exceeded the maximum of {limit} redirects (last URL: {request.url})", request=request
    )


def _replace_query(url: str, params: Mapping[str, Any]) -> str:
    """``url`` with the query parameters named in ``params`` replaced."""
    url, _, fragment = url.partition("#")
    base, _, query = url.partition("?")
    replaced = {unquote_plus(str(name)) for name in params}
    kept = "&".join(
        pair
        for pair in query.split("&")
        if pair and unquote_plus(pair.partition("=")[0]) not in replaced
    )
    target = merge_query(f"{base}?{kept}" if kept else base, params)
    return f"{target}#{fragment}" if fragment else target


class _Pager:
    """Works out the request for the page after each response of ``paginate()``."""

    def __init__(
        self,
        client: _BaseClient,
        first: Request,
        options: dict[str, Any],
        next_page: NextPage | None,
        max_pages: int | None,
    ) -> None:
        if max_pages is not None and max_pages < 1:
            raise ValueError("max_pages must be >= 1")
        self._client = client
        self._method = first.method
        self._first_url = str(first.url)
        # The first URL already carries every query parameter.
        self._options = {name: value for name, value in options.items() if name != "params"}
        self._next_page = next_page
        self._max_pages = max_pages
        self._pages = 0
        self._seen = {self._first_url}

    def target(self, response: Response) -> Any:
        """What the caller's ``next_page`` (or the ``Link`` header) points to."""
        self._pages += 1
        if self._max_pages is not None and self._pages >= self._max_pages:
            return None
        if self._next_page is not None:
            return self._next_page(response)
        return response.links.get("next", {}).get("url")

    def request(self, response: Response, target: Any) -> Request | None:
        """The request for ``target``, or ``None`` when the last page was reached."""
        if not target:
            return None
        if isinstance(target, Request):
            return target
        if isinstance(target, Mapping):
            url = _replace_query(self._first_url, target)
        else:
            url = str(response.url.join(str(target)))
        if url in self._seen:  # the server sent us round in a circle
            return None
        self._seen.add(url)
        return self._client._build_for(self._method, url, **self._options)


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
        uds: path of a Unix domain socket to connect to instead of the host
            in the URL (e.g. ``/var/run/docker.sock``). Not on Windows.
        rate_limit: requests per second, or a :class:`RateLimit`. Requests
            beyond it wait their turn. Default: no limit.
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
        uds: str | os.PathLike[str] | None = None,
        rate_limit: RateLimitTypes = None,
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
            rate_limit=rate_limit,
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
                        uds=uds,
                    )
                )

    # -- sending ----------------------------------------------------------

    def _exchange(self, request: Request, stream: bool) -> Response:
        """One request/response exchange, with cookies and hooks applied."""
        limit = self._rate_limit
        if limit is not None:
            wait = limit.reserve()
            if wait > 0:
                _sleep(wait)
        if self._cookies._jar or request._cookie_base is not None:
            self._apply_cookies(request)
        if self._on_request:
            for hook in self._on_request:
                hook(request)
        try:
            raw = self._raw
            if raw is not None:
                body_stream = request.stream
                response = Response._from_raw(
                    raw.send(
                        request.method,
                        request.url._url,
                        request.headers._list,
                        (request.content or None) if body_stream is None else body_stream.open(),
                        request.timeout,  # type: ignore[arg-type]
                        stream,
                    ),
                    request,
                )
            else:
                assert self._transport is not None
                started = time.perf_counter()
                if request.stream is not None and not getattr(
                    self._transport, "streams_requests", False
                ):
                    request.read()  # this transport takes the body in one piece
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

    def _exchange_authed(self, request: Request, stream: bool) -> Response:
        """One exchange, or as many as the request's auth flow asks for."""
        auth = request._auth
        if auth is None:
            return self._exchange(request, stream)
        flow = auth.auth_flow(request)
        try:
            current = next(flow)
            while True:
                if not current._prepared:
                    current = self._prepare(current)
                    current._auth = None
                response = self._exchange(current, stream)
                if stream and auth.requires_response_body:
                    response.read()
                try:
                    current = flow.send(response)
                except StopIteration:
                    return response
                response.close()  # an intermediate answer (e.g. the 401 challenge)
        finally:
            flow.close()

    def _send_following(self, request: Request, stream: bool, follow: bool) -> Response:
        response = self._exchange_authed(request, stream)
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
            response = self._exchange_authed(request, stream)
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
                if attempt >= limit or not _is_retryable(exc) or not _can_resend(request):
                    exc.retry_count = attempt  # type: ignore[attr-defined]
                    raise
                delay = policy.delay(attempt + 1)
            else:
                if (
                    attempt >= limit
                    or not policy.allows_status(response.status_code)
                    or not _can_resend(request)
                ):
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

    # -- pagination and downloads -----------------------------------------

    def paginate(
        self,
        url: URLTypes,
        *,
        method: str = "GET",
        next_page: NextPage | None = None,
        max_pages: int | None = None,
        **kwargs: Unpack[RequestOptions],
    ) -> Iterator[Response]:
        """Iterate over the pages of a paginated endpoint, one response each.

        ::

            for page in client.paginate("/items", params={"per_page": 100}):
                for item in page.json():
                    ...

        By default the next page is the ``rel="next"`` link of the ``Link``
        header (GitHub, GitLab and many others). For any other scheme pass
        ``next_page``, a function that takes a page and returns one of:

        - a URL (absolute, or relative to the page) to fetch next;
        - a mapping of query parameters to change on the first request, e.g.
          ``{"cursor": page.json()["next_cursor"]}``;
        - a :class:`Request`, for anything else (such as a cursor in a body);
        - ``None`` (or an empty value) when there are no more pages.

        Headers, auth and the other options are sent with every page.
        ``max_pages`` stops after that many pages. Iteration also stops if a
        page points to a URL that was already fetched.
        """
        build_options, send_options = _split_options(kwargs)
        first = self.build_request(method, url, **build_options)
        pager = _Pager(self, first, build_options, next_page, max_pages)
        request: Request | None = first
        while request is not None:
            response = self.send(request, **send_options)
            yield response
            request = pager.request(response, pager.target(response))

    def download(
        self,
        url: URLTypes,
        path: str | os.PathLike[str],
        *,
        method: str = "GET",
        resume: bool = False,
        on_progress: ProgressCallback | None = None,
        **kwargs: Unpack[RequestOptions],
    ) -> pathlib.Path:
        """Download ``url`` to the file ``path`` without holding it in memory.

        ::

            client.download("https://example.com/big.iso", "big.iso", resume=True)

        The body is written to ``<path>.part`` and renamed to ``path`` when it
        is complete, so ``path`` never holds half a file. An error status
        raises :class:`HTTPStatusError` and writes nothing.

        Args:
            resume: continue a ``.part`` file that an earlier call left
                behind, instead of starting again. The server is asked for
                the missing bytes with ``Range`` and ``If-Range``; if the file
                changed there, or the server cannot do ranges, the download
                starts again.
            on_progress: called with ``(bytes_on_disk, total_or_None)``.
            retries: besides its usual meaning, the number of times a
                connection that breaks in the middle of the body is picked
                up again: from where it stopped, or from the start when the
                server sent no ``ETag`` or ``Last-Modified`` to check the
                rest against.

        Returns the path of the finished file.
        """
        build_options, send_options = _split_options(kwargs)
        policy = self._resolve_policy(send_options.get("retries", UNSET), method.upper())
        send_options["raise_for_status"] = False
        state = Download(path, resume=resume, on_progress=on_progress)
        attempt = 0
        while True:
            request = self.build_request(method, url, **build_options)
            state.prepare(request)
            response = self.send(request, stream=True, **send_options)
            try:
                if response.status_code == 416 and state.ranged:
                    state.restart()  # the part is not a prefix of the file
                    continue
                if response.status_code >= 400:
                    response.read()
                    response.raise_for_status()
                state.begin(response)
                for chunk in response.iter_bytes():
                    state.write((chunk,))
                    state.report()
                return state.complete(response)
            except TransportError as exc:
                state.interrupted()
                if not _can_continue(policy, attempt, request, exc):
                    raise
                attempt += 1
                assert policy is not None
                delay = policy.delay(attempt)
            except BaseException:
                state.interrupted()
                raise
            finally:
                response.close()
            if delay > 0:
                _sleep(delay)

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
        compression: bool = False,
    ) -> SyncWebSocket:
        """Open a WebSocket connection and return it once the handshake is done.

        ::

            with client.websocket("wss://example.com/ws") as ws:
                ws.send_json({"hello": "world"})
                reply = ws.receive_json()

        The connection uses the client's TLS settings, proxies, cookies and
        default headers. ``compression=True`` offers permessage-deflate; check
        ``ws.compressed`` to see whether the server accepted it.

        ``auth`` applies to the handshake, including schemes that answer a
        challenge (``DigestAuth``, or your own ``auth_flow``). A handshake the
        server refuses raises :class:`WebSocketError` with the server's answer
        in ``.response``.
        """
        self._check_open()
        raw = self._raw
        if raw is None:
            raise WebSocketError("WebSockets are not supported with a custom transport or `app`.")
        request = _websocket_handshake(self, url, headers, auth, timeout)
        protocols = list(subprotocols) if subprotocols else None

        def attempt(current: Request) -> WebSocketConnection | Response:
            outcome = raw.ws_connect_blocking(
                str(current.url),
                _handshake_headers(self, current),
                protocols,
                _prepared_timeout(current),
                max_message_size,
                compression,
            )
            return _handshake_refused(current, outcome) if isinstance(outcome, tuple) else outcome

        opened = (
            attempt(request) if request._auth is None else self._authed_handshake(request, attempt)
        )
        if isinstance(opened, Response):
            raise _handshake_error(opened)
        return SyncWebSocket(opened)

    def _authed_handshake(
        self, request: Request, attempt: Callable[[Request], WebSocketConnection | Response]
    ) -> WebSocketConnection | Response:
        """Open a WebSocket, letting the request's auth flow answer refusals."""
        flow = request._auth.auth_flow(request)
        try:
            current = next(flow)
            while True:
                handshake = current.url.scheme in ("ws", "wss")
                if handshake:
                    answer = attempt(current)
                    if not isinstance(answer, Response):
                        return answer
                else:  # a request of the flow's own, such as fetching a token
                    if not current._prepared:
                        current = self._prepare(current)
                        current._auth = None
                    answer = self._exchange(current, False)
                try:
                    current = flow.send(answer)
                except StopIteration:
                    if handshake:
                        return answer  # the flow has no answer to this refusal
                    raise _flow_ended_early() from None
        finally:
            flow.close()

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
        uds: str | os.PathLike[str] | None = None,
        rate_limit: RateLimitTypes = None,
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
            rate_limit=rate_limit,
            on_request=on_request,
            on_response=on_response,
            on_error=on_error,
        )
        self._transport: AsyncTransport | None = transport
        self._loop: asyncio.AbstractEventLoop | None = None
        self._bridge: LoopBridge | None = None
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
                        uds=uds,
                    )
                )

    # -- sending ----------------------------------------------------------

    def _bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Attach to the running event loop (a client may outlive one loop)."""
        self._loop = loop
        self._bridge = loop_bridge(loop)

    async def _exchange(self, request: Request, stream: bool) -> Response:
        """One request/response exchange, with cookies and hooks applied."""
        limit = self._rate_limit
        if limit is not None:
            wait = limit.reserve()
            if wait > 0:
                await _asleep(wait)
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
                loop = _get_running_loop()
                if loop is not self._loop:
                    self._bind_loop(loop)
                future = loop.create_future()
                body_stream = request.stream
                if body_stream is None:
                    task = raw.send_async(
                        self._bridge,  # type: ignore[arg-type]
                        future,
                        request.method,
                        request.url._url,
                        request.headers._list,
                        request.content or None,
                        request.timeout,  # type: ignore[arg-type]
                        stream,
                    )
                    try:
                        response = Response._from_raw(await future, request)
                    except BaseException:
                        # Cancelled (or failed): stop the in-flight request so its
                        # connection slot is released immediately.
                        task.abort()
                        raise
                else:
                    response = await self._exchange_streamed(
                        raw, loop, future, request, body_stream, stream
                    )
            else:
                assert self._transport is not None
                started = time.perf_counter()
                if request.stream is not None and not getattr(
                    self._transport, "streams_requests", False
                ):
                    await request.aread()  # this transport takes the body in one piece
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

    async def _exchange_streamed(
        self,
        raw: HttpClient,
        loop: asyncio.AbstractEventLoop,
        future: asyncio.Future[Any],
        request: Request,
        body_stream: RequestStream,
        stream: bool,
    ) -> Response:
        """Send a request whose body is produced while it is in flight."""
        upload = UploadStream()
        task = raw.send_async(
            self._bridge,  # type: ignore[arg-type]
            future,
            request.method,
            request.url._url,
            request.headers._list,
            upload,
            request.timeout,  # type: ignore[arg-type]
            stream,
        )
        pump = loop.create_task(_pump_upload(body_stream, upload))
        try:
            result = await future
        except BaseException as exc:
            task.abort()
            if pump.done() and not pump.cancelled():
                failure = pump.exception()
                if failure is not None and not isinstance(exc, asyncio.CancelledError):
                    # The body could not be produced: that is the real error.
                    raise failure from None
            pump.cancel()
            pump.add_done_callback(_retrieve_exception)
            raise
        if not pump.done():
            if stream:
                # A streaming response may arrive while the body is still being
                # sent; the upload continues until the response is closed.
                pump.add_done_callback(_retrieve_exception)
            else:
                pump.cancel()  # the exchange is complete; the rest is not needed
                pump.add_done_callback(_retrieve_exception)
        return Response._from_raw(result, request)

    async def _exchange_authed(self, request: Request, stream: bool) -> Response:
        """One exchange, or as many as the request's auth flow asks for."""
        auth = request._auth
        if auth is None:
            return await self._exchange(request, stream)
        flow = auth.auth_flow(request)
        try:
            current = next(flow)
            while True:
                if not current._prepared:
                    current = self._prepare(current)
                    current._auth = None
                response = await self._exchange(current, stream)
                if stream and auth.requires_response_body:
                    await response.aread()
                try:
                    current = flow.send(response)
                except StopIteration:
                    return response
                await response.aclose()  # an intermediate answer (e.g. the 401 challenge)
        finally:
            flow.close()

    async def _send_following(self, request: Request, stream: bool, follow: bool) -> Response:
        response = await self._exchange_authed(request, stream)
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
            response = await self._exchange_authed(request, stream)
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
                if attempt >= limit or not _is_retryable(exc) or not _can_resend(request):
                    exc.retry_count = attempt  # type: ignore[attr-defined]
                    raise
                delay = policy.delay(attempt + 1)
            else:
                if (
                    attempt >= limit
                    or not policy.allows_status(response.status_code)
                    or not _can_resend(request)
                ):
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

    # -- pagination and downloads -----------------------------------------

    async def paginate(
        self,
        url: URLTypes,
        *,
        method: str = "GET",
        next_page: NextPage | None = None,
        max_pages: int | None = None,
        **kwargs: Unpack[RequestOptions],
    ) -> AsyncIterator[Response]:
        """Iterate over the pages of a paginated endpoint, one response each.

        ::

            async for page in client.paginate("/items", params={"per_page": 100}):
                for item in page.json():
                    ...

        See :meth:`Client.paginate`. ``next_page`` may also be a coroutine
        function.
        """
        build_options, send_options = _split_options(kwargs)
        first = self.build_request(method, url, **build_options)
        pager = _Pager(self, first, build_options, next_page, max_pages)
        request: Request | None = first
        while request is not None:
            response = await self.send(request, **send_options)
            yield response
            target = pager.target(response)
            if inspect.isawaitable(target):
                target = await target
            request = pager.request(response, target)

    async def download(
        self,
        url: URLTypes,
        path: str | os.PathLike[str],
        *,
        method: str = "GET",
        resume: bool = False,
        on_progress: ProgressCallback | None = None,
        **kwargs: Unpack[RequestOptions],
    ) -> pathlib.Path:
        """Download ``url`` to the file ``path`` without holding it in memory.

        See :meth:`Client.download`. The file is written in a worker thread,
        so a slow disk does not stall the event loop; ``on_progress`` is
        called on the event loop.
        """
        build_options, send_options = _split_options(kwargs)
        policy = self._resolve_policy(send_options.get("retries", UNSET), method.upper())
        send_options["raise_for_status"] = False
        state = Download(path, resume=resume, on_progress=on_progress)
        loop = _get_running_loop()
        attempt = 0
        while True:
            request = self.build_request(method, url, **build_options)
            state.prepare(request)
            response = await self.send(request, stream=True, **send_options)
            pending: list[bytes] = []
            try:
                if response.status_code == 416 and state.ranged:
                    state.restart()  # the part is not a prefix of the file
                    continue
                if response.status_code >= 400:
                    await response.aread()
                    response.raise_for_status()
                state.begin(response)
                size = 0
                async for chunk in response.aiter_bytes():
                    pending.append(chunk)
                    size += len(chunk)
                    if size >= WRITE_BATCH:
                        batch, pending, size = pending, [], 0
                        await loop.run_in_executor(None, state.write, batch)
                        state.report()
                if pending:
                    batch, pending = pending, []
                    await loop.run_in_executor(None, state.write, batch)
                    state.report()
                return state.complete(response)
            except TransportError as exc:
                if pending:  # keep what arrived before the connection broke
                    state.write(pending)
                state.interrupted()
                if not _can_continue(policy, attempt, request, exc):
                    raise
                attempt += 1
                assert policy is not None
                delay = policy.delay(attempt)
            except BaseException:
                state.interrupted()
                raise
            finally:
                await response.aclose()
            if delay > 0:
                await _asleep(delay)

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
        compression: bool = False,
    ) -> _WebSocketConnector:
        """Open a WebSocket connection.

        ::

            async with client.websocket("wss://example.com/ws") as ws:
                await ws.send_json({"hello": "world"})
                reply = await ws.receive_json()

        The connection uses the client's TLS settings, proxies, cookies and
        default headers. ``compression=True`` offers permessage-deflate; check
        ``ws.compressed`` to see whether the server accepted it.

        ``auth`` applies to the handshake, including schemes that answer a
        challenge (``DigestAuth``, or your own ``auth_flow``). A handshake the
        server refuses raises :class:`WebSocketError` with the server's answer
        in ``.response``.
        """
        self._check_open()
        if self._raw is None:
            raise WebSocketError("WebSockets are not supported with a custom transport or `app`.")
        return _WebSocketConnector(
            self,
            self._raw,
            _websocket_handshake(self, url, headers, auth, timeout),
            list(subprotocols) if subprotocols else None,
            max_message_size,
            compression,
        )

    async def _authed_handshake(
        self,
        request: Request,
        attempt: Callable[[Request], Awaitable[WebSocketConnection | Response]],
    ) -> WebSocketConnection | Response:
        """Open a WebSocket, letting the request's auth flow answer refusals."""
        flow = request._auth.auth_flow(request)
        try:
            current = next(flow)
            while True:
                handshake = current.url.scheme in ("ws", "wss")
                if handshake:
                    answer = await attempt(current)
                    if not isinstance(answer, Response):
                        return answer
                else:  # a request of the flow's own, such as fetching a token
                    if not current._prepared:
                        current = self._prepare(current)
                        current._auth = None
                    answer = await self._exchange(current, False)
                try:
                    current = flow.send(answer)
                except StopIteration:
                    if handshake:
                        return answer  # the flow has no answer to this refusal
                    raise _flow_ended_early() from None
        finally:
            flow.close()

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


class _WebSocketBase:
    """State shared by the async and the sync WebSocket."""

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
    def compressed(self) -> bool:
        """Whether permessage-deflate compression was negotiated."""
        return self._conn.compressed

    @property
    def closed(self) -> bool:
        return self._closed

    def _check_receivable(self) -> None:
        if self._closed:
            raise WebSocketDisconnect(self.close_code or 1006, self.close_reason or "")

    def _unpack(self, message: tuple[int, str | bytes, int]) -> str | bytes:
        kind, payload, code = message
        if kind == 2:
            self._closed = True
            self.close_code = code
            self.close_reason = str(payload)
            raise WebSocketDisconnect(code, str(payload))
        return payload

    def _mark_closed(self, code: int, reason: str) -> None:
        if not self._closed:
            self._closed = True
            self.close_code = code
            self.close_reason = reason


def _as_text(message: str | bytes) -> str:
    return message if isinstance(message, str) else message.decode("utf-8")


def _as_bytes(message: str | bytes) -> bytes:
    return message.encode("utf-8") if isinstance(message, str) else message


class WebSocket(_WebSocketBase):
    """An open WebSocket connection (see :meth:`AsyncClient.websocket`)."""

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
        self._check_receivable()
        pending = self._conn.receive()
        if timeout is not None:
            try:
                message = await asyncio.wait_for(pending, timeout)
            except asyncio.TimeoutError:
                raise ReadTimeout(
                    f"timed out after {timeout:.3f}s waiting for a WebSocket message"
                ) from None
        else:
            message = await pending
        return self._unpack(message)

    async def receive_text(self, *, timeout: float | None = None) -> str:
        return _as_text(await self.receive(timeout=timeout))

    async def receive_bytes(self, *, timeout: float | None = None) -> bytes:
        return _as_bytes(await self.receive(timeout=timeout))

    async def receive_json(self, *, timeout: float | None = None) -> Any:
        import json

        return json.loads(await self.receive(timeout=timeout))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Perform the closing handshake. Safe to call repeatedly."""
        self._mark_closed(code, reason)
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


class SyncWebSocket(_WebSocketBase):
    """An open WebSocket connection with a blocking API (see :meth:`Client.websocket`).

    Calls block the calling thread with the GIL released. One thread may sit
    in :meth:`receive` while another sends.
    """

    def send(self, data: str | bytes) -> None:
        """Send a text (``str``) or binary (``bytes``) message."""
        if isinstance(data, str):
            self._conn.send_text_blocking(data)
        else:
            self._conn.send_bytes_blocking(bytes(data))

    def send_text(self, text: str) -> None:
        self._conn.send_text_blocking(text)

    def send_bytes(self, data: bytes) -> None:
        self._conn.send_bytes_blocking(bytes(data))

    def send_json(self, obj: Any, *, mode: Literal["text", "binary"] = "text") -> None:
        """Serialise ``obj`` to JSON and send it as a text (or binary) message."""
        payload = json_dumps(obj)
        if mode == "binary":
            self._conn.send_bytes_blocking(payload)
        else:
            self._conn.send_text_blocking(payload.decode("utf-8"))

    def ping(self, data: bytes = b"") -> None:
        self._conn.ping_blocking(data)

    def receive(self, *, timeout: float | None = None) -> str | bytes:
        """Receive the next message.

        Raises:
            WebSocketDisconnect: the peer closed the connection.
            ReadTimeout: no message arrived within ``timeout`` seconds.
        """
        self._check_receivable()
        return self._unpack(self._conn.receive_blocking(timeout))

    def receive_text(self, *, timeout: float | None = None) -> str:
        return _as_text(self.receive(timeout=timeout))

    def receive_bytes(self, *, timeout: float | None = None) -> bytes:
        return _as_bytes(self.receive(timeout=timeout))

    def receive_json(self, *, timeout: float | None = None) -> Any:
        import json

        return json.loads(self.receive(timeout=timeout))

    def close(self, code: int = 1000, reason: str = "") -> None:
        """Perform the closing handshake. Safe to call repeatedly."""
        self._mark_closed(code, reason)
        self._conn.close_blocking(code, reason)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> str | bytes:
        try:
            return self.receive()
        except WebSocketDisconnect:
            raise StopIteration from None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class _WebSocketConnector:
    """Awaitable / async context manager returned by ``AsyncClient.websocket()``."""

    def __init__(
        self,
        client: AsyncClient,
        engine: HttpClient,
        request: Request,
        subprotocols: list[str] | None,
        max_message_size: int | None,
        compression: bool = False,
    ) -> None:
        self._client = client
        self._engine = engine
        self._request = request
        self._subprotocols = subprotocols
        self._max_message_size = max_message_size
        self._compression = compression
        self._socket: WebSocket | None = None

    async def _attempt(self, request: Request) -> WebSocketConnection | Response:
        outcome = await self._engine.ws_connect(
            str(request.url),
            _handshake_headers(self._client, request),
            self._subprotocols,
            _prepared_timeout(request),
            self._max_message_size,
            self._compression,
        )
        return _handshake_refused(request, outcome) if isinstance(outcome, tuple) else outcome

    async def _connect(self) -> WebSocket:
        request = self._request
        if request._auth is None:
            opened = await self._attempt(request)
        else:
            opened = await self._client._authed_handshake(request, self._attempt)
        if isinstance(opened, Response):
            raise _handshake_error(opened)
        self._socket = WebSocket(opened)
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


def download(
    url: URLTypes,
    path: str | os.PathLike[str],
    *,
    resume: bool = False,
    on_progress: ProgressCallback | None = None,
    verify: VerifyTypes = True,
    cert: CertTypes = None,
    proxy: ProxyTypes = None,
    trust_env: bool = True,
    **kwargs: Unpack[RequestOptions],
) -> pathlib.Path:
    """Download ``url`` to the file ``path``; see :meth:`Client.download`."""
    with _one_shot_client(verify, cert, proxy, trust_env) as client:
        return client.download(url, path, resume=resume, on_progress=on_progress, **kwargs)


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
