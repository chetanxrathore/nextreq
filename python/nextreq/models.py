"""Core data model: ``URL``, ``Headers``, ``Cookies``, ``Request``, ``Response``, ``SSEEvent``."""

from __future__ import annotations

import codecs
import dataclasses
import datetime
import enum
import json as _json
import mimetypes
import os
import re
import time
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Sequence,
)
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import IO, TYPE_CHECKING, Any, Final, TypeVar, Union, overload
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

from ._nextreq import ResponseStream, SSEEvent, SSEParser, Timeout
from .exceptions import (
    HTTPStatusError,
    InvalidURL,
    ResponseNotRead,
    StreamClosed,
    StreamConsumed,
)

if TYPE_CHECKING:
    from typing_extensions import Self

__all__ = [
    "UNSET",
    "URL",
    "Cookies",
    "Elapsed",
    "Headers",
    "Request",
    "Response",
    "SSEEvent",
]

T = TypeVar("T")


class _Unset(enum.Enum):
    """Sentinel type for "argument not provided" (distinct from ``None``)."""

    UNSET = "UNSET"

    def __repr__(self) -> str:
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET: Final = _Unset.UNSET

# -- public type aliases ------------------------------------------------------

PrimitiveData = Union[str, int, float, bool, None]
URLTypes = Union["URL", str]
HeaderTypes = Union[
    "Headers",
    Mapping[str, str],
    Sequence[tuple[str, str]],
]
QueryParamTypes = Union[
    Mapping[str, PrimitiveData | Sequence[PrimitiveData]],
    Sequence[tuple[str, PrimitiveData]],
    str,
]
CookieTypes = Union["Cookies", Mapping[str, str], Sequence[tuple[str, str]]]
TimeoutTypes = Union[float, int, Timeout, None]
RequestContent = Union[str, bytes, bytearray, memoryview, Iterable[bytes]]
RequestData = Union[Mapping[str, Any], Sequence[tuple[str, Any]], str, bytes]
FileContent = Union[IO[bytes], bytes, str]
FileTypes = Union[
    FileContent,
    tuple[str | None, FileContent],
    tuple[str | None, FileContent, str | None],
    tuple[str | None, FileContent, str | None, Mapping[str, str]],
]
RequestFiles = Union[Mapping[str, FileTypes], Sequence[tuple[str, FileTypes]]]


# -- URL ----------------------------------------------------------------------

_DEFAULT_PORTS: Final = {"http": 80, "https": 443, "ws": 80, "wss": 443}


def _primitive_to_str(value: PrimitiveData) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return ""
    return str(value)


def encode_query_params(params: QueryParamTypes | None) -> str:
    """Encode query parameters. ``None`` values are omitted (requests-style)."""
    if not params:
        return ""
    if isinstance(params, str):
        return params.lstrip("?")
    pairs: list[tuple[str, str]] = []
    items: Iterable[tuple[str, Any]] = params.items() if isinstance(params, Mapping) else params
    for key, value in items:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set, frozenset)):
            pairs.extend((key, _primitive_to_str(v)) for v in value if v is not None)
        else:
            pairs.append((key, _primitive_to_str(value)))
    return urlencode(pairs)


def merge_query(url: str, params: QueryParamTypes | None) -> str:
    """Append ``params`` to ``url``, preserving any existing query and fragment."""
    query = encode_query_params(params)
    if not query:
        return url
    fragment = ""
    if "#" in url:
        url, _, fragment = url.partition("#")
        fragment = "#" + fragment
    separator = "&" if "?" in url else "?"
    if url.endswith(("?", "&")):
        separator = ""
    return f"{url}{separator}{query}{fragment}"


class URL:
    """A parsed, immutable URL. Compares equal to its string form."""

    __slots__ = ("_parts", "_url")

    def __init__(self, url: URLTypes = "", *, params: QueryParamTypes | None = None) -> None:
        if isinstance(url, URL):
            text = url._url
        elif isinstance(url, str):
            text = url
        else:
            raise TypeError(f"URL must be a str or URL, not {type(url).__name__}")
        if params:
            text = merge_query(text, params)
        self._url: str = text
        self._parts: Any = None

    def _split(self) -> Any:
        parts = self._parts
        if parts is None:
            try:
                parts = self._parts = urlsplit(self._url)
            except ValueError as exc:
                raise InvalidURL(f"invalid URL {self._url!r}: {exc}") from None
        return parts

    @property
    def scheme(self) -> str:
        return str(self._split().scheme).lower()

    @property
    def host(self) -> str:
        """Lower-cased hostname without port, userinfo or IPv6 brackets."""
        return self._split().hostname or ""

    @property
    def port(self) -> int | None:
        """The explicit port, or ``None`` when the URL does not specify one."""
        try:
            port: int | None = self._split().port
        except ValueError as exc:
            raise InvalidURL(f"invalid URL {self._url!r}: {exc}") from None
        return port

    @property
    def effective_port(self) -> int | None:
        """The explicit port, falling back to the scheme's default port."""
        port = self.port
        return port if port is not None else _DEFAULT_PORTS.get(self.scheme)

    @property
    def netloc(self) -> str:
        return str(self._split().netloc)

    @property
    def username(self) -> str:
        return self._split().username or ""

    @property
    def password(self) -> str:
        return self._split().password or ""

    @property
    def path(self) -> str:
        return self._split().path or "/"

    @property
    def query(self) -> str:
        return str(self._split().query)

    @property
    def fragment(self) -> str:
        return str(self._split().fragment)

    @property
    def raw_path(self) -> str:
        """Path plus query string, as sent on the request line."""
        query = self.query
        return f"{self.path}?{query}" if query else self.path

    @property
    def origin(self) -> tuple[str, str, int | None]:
        """``(scheme, host, port)`` with default ports made explicit."""
        return (self.scheme, self.host, self.effective_port)

    @property
    def is_absolute(self) -> bool:
        parts = self._split()
        return bool(parts.scheme and parts.netloc)

    def join(self, url: URLTypes) -> URL:
        """Resolve ``url`` relative to this URL (RFC 3986)."""
        return URL(urljoin(self._url, str(url)))

    def copy_merge_params(self, params: QueryParamTypes | None) -> URL:
        return URL(merge_query(self._url, params))

    def without_userinfo(self) -> URL:
        parts = self._split()
        if "@" not in parts.netloc:
            return self
        netloc = parts.netloc.rpartition("@")[2]
        return URL(urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment)))

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        return f"URL({self._url!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, URL):
            return self._url == other._url
        if isinstance(other, str):
            return self._url == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._url)


# -- Headers ------------------------------------------------------------------

_SENSITIVE_HEADERS: Final = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie"}
)


def _normalize_header_value(value: object, name: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("latin-1")
    raise TypeError(f"Header value for {name!r} must be str or bytes, not {type(value).__name__}")


class Headers(MutableMapping[str, str]):
    """Case-insensitive, multi-value HTTP headers.

    Lookups are case-insensitive. A header that occurs several times is
    returned as one comma-joined value by ``headers[name]`` and as individual
    values by :meth:`get_list` (important for ``Set-Cookie``).
    """

    __slots__ = ("_list", "_map")

    def __init__(self, headers: HeaderTypes | None = None) -> None:
        self._map: dict[str, str] | None = None
        self._list: list[tuple[str, str]]
        if headers is None:
            self._list = []
        elif isinstance(headers, Headers):
            self._list = list(headers._list)
        elif isinstance(headers, Mapping):
            self._list = [
                (str(k).lower(), _normalize_header_value(v, str(k))) for k, v in headers.items()
            ]
        else:
            self._list = [(str(k).lower(), _normalize_header_value(v, str(k))) for k, v in headers]

    @classmethod
    def _from_raw(cls, items: list[tuple[str, str]]) -> Headers:
        """Wrap an already lower-cased list without copying (internal)."""
        self = cls.__new__(cls)
        self._list = items
        self._map = None
        return self

    def _mapping(self) -> dict[str, str]:
        mapping = self._map
        if mapping is None:
            mapping = {}
            for key, value in self._list:
                if key in mapping:
                    mapping[key] = f"{mapping[key]}, {value}"
                else:
                    mapping[key] = value
            self._map = mapping
        return mapping

    def __getitem__(self, key: str) -> str:
        return self._mapping()[key.lower()]

    def __setitem__(self, key: str, value: str) -> None:
        name = key.lower()
        value = _normalize_header_value(value, key)
        if self._map is not None and name not in self._map:
            self._list.append((name, value))
            self._map[name] = value
            return
        self._list = [item for item in self._list if item[0] != name]
        self._list.append((name, value))
        self._map = None

    def __delitem__(self, key: str) -> None:
        name = key.lower()
        remaining = [item for item in self._list if item[0] != name]
        if len(remaining) == len(self._list):
            raise KeyError(key)
        self._list = remaining
        self._map = None

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key.lower() in self._mapping()

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping())

    def __len__(self) -> int:
        return len(self._mapping())

    @overload
    def get(self, key: str, default: None = None) -> str | None: ...
    @overload
    def get(self, key: str, default: str) -> str: ...
    @overload
    def get(self, key: str, default: T) -> str | T: ...
    def get(self, key: str, default: Any = None) -> Any:
        return self._mapping().get(key.lower(), default)

    def get_list(self, key: str) -> list[str]:
        """Every value of a repeated header, in the order received."""
        name = key.lower()
        return [value for k, value in self._list if k == name]

    def add(self, key: str, value: str) -> None:
        """Append a value without replacing existing ones."""
        self._list.append((key.lower(), _normalize_header_value(value, key)))
        self._map = None

    def multi_items(self) -> list[tuple[str, str]]:
        """All ``(name, value)`` pairs, including repeated names."""
        return list(self._list)

    @property
    def raw(self) -> list[tuple[str, str]]:
        return list(self._list)

    def copy(self) -> Headers:
        return Headers._from_raw(list(self._list))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Headers):
            return sorted(self._list) == sorted(other._list)
        if isinstance(other, Mapping):
            try:
                return self._mapping() == {str(k).lower(): v for k, v in other.items()}
            except Exception:
                return False
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        shown = {
            k: ("[secure]" if k in _SENSITIVE_HEADERS else v) for k, v in self._mapping().items()
        }
        return f"Headers({shown!r})"


# -- Cookies ------------------------------------------------------------------


class _Cookie:
    __slots__ = ("domain", "expires", "host_only", "name", "path", "secure", "value")

    def __init__(
        self,
        name: str,
        value: str,
        domain: str,
        path: str,
        *,
        host_only: bool,
        secure: bool,
        expires: float | None,
    ) -> None:
        self.name = name
        self.value = value
        self.domain = domain
        self.path = path
        self.host_only = host_only
        self.secure = secure
        self.expires = expires

    def matches(self, scheme: str, host: str, path: str, now: float) -> bool:
        if self.expires is not None and self.expires <= now:
            return False
        if self.secure and scheme not in ("https", "wss"):
            return False
        domain = self.domain
        if domain:
            if self.host_only:
                if host != domain:
                    return False
            elif host != domain and not host.endswith("." + domain):
                return False
        cookie_path = self.path
        if cookie_path in ("", "/") or path == cookie_path:
            return True
        if path.startswith(cookie_path):
            return cookie_path.endswith("/") or path[len(cookie_path)] == "/"
        return False


def _default_cookie_path(request_path: str) -> str:
    if not request_path.startswith("/") or request_path.count("/") <= 1:
        return "/"
    return request_path.rsplit("/", 1)[0]


def _parse_cookie_expiry(value: str) -> float | None:
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def parse_set_cookie(header: str) -> tuple[str, str, dict[str, str]] | None:
    """Split a ``Set-Cookie`` header into ``(name, value, attributes)``."""
    first, _, rest = header.partition(";")
    name, sep, value = first.partition("=")
    name = name.strip()
    if not sep or not name:
        return None
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        value = value[1:-1]
    attributes: dict[str, str] = {}
    for chunk in rest.split(";"):
        key, _, attr_value = chunk.partition("=")
        key = key.strip().lower()
        if key:
            attributes[key] = attr_value.strip()
    return name, value, attributes


class Cookies(MutableMapping[str, str]):
    """A persistent cookie jar with domain / path / secure / expiry matching.

    Behaves as a ``name -> value`` mapping for convenience. Cookies set
    directly (``client.cookies["k"] = "v"``) have no domain and are sent to
    every host; cookies received from servers are scoped as the server asked.
    """

    __slots__ = ("_jar",)

    def __init__(self, cookies: CookieTypes | None = None) -> None:
        self._jar: dict[tuple[str, str, str], _Cookie] = {}
        if cookies is None:
            return
        if isinstance(cookies, Cookies):
            self._jar = dict(cookies._jar)
        elif isinstance(cookies, Mapping):
            for name, value in cookies.items():
                self.set(name, value)
        else:
            for name, value in cookies:
                self.set(name, value)

    def set(
        self,
        name: str,
        value: str,
        *,
        domain: str = "",
        path: str = "/",
        secure: bool = False,
        expires: float | None = None,
    ) -> None:
        """Store a cookie. An empty ``domain`` matches every host."""
        domain = domain.lower().lstrip(".")
        self._jar[(domain, path, name)] = _Cookie(
            name, value, domain, path, host_only=False, secure=secure, expires=expires
        )

    def delete(self, name: str, *, domain: str | None = None, path: str | None = None) -> None:
        """Remove every cookie called ``name`` (optionally scoped)."""
        for key in [
            key
            for key in self._jar
            if key[2] == name
            and (domain is None or key[0] == domain.lower().lstrip("."))
            and (path is None or key[1] == path)
        ]:
            del self._jar[key]

    def clear(self, *, domain: str | None = None) -> None:
        if domain is None:
            self._jar.clear()
            return
        wanted = domain.lower().lstrip(".")
        for key in [key for key in self._jar if key[0] == wanted]:
            del self._jar[key]

    def extract(self, url: URL, set_cookie_headers: Iterable[str]) -> None:
        """Store cookies from ``Set-Cookie`` headers received for ``url``."""
        host = url.host
        now = time.time()
        for header in set_cookie_headers:
            parsed = parse_set_cookie(header)
            if parsed is None:
                continue
            name, value, attrs = parsed
            domain_attr = attrs.get("domain", "").lower().lstrip(".")
            if domain_attr:
                # A server may only set cookies for its own domain or a parent.
                if host != domain_attr and not host.endswith("." + domain_attr):
                    continue
                # Refuse dotless "domains" (e.g. "com") unless it is the host itself.
                if "." not in domain_attr and host != domain_attr:
                    continue
                domain, host_only = domain_attr, False
            else:
                domain, host_only = host, True
            path = attrs.get("path", "")
            if not path.startswith("/"):
                path = _default_cookie_path(url.path)
            expires: float | None = None
            if "max-age" in attrs:
                try:
                    expires = now + int(attrs["max-age"])
                except ValueError:
                    expires = None
            elif "expires" in attrs:
                expires = _parse_cookie_expiry(attrs["expires"])
            key = (domain, path, name)
            if expires is not None and expires <= now:
                self._jar.pop(key, None)
                continue
            self._jar[key] = _Cookie(
                name,
                value,
                domain,
                path,
                host_only=host_only,
                secure="secure" in attrs,
                expires=expires,
            )

    def header_for(self, url: URL) -> str | None:
        """The ``Cookie`` header value to send to ``url``, if any."""
        if not self._jar:
            return None
        now = time.time()
        scheme, host, path = url.scheme, url.host, url.path
        matched = [c for c in self._jar.values() if c.matches(scheme, host, path, now)]
        if not matched:
            return None
        matched.sort(key=lambda c: len(c.path), reverse=True)
        return "; ".join(f"{c.name}={c.value}" for c in matched)

    def _live(self) -> list[_Cookie]:
        now = time.time()
        expired = [k for k, c in self._jar.items() if c.expires is not None and c.expires <= now]
        for key in expired:
            del self._jar[key]
        return list(self._jar.values())

    def __getitem__(self, name: str) -> str:
        for cookie in self._live():
            if cookie.name == name:
                return cookie.value
        raise KeyError(name)

    def __setitem__(self, name: str, value: str) -> None:
        self.set(name, value)

    def __delitem__(self, name: str) -> None:
        if not any(key[2] == name for key in self._jar):
            raise KeyError(name)
        self.delete(name)

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for cookie in self._live():
            if cookie.name not in seen:
                seen.add(cookie.name)
                yield cookie.name

    def __len__(self) -> int:
        return len({cookie.name for cookie in self._live()})

    def __bool__(self) -> bool:
        return bool(self._jar)

    def __repr__(self) -> str:
        cookies = ", ".join(
            f"<Cookie {c.name}={c.value} for {c.domain or '*'}{c.path}>" for c in self._live()
        )
        return f"Cookies([{cookies}])"


# -- request bodies -----------------------------------------------------------


def _json_default(obj: Any) -> Any:
    """``json.dumps`` fallback for common non-JSON types."""
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):  # Pydantic v2 models
        return model_dump(mode="json")
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if hasattr(obj, "__struct_fields__"):  # msgspec.Struct
        import importlib

        return importlib.import_module("msgspec").to_builtins(obj)
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if type(obj).__name__ in ("UUID", "Decimal", "PurePosixPath", "PosixPath", "WindowsPath"):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def json_dumps(obj: Any) -> bytes:
    """Compact UTF-8 JSON, with support for dataclasses, Pydantic and msgspec."""
    model_dump_json = getattr(obj, "model_dump_json", None)
    if callable(model_dump_json) and not isinstance(obj, type):
        dumped: str = model_dump_json()
        return dumped.encode("utf-8")
    return _json.dumps(
        obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_json_default
    ).encode("utf-8")


def _file_part(name: str, value: FileTypes) -> tuple[dict[str, str], bytes]:
    filename: str | None
    content_type: str | None = None
    extra_headers: Mapping[str, str] = {}
    if isinstance(value, tuple):
        if len(value) == 2:
            filename, content = value
        elif len(value) == 3:
            filename, content, content_type = value
        else:
            filename, content, content_type, extra_headers = value
    else:
        content = value
        raw_name = getattr(value, "name", None)
        filename = os.path.basename(raw_name) if isinstance(raw_name, str) else "upload"
    if isinstance(content, str):
        data = content.encode("utf-8")
    elif isinstance(content, (bytes, bytearray, memoryview)):
        data = bytes(content)
    else:
        # Typed as IO[bytes], but be forgiving about files opened in text mode.
        read: Any = content.read()
        data = read.encode("utf-8") if isinstance(read, str) else bytes(read)
    if content_type is None:
        content_type = (mimetypes.guess_type(filename)[0] if filename else None) or (
            "application/octet-stream"
        )
    disposition = f'form-data; name="{_escape_quoted(name)}"'
    if filename:
        disposition += f'; filename="{_escape_quoted(filename)}"'
    headers = {"Content-Disposition": disposition, "Content-Type": content_type}
    headers.update(extra_headers)
    return headers, data


def _escape_quoted(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "%22").replace("\r", "%0D").replace("\n", "%0A")


def encode_multipart(
    data: RequestData | None, files: RequestFiles, boundary: str | None = None
) -> tuple[bytes, str]:
    """Encode ``multipart/form-data``; returns ``(body, content_type)``."""
    boundary = boundary or os.urandom(16).hex()
    delimiter = f"--{boundary}\r\n".encode("ascii")
    out: list[bytes] = []
    if data:
        if isinstance(data, (str, bytes)):
            raise TypeError("`data` must be a mapping or a list of pairs when `files` is used")
        fields: Iterable[tuple[str, Any]] = data.items() if isinstance(data, Mapping) else data
        for key, value in fields:
            values = value if isinstance(value, (list, tuple)) else [value]
            for item in values:
                if item is None:
                    continue
                text = item if isinstance(item, (str, bytes)) else _primitive_to_str(item)
                payload = text.encode("utf-8") if isinstance(text, str) else text
                out.append(delimiter)
                out.append(
                    f'Content-Disposition: form-data; name="{_escape_quoted(key)}"\r\n\r\n'.encode()
                )
                out.append(payload)
                out.append(b"\r\n")
    file_items: Iterable[tuple[str, FileTypes]] = (
        files.items() if isinstance(files, Mapping) else files
    )
    for name, value in file_items:
        headers, payload = _file_part(name, value)
        out.append(delimiter)
        for header_name, header_value in headers.items():
            out.append(f"{header_name}: {header_value}\r\n".encode())
        out.append(b"\r\n")
        out.append(payload)
        out.append(b"\r\n")
    out.append(f"--{boundary}--\r\n".encode("ascii"))
    return b"".join(out), f"multipart/form-data; boundary={boundary}"


def encode_body(
    content: RequestContent | None,
    data: RequestData | None,
    json: Any,
    files: RequestFiles | None,
) -> tuple[bytes, str | None]:
    """Turn the ``content=`` / ``data=`` / ``json=`` / ``files=`` arguments into
    ``(body, content_type)``. At most one of content / json / (data, files) wins,
    in that order of precedence.
    """
    if content is not None:
        if isinstance(content, bytes):
            return content, None
        if isinstance(content, str):
            return content.encode("utf-8"), "text/plain; charset=utf-8"
        if isinstance(content, (bytearray, memoryview)):
            return bytes(content), None
        # Any other iterable of byte chunks is buffered (uploads are not
        # streamed in this release).
        return b"".join(content), None
    if json is not None:
        return json_dumps(json), "application/json"
    if files:
        return encode_multipart(data, files)
    if data is not None:
        if isinstance(data, bytes):
            return data, None
        if isinstance(data, str):
            return data.encode("utf-8"), "text/plain; charset=utf-8"
        pairs: list[tuple[str, str]] = []
        fields: Iterable[tuple[str, Any]] = data.items() if isinstance(data, Mapping) else data
        for key, value in fields:
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                pairs.extend((key, _primitive_to_str(v)) for v in value if v is not None)
            else:
                pairs.append((key, _primitive_to_str(value)))
        return urlencode(pairs).encode("ascii"), "application/x-www-form-urlencoded"
    return b"", None


def encode_cookie_pairs(cookies: CookieTypes | None) -> str | None:
    """Format request-scoped cookies as a ``Cookie`` header value."""
    if not cookies:
        return None
    items: Iterable[tuple[str, str]] = cookies.items() if isinstance(cookies, Mapping) else cookies
    return "; ".join(f"{name}={value}" for name, value in items) or None


def coerce_timeout(timeout: TimeoutTypes) -> Timeout | None:
    """Normalise a user-supplied timeout: ``None`` disables every timeout."""
    if timeout is None:
        return None
    if isinstance(timeout, Timeout):
        return timeout
    return Timeout(timeout)


# -- Request ------------------------------------------------------------------


class Request:
    """An HTTP request.

    Usually created through ``client.build_request()`` or the request helpers,
    but can be constructed directly and passed to ``client.send()`` or
    ``client.gather()``; client defaults (base URL, default headers, timeout,
    auth) are then applied at send time without overriding explicit values.
    """

    __slots__ = (
        "_cookie_base",
        "_extensions",
        "_prepared",
        "content",
        "headers",
        "method",
        "timeout",
        "url",
    )

    def __init__(
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
        timeout: TimeoutTypes | _Unset = UNSET,
        extensions: dict[str, Any] | None = None,
    ) -> None:
        self.method: str = method.upper()
        self.url: URL = (
            URL(url, params=params) if params else (url if isinstance(url, URL) else URL(url))
        )
        self.headers: Headers = Headers(headers)
        body, content_type = encode_body(content, data, json, files)
        self.content: bytes = body
        if content_type is not None and "content-type" not in self.headers:
            self.headers["content-type"] = content_type
        if body and "content-length" not in self.headers:
            self.headers["content-length"] = str(len(body))
        self.timeout: Timeout | _Unset | None = (
            UNSET if isinstance(timeout, _Unset) else coerce_timeout(timeout)
        )
        explicit = self.headers.get("cookie")
        if explicit is not None:
            del self.headers["cookie"]
        pairs = encode_cookie_pairs(cookies)
        self._cookie_base: str | None = "; ".join(p for p in (explicit, pairs) if p) or None
        self._extensions: dict[str, Any] | None = extensions
        self._prepared: bool = False

    @classmethod
    def _build(
        cls,
        method: str,
        url: URL,
        headers: Headers,
        content: bytes,
        timeout: Timeout | None,
        cookie_base: str | None,
    ) -> Request:
        """Fast internal constructor for requests assembled by a client."""
        self = cls.__new__(cls)
        self.method = method
        self.url = url
        self.headers = headers
        self.content = content
        self.timeout = timeout
        self._cookie_base = cookie_base
        self._extensions = None
        self._prepared = True
        return self

    @property
    def extensions(self) -> dict[str, Any]:
        """Free-form per-request storage for hooks and middleware."""
        if self._extensions is None:
            self._extensions = {}
        return self._extensions

    def __repr__(self) -> str:
        return f"<Request [{self.method} {self.url}]>"


# -- Response -----------------------------------------------------------------


class Elapsed(datetime.timedelta):
    """Request duration, with per-phase timings in seconds as attributes.

    A ``timedelta`` (so ``response.elapsed.total_seconds()`` works as it does in
    requests and httpx) carrying:

    * ``total``    - whole exchange; for streamed responses, until headers arrived
    * ``pool``     - waiting for a free connection slot
    * ``connect``  - DNS + TCP + TLS; ``None`` when a pooled connection was reused
    * ``ttfb``     - request sent until response headers received
    * ``download`` - reading the body; ``None`` for streamed responses

    Phase timings are ``None`` when the transport cannot measure them
    (mock / ASGI / WSGI transports).
    """

    pool: float | None
    connect: float | None
    ttfb: float | None
    download: float | None

    def __new__(
        cls,
        total: float = 0.0,
        pool: float | None = None,
        connect: float | None = None,
        ttfb: float | None = None,
        download: float | None = None,
    ) -> Self:
        self = super().__new__(cls, seconds=total)
        self.pool = pool
        self.connect = connect
        self.ttfb = ttfb
        self.download = download
        return self

    @property
    def total(self) -> float:
        return self.total_seconds()

    def as_dict(self) -> dict[str, float | None]:
        return {
            "total": self.total,
            "pool": self.pool,
            "connect": self.connect,
            "ttfb": self.ttfb,
            "download": self.download,
        }

    def __repr__(self) -> str:
        parts = ", ".join(
            f"{name}={value * 1000:.3f}ms"
            for name, value in self.as_dict().items()
            if value is not None
        )
        return f"Elapsed({parts})"


_CHARSET_RE: Final = re.compile(r"charset\s*=\s*\"?([^\s;\"]+)", re.IGNORECASE)
_LINE_BREAK_RE: Final = re.compile(r"\r\n|\n|\r")


def _charset_from(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = _CHARSET_RE.search(content_type)
    if match is None:
        return None
    try:
        return codecs.lookup(match.group(1)).name
    except LookupError:
        return None


def _rechunk(chunks: Iterable[bytes], size: int) -> Iterator[bytes]:
    buffer = bytearray()
    for chunk in chunks:
        buffer += chunk
        while len(buffer) >= size:
            yield bytes(buffer[:size])
            del buffer[:size]
    if buffer:
        yield bytes(buffer)


class _LineSplitter:
    """Incrementally split text on CRLF / LF / CR."""

    __slots__ = ("_buffer",)

    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, text: str) -> list[str]:
        buffer = self._buffer + text
        # A trailing CR may be the first half of a CRLF; hold it back.
        held = ""
        if buffer.endswith("\r"):
            buffer, held = buffer[:-1], "\r"
        parts = _LINE_BREAK_RE.split(buffer)
        self._buffer = parts.pop() + held
        return parts

    def flush(self) -> list[str]:
        rest, self._buffer = self._buffer, ""
        if rest.endswith("\r"):
            rest = rest[:-1]
            return [rest]
        return [rest] if rest else []


class Response:
    """An HTTP response.

    For normal requests the body is already in memory: use :attr:`content`,
    :attr:`text` or :meth:`json`. For streamed responses (``client.stream()``),
    iterate with ``iter_bytes`` / ``iter_lines`` / ``iter_sse`` (or their
    ``aiter_*`` twins) or call ``read()`` / ``aread()`` to buffer the body.
    """

    __slots__ = (
        "_closed",
        "_content",
        "_encoding",
        "_exhausted",
        "_handle",
        "_stream",
        "_stream_consumed",
        "_text",
        "_timings",
        "headers",
        "history",
        "http_version",
        "request",
        "retry_count",
        "status_code",
    )

    def __init__(
        self,
        status_code: int = 200,
        *,
        headers: HeaderTypes | None = None,
        content: bytes | str | None = None,
        text: str | None = None,
        json: Any = None,
        stream: Iterable[bytes] | AsyncIterable[bytes] | None = None,
        request: Request | None = None,
        http_version: str = "HTTP/1.1",
    ) -> None:
        self.status_code: int = int(status_code)
        self.headers: Headers = Headers(headers)
        self.http_version: str = http_version
        self.request: Request | None = request
        self.history: list[Response] = []
        self.retry_count: int = 0
        self._handle: ResponseStream | None = None
        self._stream: Iterable[bytes] | AsyncIterable[bytes] | None = None
        self._stream_consumed: bool = False
        self._exhausted: bool = False
        self._closed: bool = False
        self._encoding: str | None = None
        self._text: str | None = None
        self._timings: tuple[float, float | None, float | None, float | None, float | None] | None
        self._timings = None
        self._content: bytes | None

        default_type: str | None = None
        if json is not None:
            self._content = json_dumps(json)
            default_type = "application/json"
        elif text is not None:
            self._content = text.encode("utf-8")
            default_type = "text/plain; charset=utf-8"
        elif content is not None:
            self._content = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        elif stream is not None:
            self._content = None
            self._stream = stream
        else:
            self._content = b""
        if default_type is not None and "content-type" not in self.headers:
            self.headers["content-type"] = default_type

    @classmethod
    def _from_raw(
        cls, raw: tuple[int, str, list[tuple[str, str]], Any, Any], request: Request
    ) -> Response:
        """Build from the tuple returned by the Rust engine (internal)."""
        self = cls.__new__(cls)
        body: Any
        self.status_code, self.http_version, items, body, self._timings = raw
        self.headers = Headers._from_raw(items)
        self.request = request
        self.history = []
        self.retry_count = 0
        self._stream = None
        self._stream_consumed = False
        self._exhausted = False
        self._closed = False
        self._encoding = None
        self._text = None
        if type(body) is bytes:
            self._content = body
            self._handle = None
        else:
            self._content = None
            self._handle = body
        return self

    # -- metadata ---------------------------------------------------------

    @property
    def url(self) -> URL:
        """The URL that produced this response (after redirects)."""
        return self.request.url if self.request is not None else URL("")

    @property
    def reason_phrase(self) -> str:
        try:
            return HTTPStatus(self.status_code).phrase
        except ValueError:
            return ""

    @property
    def is_informational(self) -> bool:
        return 100 <= self.status_code < 200

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def is_redirect(self) -> bool:
        return self.status_code in (301, 302, 303, 307, 308) and "location" in self.headers

    @property
    def is_client_error(self) -> bool:
        return 400 <= self.status_code < 500

    @property
    def is_server_error(self) -> bool:
        return 500 <= self.status_code < 600

    @property
    def is_error(self) -> bool:
        return 400 <= self.status_code < 600

    @property
    def ok(self) -> bool:
        """``True`` when the status is below 400 (requests-compatible)."""
        return self.status_code < 400

    @property
    def elapsed(self) -> Elapsed:
        """How long the exchange took, with per-phase timings as attributes."""
        timings = self._timings
        return Elapsed(*timings) if timings is not None else Elapsed(0.0)

    @property
    def cookies(self) -> dict[str, str]:
        """Cookies set by this response (``name -> value``)."""
        out: dict[str, str] = {}
        for header in self.headers.get_list("set-cookie"):
            parsed = parse_set_cookie(header)
            if parsed is not None:
                out[parsed[0]] = parsed[1]
        return out

    @property
    def encoding(self) -> str:
        """Text encoding: explicit, else ``charset`` from Content-Type, else UTF-8."""
        if self._encoding is None:
            self._encoding = _charset_from(self.headers.get("content-type")) or "utf-8"
        return self._encoding

    @encoding.setter
    def encoding(self, value: str) -> None:
        self._encoding = value
        self._text = None

    @property
    def is_stream_consumed(self) -> bool:
        return self._stream_consumed

    @property
    def is_closed(self) -> bool:
        return self._closed

    # -- buffered body ----------------------------------------------------

    @property
    def content(self) -> bytes:
        """The response body as bytes."""
        content = self._content
        if content is None:
            raise ResponseNotRead()
        return content

    @property
    def text(self) -> str:
        """The response body decoded to ``str`` (undecodable bytes are replaced)."""
        text = self._text
        if text is None:
            content = self.content
            try:
                text = content.decode(self.encoding, errors="replace")
            except LookupError:
                text = content.decode("utf-8", errors="replace")
            self._text = text
        return text

    @overload
    def json(self, *, model: None = None, **kwargs: Any) -> Any: ...
    @overload
    def json(self, *, model: type[T], **kwargs: Any) -> T: ...
    def json(self, *, model: Any = None, **kwargs: Any) -> Any:
        """Parse the body as JSON.

        With ``model=`` the payload is validated into that type - a Pydantic v2
        model, a dataclass, a ``msgspec.Struct``, or a generic such as
        ``list[User]`` - and a :class:`~nextreq.DecodeError` carrying the status
        and URL is raised if it does not fit.
        """
        if model is None:
            return _json.loads(self.content, **kwargs)
        from .parsing import parse_response

        return parse_response(self, model)

    def raise_for_status(self) -> Self:
        """Raise :class:`~nextreq.HTTPStatusError` for 4xx / 5xx responses."""
        status = self.status_code
        if status < 400:
            return self
        kind = "Client error" if status < 500 else "Server error"
        message = f"{kind} '{status} {self.reason_phrase}' for url '{self.url}'"
        raise HTTPStatusError(message, request=self.request, response=self)

    # -- streaming --------------------------------------------------------

    def _begin_stream(self) -> None:
        if self._exhausted:
            raise StreamConsumed()
        if self._closed:
            raise StreamClosed()
        if self._stream_consumed:
            raise StreamConsumed()
        self._stream_consumed = True

    def read(self) -> bytes:
        """Read and buffer the whole body, then return it."""
        if self._content is None:
            if self._handle is not None:
                self._begin_stream()
                try:
                    self._content = self._handle.read_all()
                finally:
                    self.close()
            else:
                self._content = b"".join(self.iter_bytes())
        return self._content

    def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """Iterate over the (decompressed) body as it arrives."""
        if chunk_size is not None:
            if chunk_size <= 0:
                raise ValueError("chunk_size must be a positive integer")
            yield from _rechunk(self.iter_bytes(), chunk_size)
            return
        content = self._content
        if content is not None:
            if content:
                yield content
            return
        self._begin_stream()
        try:
            handle = self._handle
            if handle is not None:
                read = handle.read
                while (chunk := read()) is not None:
                    yield chunk
            elif self._stream is not None:
                stream = self._stream
                if not isinstance(stream, Iterable):
                    raise RuntimeError(
                        "This response has an async byte stream; use `aiter_bytes()` / `aread()`."
                    )
                for chunk in stream:
                    if chunk:
                        yield chunk
            self._exhausted = True
        finally:
            self.close()

    def iter_text(self) -> Iterator[str]:
        """Iterate over the body decoded to text."""
        decoder = codecs.getincrementaldecoder(self.encoding)(errors="replace")
        for chunk in self.iter_bytes():
            text = decoder.decode(chunk)
            if text:
                yield text
        tail = decoder.decode(b"", True)
        if tail:
            yield tail

    def iter_lines(self) -> Iterator[str]:
        """Iterate over the body line by line (line endings stripped)."""
        splitter = _LineSplitter()
        for text in self.iter_text():
            yield from splitter.feed(text)
        yield from splitter.flush()

    def iter_sse(self) -> Iterator[SSEEvent]:
        """Iterate over Server-Sent Events.

        Parsing (WHATWG event-stream rules) and ``SSEEvent`` construction both
        happen in the Rust core, in batches, with the GIL released while
        waiting for data.
        """
        if self._content is None and self._handle is not None:
            self._begin_stream()
            try:
                read_events = self._handle.read_events
                while (batch := read_events()) is not None:
                    yield from batch
                self._exhausted = True
            finally:
                self.close()
            return
        parser = SSEParser()
        for chunk in self.iter_bytes():
            yield from parser.feed(chunk)

    def close(self) -> None:
        """Release the connection. Safe to call repeatedly."""
        if self._closed:
            return
        self._closed = True
        handle = self._handle
        if handle is not None:
            handle.close()
        stream = self._stream
        if stream is not None:
            closer = getattr(stream, "close", None)
            if callable(closer):
                closer()

    # -- async streaming --------------------------------------------------

    async def aread(self) -> bytes:
        """Read and buffer the whole body, then return it."""
        if self._content is None:
            if self._handle is not None:
                self._begin_stream()
                try:
                    self._content = await self._handle.aread_all()
                finally:
                    await self.aclose()
            else:
                self._content = b"".join([chunk async for chunk in self.aiter_bytes()])
        return self._content

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        """Iterate over the (decompressed) body as it arrives."""
        if chunk_size is not None:
            if chunk_size <= 0:
                raise ValueError("chunk_size must be a positive integer")
            buffer = bytearray()
            async for chunk in self.aiter_bytes():
                buffer += chunk
                while len(buffer) >= chunk_size:
                    yield bytes(buffer[:chunk_size])
                    del buffer[:chunk_size]
            if buffer:
                yield bytes(buffer)
            return
        content = self._content
        if content is not None:
            if content:
                yield content
            return
        self._begin_stream()
        try:
            handle = self._handle
            if handle is not None:
                aread = handle.aread
                while (piece := await aread()) is not None:
                    yield piece
            elif self._stream is not None:
                stream = self._stream
                if isinstance(stream, AsyncIterable):
                    async for chunk in stream:
                        if chunk:
                            yield chunk
                else:
                    for chunk in stream:
                        if chunk:
                            yield chunk
            self._exhausted = True
        finally:
            await self.aclose()

    async def aiter_text(self) -> AsyncIterator[str]:
        """Iterate over the body decoded to text."""
        decoder = codecs.getincrementaldecoder(self.encoding)(errors="replace")
        async for chunk in self.aiter_bytes():
            text = decoder.decode(chunk)
            if text:
                yield text
        tail = decoder.decode(b"", True)
        if tail:
            yield tail

    async def aiter_lines(self) -> AsyncIterator[str]:
        """Iterate over the body line by line (line endings stripped)."""
        splitter = _LineSplitter()
        async for text in self.aiter_text():
            for line in splitter.feed(text):
                yield line
        for line in splitter.flush():
            yield line

    async def aiter_sse(self) -> AsyncIterator[SSEEvent]:
        """Iterate over Server-Sent Events, parsed by the Rust core."""
        if self._content is None and self._handle is not None:
            self._begin_stream()
            try:
                aread_events = self._handle.aread_events
                while (batch := await aread_events()) is not None:
                    for event in batch:
                        yield event
                self._exhausted = True
            finally:
                await self.aclose()
            return
        parser = SSEParser()
        async for chunk in self.aiter_bytes():
            for event in parser.feed(chunk):
                yield event

    async def aclose(self) -> None:
        """Release the connection. Safe to call repeatedly."""
        if self._closed:
            return
        self._closed = True
        handle = self._handle
        if handle is not None:
            handle.close()
        stream = self._stream
        if stream is not None:
            closer = getattr(stream, "aclose", None) or getattr(stream, "close", None)
            if callable(closer):
                result = closer()
                if hasattr(result, "__await__"):
                    await result

    # -- context managers -------------------------------------------------

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"<Response [{self.status_code} {self.reason_phrase}]>"
