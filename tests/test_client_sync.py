"""NXRQ-201 / NXRQ-301: the synchronous client against a real local server."""

from __future__ import annotations

import base64
import importlib.metadata
import io
import signal
import sys
import threading
import time
from typing import Any

import pytest
from packaging.version import Version

import nextreq
from nextreq import URL, Cookies, Headers, Request, Response

from .conftest import COMPRESSIBLE, LiveServer, TLSServer, brotli_compress, zstd_compress

# -- basics -------------------------------------------------------------------


def test_version_and_exports() -> None:
    # The version is defined once, in Cargo.toml. Compare against the installed
    # package metadata instead of a number hardcoded here, so a release bump
    # cannot break the suite.
    assert Version(nextreq.__version__) == Version(importlib.metadata.version("nextreq"))
    assert Version(nextreq.__version__) >= Version("0.1.0")
    for name in nextreq.__all__:
        assert hasattr(nextreq, name), name
    assert isinstance(nextreq.HTTP3_ENABLED, bool)


def test_get_returns_response(client: nextreq.Client) -> None:
    response = client.get("/json")
    assert isinstance(response, Response)
    assert response.status_code == 200
    assert response.reason_phrase == "OK"
    assert response.is_success and response.ok and not response.is_error
    assert response.json() == {"hello": "world"}
    assert response.text == '{"hello": "world"}'
    assert response.content == b'{"hello": "world"}'
    assert response.http_version == "HTTP/1.1"
    assert response.headers["Content-Type"].startswith("application/json")
    assert response.encoding == "utf-8"
    assert str(response.url).endswith("/json")
    assert repr(response) == "<Response [200 OK]>"
    assert response.request is not None and response.request.method == "GET"


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
def test_all_verbs(client: nextreq.Client, method: str) -> None:
    response = getattr(client, method.lower())("/echo")
    assert response.status_code == 200
    assert response.json()["method"] == method
    assert client.request(method, "/echo").json()["method"] == method


def test_head_has_no_body(client: nextreq.Client) -> None:
    response = client.head("/echo")
    assert response.status_code == 200
    assert response.content == b""


def test_module_level_helpers(server: LiveServer) -> None:
    assert nextreq.get(f"{server.url}/json").json() == {"hello": "world"}
    assert nextreq.post(f"{server.url}/echo", json={"a": 1}).json()["json"] == {"a": 1}
    assert nextreq.put(f"{server.url}/echo", content=b"x").json()["method"] == "PUT"
    assert nextreq.patch(f"{server.url}/echo").json()["method"] == "PATCH"
    assert nextreq.delete(f"{server.url}/echo").json()["method"] == "DELETE"
    assert nextreq.head(f"{server.url}/echo").status_code == 200
    assert nextreq.options(f"{server.url}/echo").json()["method"] == "OPTIONS"
    assert nextreq.request("GET", f"{server.url}/echo", params={"q": "1"}).json()["query"] == {
        "q": ["1"]
    }
    with nextreq.stream("GET", f"{server.url}/stream/3") as response:
        assert b"".join(response.iter_bytes()) == b"chunk-0;chunk-1;chunk-2;"


def test_module_level_helpers_reuse_connections_but_not_cookies(server: LiveServer) -> None:
    for _ in range(5):
        nextreq.get(f"{server.url}/json")
    assert len(server.peers) == 1, "top-level helpers should share a keep-alive pool"

    assert nextreq.get(f"{server.url}/cookies/set?session=abc").json() == {
        "cookies": {"session": "abc"}
    }
    # ...but a later, unrelated call must not see that cookie.
    assert nextreq.get(f"{server.url}/cookies").json() == {"cookies": {}}


# -- request construction -----------------------------------------------------


def test_base_url_joining(server: LiveServer) -> None:
    with nextreq.Client(base_url=f"{server.url}/echo/") as client:
        assert client.base_url == f"{server.url}/echo"
        assert client.get("").json()["path"] == "/echo"
    with nextreq.Client(base_url=server.url) as client:
        assert client.get("echo").json()["path"] == "/echo"
        assert client.get("/echo").json()["path"] == "/echo"
        # Absolute URLs bypass the base URL.
        assert client.get(f"{server.url}/json").json() == {"hello": "world"}


def test_params_are_merged_and_encoded(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, params={"api_key": "k"}) as client:
        query = client.get(
            "/echo?existing=1",
            params={"q": "a b&c", "n": 3, "flag": True, "skip": None, "multi": ["x", "y"]},
        ).json()["query"]
    assert query == {
        "existing": ["1"],
        "api_key": ["k"],
        "q": ["a b&c"],
        "n": ["3"],
        "flag": ["true"],
        "multi": ["x", "y"],
    }


def test_params_as_pairs_and_string(client: nextreq.Client) -> None:
    assert client.get("/echo", params=[("a", "1"), ("a", "2")]).json()["query"] == {"a": ["1", "2"]}
    assert client.get("/echo", params="x=1&y=2").json()["query"] == {"x": ["1"], "y": ["2"]}


def test_default_and_custom_headers(server: LiveServer) -> None:
    with nextreq.Client(
        base_url=server.url, headers={"X-Client": "one", "X-Shared": "c"}
    ) as client:
        headers = client.get("/echo", headers={"X-Request": "two", "x-shared": "r"}).json()[
            "headers"
        ]
    assert headers["x-client"] == "one"
    assert headers["x-request"] == "two"
    assert headers["x-shared"] == "r", "request headers override client headers"
    assert headers["user-agent"] == f"nextreq/{nextreq.__version__}"
    assert headers["accept"] == "*/*"
    assert headers["accept-encoding"] == "gzip, deflate, br, zstd"


def test_client_headers_are_mutable(client: nextreq.Client) -> None:
    client.headers["X-Late"] = "yes"
    client.headers["User-Agent"] = "custom/1.0"
    headers = client.get("/echo").json()["headers"]
    assert headers["x-late"] == "yes"
    assert headers["user-agent"] == "custom/1.0"


def test_json_body(client: nextreq.Client) -> None:
    echoed = client.post("/echo", json={"name": "Zoë", "n": [1, 2, 3]}).json()
    assert echoed["json"] == {"name": "Zoë", "n": [1, 2, 3]}
    assert echoed["headers"]["content-type"] == "application/json"
    assert echoed["body"] == '{"name":"Zoë","n":[1,2,3]}'


def test_form_body(client: nextreq.Client) -> None:
    echoed = client.post("/echo", data={"a": "1", "b": ["x", "y"], "c": 5, "skip": None}).json()
    assert echoed["headers"]["content-type"] == "application/x-www-form-urlencoded"
    assert echoed["body"] == "a=1&b=x&b=y&c=5"


def test_raw_content_bodies(client: nextreq.Client) -> None:
    assert (
        client.post("/echo", content=b"\x00\x01\xff").json()["body_b64"]
        == base64.b64encode(b"\x00\x01\xff").decode()
    )
    text = client.post("/echo", content="héllo").json()
    assert text["body"] == "héllo"
    assert text["headers"]["content-type"] == "text/plain; charset=utf-8"
    assert client.post("/echo", content=iter([b"ab", b"cd"])).json()["body"] == "abcd"
    assert client.post("/echo", content=bytearray(b"xyz")).json()["body"] == "xyz"
    assert client.post("/echo", data=b"raw-bytes").json()["body"] == "raw-bytes"


def test_explicit_content_type_wins(client: nextreq.Client) -> None:
    echoed = client.post(
        "/echo", content=b"<a/>", headers={"Content-Type": "application/xml"}
    ).json()
    assert echoed["headers"]["content-type"] == "application/xml"


def test_multipart_upload(client: nextreq.Client) -> None:
    result = client.post(
        "/upload",
        data={"title": "Report", "count": 2},
        files={
            "plain": b"just bytes",
            "named": ("notes.txt", b"hello notes"),
            "typed": ("data.bin", b"\x01\x02", "application/x-custom"),
            "stream": io.BytesIO(b"from a file object"),
        },
    ).json()
    assert result["fields"] == {"title": "Report", "count": "2"}
    assert result["files"]["plain"]["size"] == 10
    assert result["files"]["named"] == {
        "filename": "notes.txt",
        "content_type": "text/plain",
        "size": 11,
        "content": "hello notes",
    }
    assert result["files"]["typed"]["content_type"] == "application/x-custom"
    assert result["files"]["stream"]["content"] == "from a file object"


def test_large_upload_and_download_roundtrip(client: nextreq.Client) -> None:
    payload = bytes(range(256)) * 8192  # 2 MiB: exercises the tracked-body path
    echoed = client.post("/echo", content=payload).json()
    assert echoed["body_length"] == len(payload)
    assert base64.b64decode(echoed["body_b64"]) == payload

    big = client.get("/large", params={"size": 3_000_000})
    assert len(big.content) == 3_000_000


def test_build_request_and_send(client: nextreq.Client) -> None:
    request = client.build_request("POST", "/echo", json={"k": "v"}, headers={"X-A": "1"})
    assert isinstance(request, Request)
    assert request.method == "POST"
    assert request.url.path == "/echo"
    assert request.headers["content-length"] == "9"
    assert repr(request).startswith("<Request [POST http://127.0.0.1")
    assert client.send(request).json()["json"] == {"k": "v"}


def test_send_user_constructed_request_gets_client_defaults(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, headers={"X-Default": "d"}, auth=("u", "p")) as client:
        echoed = client.send(Request("POST", "/echo", json=[1], headers={"X-Own": "o"})).json()
    assert echoed["json"] == [1]
    assert echoed["headers"]["x-default"] == "d"
    assert echoed["headers"]["x-own"] == "o"
    assert echoed["headers"]["authorization"].startswith("Basic ")
    assert echoed["headers"]["user-agent"].startswith("nextreq/")


# -- cookies ------------------------------------------------------------------


def test_cookie_jar_persists_across_requests(client: nextreq.Client) -> None:
    assert client.get("/cookies").json() == {"cookies": {}}
    assert client.get("/cookies/set?session=abc&theme=dark").json() == {
        "cookies": {"session": "abc", "theme": "dark"}
    }
    assert client.cookies["session"] == "abc"
    assert dict(client.cookies) == {"session": "abc", "theme": "dark"}
    assert client.get("/cookies").json()["cookies"]["theme"] == "dark"

    del client.cookies["session"]
    assert client.get("/cookies").json() == {"cookies": {"theme": "dark"}}


def test_client_and_request_cookies(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, cookies={"a": "1"}) as client:
        assert client.get("/cookies").json() == {"cookies": {"a": "1"}}
        assert client.get("/cookies", cookies={"b": "2"}).json() == {
            "cookies": {"a": "1", "b": "2"}
        }
        # Request-scoped cookies are not persisted.
        assert client.get("/cookies").json() == {"cookies": {"a": "1"}}
        assert client.get("/cookies", headers={"Cookie": "c=3"}).json() == {
            "cookies": {"a": "1", "c": "3"}
        }


def test_response_cookies_property(client: nextreq.Client) -> None:
    response = client.get("/cookies/set?x=1", follow_redirects=False)
    assert response.status_code == 302
    assert response.cookies == {"x": "1"}


def test_cookie_jar_scoping_rules() -> None:
    jar = Cookies()
    jar.extract(
        URL("https://app.example.com/account/settings"),
        [
            "host=only",
            "wide=1; Domain=example.com; Path=/",
            "deep=1; Path=/account",
            "tls=1; Secure",
            "evil=1; Domain=attacker.test",
            "tld=1; Domain=com",
            "gone=1; Max-Age=0",
            "old=1; Expires=Wed, 21 Oct 2015 07:28:00 GMT",
            'quoted="q v"',
        ],
    )
    assert jar.header_for(URL("https://app.example.com/account/settings/x")) is not None
    sent = jar.header_for(URL("https://app.example.com/account/x")) or ""
    assert set(sent.split("; ")) == {"host=only", "wide=1", "deep=1", "tls=1", "quoted=q v"}

    # Parent-domain cookie goes to siblings; host-only cookies do not.
    assert jar.header_for(URL("https://other.example.com/")) == "wide=1"
    # Secure cookies never travel over plain HTTP.
    plain = jar.header_for(URL("http://app.example.com/account")) or ""
    assert set(plain.split("; ")) == {"host=only", "wide=1", "deep=1", "quoted=q v"}
    # Path scoping: cookies default to the directory of the URL that set them,
    # and "/accounting" is not inside "/account".
    assert jar.header_for(URL("https://app.example.com/other")) == "wide=1"
    assert jar.header_for(URL("https://app.example.com/accounting")) == "wide=1"
    # Nothing leaks to unrelated hosts, including look-alike suffixes.
    assert jar.header_for(URL("https://notexample.com/")) is None
    assert "evil" not in jar and "tld" not in jar and "gone" not in jar and "old" not in jar

    jar.set("manual", "m")
    assert jar.header_for(URL("https://anything.test/")) == "manual=m"
    jar.clear()
    assert len(jar) == 0 and not jar


# -- auth ---------------------------------------------------------------------


def test_basic_auth_tuple_and_class(server: LiveServer) -> None:
    url = f"{server.url}/basic-auth/alice/s3cret"
    assert nextreq.get(url).status_code == 401
    assert nextreq.get(url, auth=("alice", "s3cret")).json() == {"authenticated": True}
    assert nextreq.get(url, auth=nextreq.BasicAuth("alice", "s3cret")).status_code == 200
    with nextreq.Client(auth=("alice", "s3cret")) as client:
        assert client.get(url).status_code == 200
        assert client.get(url, auth=None).status_code == 401, "auth=None disables client auth"
        assert client.get(url, auth=("alice", "wrong")).status_code == 401


def test_bearer_and_callable_auth(client: nextreq.Client) -> None:
    headers = client.get("/echo", auth=nextreq.BearerAuth("tok")).json()["headers"]
    assert headers["authorization"] == "Bearer tok"

    def sign(request: Request) -> None:
        request.headers["X-Signature"] = f"sig:{request.method}:{request.url.path}"

    assert client.get("/echo", auth=sign).json()["headers"]["x-signature"] == "sig:GET:/echo"


def test_userinfo_in_url_becomes_basic_auth(server: LiveServer) -> None:
    url = f"http://alice:s3cret@127.0.0.1:{server.port}/basic-auth/alice/s3cret"
    response = nextreq.get(url)
    assert response.json() == {"authenticated": True}
    assert "@" not in str(response.url), "credentials must be stripped from the URL"
    assert response.request is not None
    assert response.request.headers["authorization"].startswith("Basic ")


# -- redirects ----------------------------------------------------------------


def test_redirects_are_followed_with_history(client: nextreq.Client) -> None:
    response = client.get("/redirect/3")
    assert response.status_code == 200
    assert response.json()["path"] == "/echo"
    assert [r.status_code for r in response.history] == [302, 302, 302]
    assert response.history[0].url.path == "/redirect/3"
    assert response.url.path == "/echo"


def test_follow_redirects_can_be_disabled(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, follow_redirects=False) as client:
        response = client.get("/redirect/1")
        assert response.status_code == 302 and response.is_redirect
        assert response.headers["location"] == "/echo"
        assert client.get("/redirect/1", follow_redirects=True).status_code == 200
    with nextreq.Client(base_url=server.url) as client:
        assert client.get("/redirect/1", follow_redirects=False).status_code == 302


def test_ten_redirects_allowed_eleven_rejected(client: nextreq.Client) -> None:
    assert len(client.get("/redirect/10").history) == 10
    with pytest.raises(nextreq.TooManyRedirects) as info:
        client.get("/redirect/11")
    assert isinstance(info.value, nextreq.TransportError)
    assert info.value.request is not None


def test_max_redirects_is_configurable(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, max_redirects=2) as client:
        assert client.get("/redirect/2").status_code == 200
        with pytest.raises(nextreq.TooManyRedirects):
            client.get("/redirect/3")


@pytest.mark.parametrize(
    ("status", "expected_method", "body_kept"),
    [
        (301, "GET", False),
        (302, "GET", False),
        (303, "GET", False),
        (307, "POST", True),
        (308, "POST", True),
    ],
)
def test_redirect_method_and_body_semantics(
    client: nextreq.Client, status: int, expected_method: str, body_kept: bool
) -> None:
    echoed = client.post("/redirect/1", params={"status": status}, json={"keep": "me"}).json()
    assert echoed["method"] == expected_method
    assert (echoed["json"] == {"keep": "me"}) is body_kept
    assert ("content-type" in echoed["headers"]) is body_kept


def test_cross_origin_redirect_strips_credentials(server: LiveServer) -> None:
    # "localhost" and "127.0.0.1" are different origins served by the same server.
    other_origin = f"http://localhost:{server.port}/echo"
    with nextreq.Client(base_url=server.url) as client:
        echoed = client.get(
            "/redirect-to",
            params={"url": other_origin},
            headers={"Authorization": "Bearer secret", "X-Keep": "1"},
            cookies={"request_scoped": "1"},
        ).json()
        assert "authorization" not in echoed["headers"]
        assert "cookie" not in echoed["headers"]
        assert echoed["headers"]["x-keep"] == "1"

        same_origin = client.get(
            "/redirect-to",
            params={"url": "/echo"},
            headers={"Authorization": "Bearer secret"},
        ).json()
        assert same_origin["headers"]["authorization"] == "Bearer secret"


def test_redirect_to_unsupported_scheme(client: nextreq.Client) -> None:
    with pytest.raises(nextreq.UnsupportedProtocol):
        client.get("/redirect-to", params={"url": "ftp://example.com/file"})


# -- status handling ----------------------------------------------------------


def test_raise_for_status_method(client: nextreq.Client) -> None:
    ok = client.get("/status/204")
    assert ok.raise_for_status() is ok

    response = client.get("/status/404")
    assert response.is_client_error and not response.ok
    with pytest.raises(nextreq.HTTPStatusError) as info:
        response.raise_for_status()
    error = info.value
    assert error.response is response
    assert error.status_code == 404
    assert error.request is response.request
    assert "404 Not Found" in str(error) and "/status/404" in str(error)
    assert isinstance(error, nextreq.NextReqError) and not isinstance(error, nextreq.TransportError)

    with pytest.raises(nextreq.HTTPStatusError, match="Server error '503"):
        client.get("/status/503").raise_for_status()


def test_raise_for_status_client_and_request_level(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url) as lenient:
        assert lenient.get("/status/500").status_code == 500
        with pytest.raises(nextreq.HTTPStatusError):
            lenient.get("/status/500", raise_for_status=True)
    with nextreq.Client(base_url=server.url, raise_for_status=True) as strict:
        with pytest.raises(nextreq.HTTPStatusError):
            strict.get("/status/418")
        assert strict.get("/status/418", raise_for_status=False).status_code == 418
        assert strict.get("/status/200").status_code == 200


# -- decompression ------------------------------------------------------------


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br", "zstd"])
def test_automatic_decompression(client: nextreq.Client, encoding: str) -> None:
    if encoding == "br" and brotli_compress(b"x") is None:
        pytest.skip("brotli not installed")
    if encoding == "zstd" and zstd_compress(b"x") is None:
        pytest.skip("no zstd encoder available")
    response = client.get(f"/encoded/{encoding}")
    assert response.status_code == 200
    assert response.content == COMPRESSIBLE
    assert response.json()["items"][:3] == [0, 1, 2]


def test_corrupt_compressed_body_raises_decoding_error(client: nextreq.Client) -> None:
    with pytest.raises(nextreq.DecodingError) as info:
        client.get("/corrupt-gzip")
    assert isinstance(info.value, nextreq.TransportError)


# -- streaming ----------------------------------------------------------------


def test_stream_iter_bytes(client: nextreq.Client) -> None:
    with client.stream("GET", "/stream/5") as response:
        assert response.status_code == 200
        with pytest.raises(nextreq.ResponseNotRead):
            _ = response.content
        chunks = list(response.iter_bytes())
    assert b"".join(chunks) == b"chunk-0;chunk-1;chunk-2;chunk-3;chunk-4;"
    assert len(chunks) > 1, "body should arrive incrementally"
    assert response.is_closed and response.is_stream_consumed
    with pytest.raises(nextreq.StreamConsumed):
        list(response.iter_bytes())


def test_stream_read_then_reuse(client: nextreq.Client) -> None:
    with client.stream("GET", "/stream/3") as response:
        assert response.read() == b"chunk-0;chunk-1;chunk-2;"
        assert response.content == b"chunk-0;chunk-1;chunk-2;"
        assert response.text.startswith("chunk-0")
        assert list(response.iter_bytes()) == [b"chunk-0;chunk-1;chunk-2;"]


def test_stream_chunk_size(client: nextreq.Client) -> None:
    with client.stream("GET", "/stream/4") as response:
        chunks = list(response.iter_bytes(chunk_size=5))
    assert all(len(c) == 5 for c in chunks[:-1])
    assert b"".join(chunks) == b"chunk-0;chunk-1;chunk-2;chunk-3;"
    with pytest.raises(ValueError), client.stream("GET", "/stream/1") as response:
        list(response.iter_bytes(chunk_size=0))


def test_stream_iter_lines_handles_split_line_endings(client: nextreq.Client) -> None:
    with client.stream("GET", "/lines") as response:
        assert list(response.iter_lines()) == ["alpha", "beta", "gamma", "delta é", "last"]
    with client.stream("GET", "/lines") as response:
        assert "".join(response.iter_text()) == "alpha\r\nbeta\ngamma\rdelta é\nlast"


def test_closing_a_stream_early_releases_the_connection(server: LiveServer) -> None:
    with nextreq.Client(
        base_url=server.url, max_connections=1, timeout=nextreq.Timeout(pool=2)
    ) as c:
        with c.stream("GET", "/stream/50") as response:
            next(response.iter_bytes())
        with pytest.raises(nextreq.StreamClosed):
            list(response.iter_bytes())
        # The single connection slot must be free again.
        assert c.get("/json").status_code == 200


def test_buffered_response_supports_iteration(client: nextreq.Client) -> None:
    response = client.get("/json")
    assert list(response.iter_bytes()) == [b'{"hello": "world"}']
    assert list(response.iter_lines()) == ['{"hello": "world"}']
    assert response.read() == response.content


# -- errors -------------------------------------------------------------------


def test_connection_refused(unused_port: int) -> None:
    with pytest.raises(nextreq.ConnectError) as info:
        nextreq.get(f"http://127.0.0.1:{unused_port}/")
    error = info.value
    assert isinstance(error, nextreq.NetworkError)
    assert isinstance(error, nextreq.TransportError)
    assert isinstance(error, nextreq.NextReqError)
    assert isinstance(error, ConnectionError), "also a builtin ConnectionError"
    assert error.request is not None and error.request.method == "GET"


def test_invalid_urls(client: nextreq.Client) -> None:
    with pytest.raises(nextreq.InvalidURL):
        nextreq.get("not a url")
    with pytest.raises(nextreq.InvalidURL):
        nextreq.get("http://")
    with pytest.raises(nextreq.UnsupportedProtocol):
        nextreq.get("ftp://example.com/x")
    assert issubclass(nextreq.InvalidURL, ValueError)


def test_server_dropping_connection_is_a_transport_error(client: nextreq.Client) -> None:
    with pytest.raises(nextreq.TransportError) as info:
        client.get("/drop")
    assert isinstance(info.value, (nextreq.RemoteProtocolError, nextreq.ReadError))


def test_invalid_header_is_rejected_locally(client: nextreq.Client) -> None:
    with pytest.raises(nextreq.LocalProtocolError):
        client.get("/echo", headers={"bad header name": "x"})
    with pytest.raises(TypeError):
        client.get("/echo", headers={"X-Int": 5})  # type: ignore[dict-item]


def test_closed_client_refuses_requests(server: LiveServer) -> None:
    client = nextreq.Client(base_url=server.url)
    assert not client.is_closed
    client.close()
    client.close()  # idempotent
    assert client.is_closed
    with pytest.raises(nextreq.ClientClosed):
        client.get("/json")


# -- connection pooling & the GIL ---------------------------------------------


def test_keepalive_connection_is_reused(client: nextreq.Client, server: LiveServer) -> None:
    for _ in range(25):
        assert client.get("/json").status_code == 200
    assert server.request_count == 25
    assert len(server.peers) == 1, f"expected one pooled connection, saw {len(server.peers)}"


def test_separate_clients_have_separate_pools(server: LiveServer) -> None:
    with nextreq.Client() as a, nextreq.Client() as b:
        a.get(f"{server.url}/json")
        b.get(f"{server.url}/json")
    assert len(server.peers) == 2


def test_gil_is_released_during_blocking_io(client: nextreq.Client) -> None:
    """While a sync request waits on the network, other Python threads must run."""
    ticks = 0
    stop = threading.Event()

    def spin() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            time.sleep(0.001)

    worker = threading.Thread(target=spin, daemon=True)
    worker.start()
    try:
        before = ticks
        started = time.perf_counter()
        assert client.get("/delay/0.4").status_code == 200
        elapsed = time.perf_counter() - started
        progressed = ticks - before
    finally:
        stop.set()
        worker.join()
    assert elapsed >= 0.4
    # If the GIL were held, the spinner could not tick at all during the request.
    # (The bar is low on purpose: timers are coarse on Windows and CI is slow.)
    assert progressed >= 10, f"background thread only ticked {progressed} times"


def test_threads_share_one_client(client: nextreq.Client, server: LiveServer) -> None:
    results: list[int] = []
    errors: list[BaseException] = []

    def work(n: int) -> None:
        try:
            for _ in range(10):
                results.append(client.get("/slow", params={"delay": 0.01, "n": n}).json()["n"])
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert not errors
    assert sorted(results) == sorted(list(range(8)) * 10)
    assert server.max_inflight > 1, "threads should have run requests in parallel"
    assert len(server.peers) <= 8


@pytest.mark.skipif(sys.platform == "win32", reason="SIGALRM is POSIX-only")
def test_blocking_request_is_interruptible_by_signals(client: nextreq.Client) -> None:
    """Ctrl-C (or any signal handler that raises) must not wait for the timeout."""

    class Interrupted(Exception):
        pass

    def handler(signum: int, frame: Any) -> None:
        raise Interrupted

    previous = signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, 0.2)
    started = time.perf_counter()
    try:
        with pytest.raises(Interrupted):
            client.get("/delay/5")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    assert time.perf_counter() - started < 2.0
    # The client is still usable afterwards.
    assert client.get("/json").status_code == 200


# -- timings ------------------------------------------------------------------


def test_elapsed_exposes_total_and_phase_timings(client: nextreq.Client) -> None:
    first = client.get("/delay/0.1")
    elapsed = first.elapsed
    assert isinstance(elapsed, nextreq.Elapsed)
    assert elapsed.total_seconds() >= 0.1 and elapsed.total >= 0.1
    assert elapsed.ttfb is not None and elapsed.ttfb >= 0.1
    assert elapsed.connect is not None and elapsed.connect > 0, "first request opens a connection"
    assert elapsed.pool is not None and elapsed.pool >= 0
    assert elapsed.download is not None and elapsed.download >= 0
    assert elapsed.total >= elapsed.ttfb
    assert set(elapsed.as_dict()) == {"total", "pool", "connect", "ttfb", "download"}
    assert "ttfb=" in repr(elapsed)

    second = client.get("/json")
    assert second.elapsed.connect is None, "a pooled connection has no connect phase"


# -- TLS & HTTP/2 -------------------------------------------------------------


def test_http2_over_tls_with_custom_ca(tls_server: TLSServer) -> None:
    with nextreq.Client(verify=tls_server.ca_path) as client:
        response = client.get(f"{tls_server.url}/hello")
        assert response.http_version == "HTTP/2"
        assert response.json() == {"http_version": "2", "path": "/hello", "scheme": "https"}
        assert response.elapsed.connect is not None


def test_http1_only_over_tls(tls_server: TLSServer) -> None:
    with nextreq.Client(verify=tls_server.ca_path, http2=False) as client:
        response = client.get(f"{tls_server.url}/")
        assert response.http_version == "HTTP/1.1"
        assert response.json()["http_version"] == "1.1"


def test_untrusted_certificate_is_rejected(tls_server: TLSServer) -> None:
    with pytest.raises(nextreq.ConnectError) as info:
        nextreq.get(f"{tls_server.url}/")
    assert "certificate" in str(info.value).lower()


def test_verify_false_skips_certificate_checks(tls_server: TLSServer) -> None:
    assert nextreq.get(f"{tls_server.url}/", verify=False).status_code == 200
    with nextreq.Client(verify=False) as client:
        assert client.get(f"https://127.0.0.1:{tls_server.port}/").status_code == 200


def test_client_certificate_configuration(tls_server: TLSServer) -> None:
    with nextreq.Client(verify=tls_server.ca_path, cert=tls_server.client_cert_path) as client:
        assert client.get(f"{tls_server.url}/").status_code == 200
    with pytest.raises(nextreq.LocalProtocolError, match="certificates"):
        nextreq.Client(verify="/no/such/ca-bundle.pem")
    with pytest.raises(nextreq.LocalProtocolError):
        nextreq.Client(cert="/no/such/client.pem")


def test_http3_requires_a_custom_build() -> None:
    if nextreq.HTTP3_ENABLED:
        pytest.skip("this build has HTTP/3 enabled")
    with pytest.raises(nextreq.LocalProtocolError, match="HTTP/3"):
        nextreq.Client(http3=True)
    with pytest.raises(nextreq.LocalProtocolError, match="at least one"):
        nextreq.Client(http1=False, http2=False)


# -- models -------------------------------------------------------------------


def test_headers_model() -> None:
    headers = Headers({"Content-Type": "text/plain", "X-Multi": "a"})
    headers.add("x-multi", "b")
    assert headers["content-type"] == headers["CONTENT-TYPE"] == "text/plain"
    assert headers["x-multi"] == "a, b"
    assert headers.get_list("X-Multi") == ["a", "b"]
    assert headers.get("missing") is None and headers.get("missing", "d") == "d"
    assert "X-MULTI" in headers and "nope" not in headers and 5 not in headers
    assert sorted(headers) == ["content-type", "x-multi"] and len(headers) == 2
    assert headers.multi_items() == [
        ("content-type", "text/plain"),
        ("x-multi", "a"),
        ("x-multi", "b"),
    ]

    headers["X-Multi"] = "c"
    assert headers.get_list("x-multi") == ["c"]
    del headers["CONTENT-TYPE"]
    assert "content-type" not in headers
    with pytest.raises(KeyError):
        del headers["content-type"]
    with pytest.raises(KeyError):
        _ = headers["content-type"]

    assert Headers([("A", "1")]) == Headers({"a": "1"}) == {"A": "1"}
    assert Headers({"a": b"bytes"})["a"] == "bytes"  # type: ignore[dict-item]
    copy = headers.copy()
    copy["new"] = "1"
    assert "new" not in headers
    assert "[secure]" in repr(Headers({"Authorization": "Bearer x"}))
    assert "Bearer x" not in repr(Headers({"Authorization": "Bearer x"}))
    with pytest.raises(TypeError):
        hash(headers)


def test_url_model() -> None:
    url = URL("https://User:Pass@Example.com:8443/a/b?x=1&y=2#frag")
    assert url.scheme == "https" and url.host == "example.com" and url.port == 8443
    assert url.username == "User" and url.password == "Pass"
    assert url.path == "/a/b" and url.query == "x=1&y=2" and url.fragment == "frag"
    assert url.raw_path == "/a/b?x=1&y=2" and url.is_absolute
    assert url.origin == ("https", "example.com", 8443)
    assert str(url.without_userinfo()) == "https://Example.com:8443/a/b?x=1&y=2#frag"
    assert URL("http://h/").origin == ("http", "h", 80) and URL("http://h").path == "/"
    assert URL("http://h/a/b").join("../c?d=1") == "http://h/c?d=1"
    assert URL("http://h/p", params={"q": "a b"}) == "http://h/p?q=a+b"
    assert URL("http://h/p?a=1#f").copy_merge_params({"b": 2}) == "http://h/p?a=1&b=2#f"
    assert URL(url) == url and hash(URL("x")) == hash(URL("x")) and URL("x") != 5
    assert repr(URL("http://h/")) == "URL('http://h/')"
    assert not URL("/relative").is_absolute
    with pytest.raises(TypeError):
        URL(5)  # type: ignore[arg-type]
    with pytest.raises(nextreq.InvalidURL):
        _ = URL("http://h:notaport/").port


def test_response_model_constructed_directly() -> None:
    response = Response(201, json={"a": 1}, headers={"X-A": "b"})
    assert response.json() == {"a": 1} and response.headers["content-type"] == "application/json"
    assert response.status_code == 201 and response.reason_phrase == "Created"
    assert Response(200, text="héllo").content == "héllo".encode()
    assert (
        Response(200, content=b"\xe9", headers={"content-type": "text/plain; charset=latin-1"}).text
        == "é"
    )
    assert Response(200, content=b"\xff\xfe").text == "��", "bad bytes are replaced"
    assert Response(299).reason_phrase == "" and Response().content == b""
    streamed = Response(200, stream=iter([b"a", b"b"]))
    assert streamed.read() == b"ab"

    custom = Response(200, content=b"\xe9t\xe9")
    custom.encoding = "latin-1"
    assert custom.text == "été"
    assert Response(500).is_server_error and Response(101).is_informational
    assert Response(302, headers={"location": "/x"}).is_redirect and not Response(302).is_redirect


def test_request_model_constructed_directly() -> None:
    request = Request(
        "post",
        "https://example.com/x",
        params={"a": 1},
        json={"b": 2},
        headers={"Cookie": "c=1"},
        cookies={"d": "2"},
        timeout=3,
    )
    assert request.method == "POST" and request.url == "https://example.com/x?a=1"
    assert request.content == b'{"b":2}'
    assert request.headers["content-type"] == "application/json"
    assert request.headers["content-length"] == "7"
    assert request.timeout == nextreq.Timeout(3)
    request.extensions["k"] = "v"
    assert request.extensions == {"k": "v"}
    assert Request("POST", "http://h/", data={"x": "y"}).content == b"x=y"
