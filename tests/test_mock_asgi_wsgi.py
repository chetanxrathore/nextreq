"""NXRQ-502: ``MockTransport`` and the in-memory ASGI / WSGI transports."""

from __future__ import annotations

import asyncio
import importlib.util
import re
from typing import Any

import pydantic
import pytest

import nextreq
from nextreq import MockTransport, Request, Response, SSEEvent
from nextreq.testing import ASGITransport, MockCall, MockRouteNotFound, WSGITransport, encode_sse

# FastAPI resolves handler annotations from *module* globals (this file uses
# postponed annotations), so its types must be importable at module level.
try:
    import fastapi
    from fastapi import BackgroundTasks, Form, UploadFile
    from fastapi import Request as FastAPIRequest
except ImportError:  # pragma: no cover - exercised only without the dev extras
    fastapi = None  # type: ignore[assignment]

API = "https://api.example.test"


class ItemIn(pydantic.BaseModel):
    name: str
    price: float


# -- MockTransport: matching --------------------------------------------------


def test_static_json_response_and_call_assertions() -> None:
    mock = MockTransport()
    route = mock.get(f"{API}/users/1").respond(json={"id": 1, "name": "Ada"})
    mock.assert_not_called()
    route.assert_not_called()

    with nextreq.Client(transport=mock) as client:
        response = client.get(f"{API}/users/1")

    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Ada"}
    assert response.headers["content-type"] == "application/json"
    assert response.request is not None and response.url == f"{API}/users/1"
    assert response.elapsed.total >= 0 and response.elapsed.connect is None

    mock.assert_called()
    mock.assert_called_once()
    route.assert_called_once()
    mock.assert_call_count(1)
    assert mock.called and mock.call_count == 1 and route.call_count == 1
    assert len(mock.calls) == 1
    call = mock.calls[0]
    assert isinstance(call, MockCall)
    assert call.request.method == "GET" and call.route is route and call.response is not None
    assert mock.last_request is call.request


def test_method_matching_and_shortcuts() -> None:
    mock = MockTransport()
    routes = {
        "GET": mock.get(f"{API}/r").respond(text="get"),
        "POST": mock.post(f"{API}/r").respond(201, text="post"),
        "PUT": mock.put(f"{API}/r").respond(text="put"),
        "PATCH": mock.patch(f"{API}/r").respond(text="patch"),
        "DELETE": mock.delete(f"{API}/r").respond(204),
        "HEAD": mock.head(f"{API}/r").respond(headers={"X-Head": "1"}),
        "OPTIONS": mock.options(f"{API}/r").respond(headers={"Allow": "GET"}),
    }
    with nextreq.Client(transport=mock) as client:
        assert client.get(f"{API}/r").text == "get"
        assert client.post(f"{API}/r").status_code == 201
        assert client.put(f"{API}/r").text == "put"
        assert client.patch(f"{API}/r").text == "patch"
        assert client.delete(f"{API}/r").status_code == 204
        assert client.head(f"{API}/r").headers["x-head"] == "1"
        assert client.options(f"{API}/r").headers["allow"] == "GET"
    for route in routes.values():
        route.assert_called_once()
    mock.assert_all_called()
    assert repr(routes["GET"]) == f"<MockRoute GET {API}/r>"


def test_url_string_path_regex_and_predicate_matching() -> None:
    mock = MockTransport()
    exact = mock.get(f"{API}/exact?page=2").respond(text="exact-with-query")
    loose = mock.get(f"{API}/loose").respond(text="query-ignored")
    path = mock.get("/by/path").respond(text="path")
    regex = mock.get(re.compile(r"/items/(\d+)$")).respond(text="regex")
    predicate = mock.route(url=lambda request: request.url.host == "other.test").respond(
        text="pred"
    )
    anything = mock.route("DELETE").respond(204)

    with nextreq.Client(transport=mock) as client:
        assert client.get(f"{API}/exact", params={"page": 2}).text == "exact-with-query"
        assert client.get(f"{API}/loose", params={"anything": "goes"}).text == "query-ignored"
        assert client.get(f"{API}/loose/").text == "query-ignored", "trailing slash tolerated"
        assert client.get("https://whatever.test/by/path").text == "path"
        assert client.get(f"{API}/items/42").text == "regex"
        assert client.get("https://other.test/zzz").text == "pred"
        assert client.delete("https://any.test/thing").status_code == 204
        with pytest.raises(MockRouteNotFound):
            client.get(f"{API}/exact", params={"page": 3})
        with pytest.raises(MockRouteNotFound):
            client.get(f"{API}/items/abc")

    assert [r.call_count for r in (exact, loose, path, regex, predicate, anything)] == [
        1,
        2,
        1,
        1,
        1,
        1,
    ]


def test_params_headers_json_and_content_matchers() -> None:
    mock = MockTransport()
    by_params = mock.get(f"{API}/search", params={"q": "rust"}).respond(text="params")
    by_header = mock.get(f"{API}/search", headers={"X-Tenant": "acme"}).respond(text="header")
    by_json = mock.post(f"{API}/things", json={"name": "widget"}).respond(201)
    by_content = mock.post(f"{API}/things", content=b"raw-bytes").respond(202)
    fallback = mock.post(f"{API}/things").respond(400)

    with nextreq.Client(transport=mock) as client:
        assert client.get(f"{API}/search", params={"q": "rust", "page": 1}).text == "params"
        assert client.get(f"{API}/search", headers={"x-tenant": "acme"}).text == "header"
        assert client.post(f"{API}/things", json={"name": "widget"}).status_code == 201
        assert client.post(f"{API}/things", content=b"raw-bytes").status_code == 202
        assert client.post(f"{API}/things", json={"name": "other"}).status_code == 400
        assert client.post(f"{API}/things", content=b"not json").status_code == 400
    for route in (by_params, by_header, by_json, by_content):
        route.assert_called_once()
    assert fallback.call_count == 2


def test_unmatched_request_raises_a_helpful_error() -> None:
    mock = MockTransport()
    mock.get(f"{API}/known").respond()
    with nextreq.Client(transport=mock, retries=3) as client:
        with pytest.raises(MockRouteNotFound) as info:
            client.get(f"{API}/unknown")
    error = info.value
    assert f"GET {API}/unknown" in str(error) and "/known" in str(error)
    assert isinstance(error, AssertionError) and isinstance(error, nextreq.NextReqError)
    assert not isinstance(error, nextreq.TransportError), "must never be retried"
    assert mock.call_count == 1
    assert mock.calls[0].exception is error
    with pytest.raises(AssertionError, match="never called"):
        mock.assert_all_called()


def test_assertion_failures_describe_the_calls() -> None:
    mock = MockTransport()
    route = mock.get(f"{API}/a").respond()
    with pytest.raises(AssertionError, match="to have been called, but it was not"):
        mock.assert_called()
    with pytest.raises(AssertionError, match="called 0 times"):
        route.assert_called_once()
    with nextreq.Client(transport=mock) as client:
        client.get(f"{API}/a")
        client.get(f"{API}/a")
    with pytest.raises(AssertionError, match=rf"called 2 times \(GET {API}/a, GET {API}/a\)"):
        mock.assert_called_once()
    with pytest.raises(AssertionError, match="not to have been called"):
        route.assert_not_called()
    mock.assert_call_count(2)
    mock.reset()
    mock.assert_not_called()
    assert route.call_count == 0 and mock.last_request is None


# -- MockTransport: replies ---------------------------------------------------


def test_response_kinds() -> None:
    mock = MockTransport()
    mock.get("/text").respond(text="héllo")
    mock.get("/bytes").respond(content=b"\x00\x01", headers={"Content-Type": "application/x-bin"})
    mock.get("/empty").respond(204)
    mock.get("/headers").respond(headers=[("Set-Cookie", "a=1"), ("Set-Cookie", "b=2")])
    mock.get("/default")
    with nextreq.Client(transport=mock, base_url=API) as client:
        assert client.get("/text").text == "héllo"
        binary = client.get("/bytes")
        assert (
            binary.content == b"\x00\x01" and binary.headers["content-type"] == "application/x-bin"
        )
        assert client.get("/empty").content == b""
        assert client.get("/headers").cookies == {"a": "1", "b": "2"}
        assert client.cookies["b"] == "2"
        assert client.get("/default").status_code == 200, "a route with no reply answers 200"


def test_dynamic_handler() -> None:
    def handler(request: Request) -> Response:
        user_id = int(request.url.path.rsplit("/", 1)[1])
        if user_id > 100:
            return Response(404, json={"detail": "not found"})
        return Response(200, json={"id": user_id, "via": request.headers.get("x-via")})

    mock = MockTransport()
    route = mock.get(re.compile(r"/users/\d+")).reply(handler)
    with nextreq.Client(transport=mock, base_url=API, headers={"X-Via": "test"}) as client:
        assert client.get("/users/7").json() == {"id": 7, "via": "test"}
        assert client.get("/users/999").status_code == 404
    assert route.call_count == 2
    assert [call.response.status_code for call in route.calls if call.response] == [200, 404]


def test_fallback_handler_for_unrouted_requests() -> None:
    mock = MockTransport(lambda request: Response(200, text=f"{request.method} {request.url.path}"))
    mock.get("/routed").respond(text="routed")
    with nextreq.Client(transport=mock, base_url=API) as client:
        assert client.get("/routed").text == "routed"
        assert client.post("/anything/else").text == "POST /anything/else"
    assert mock.call_count == 2 and mock.calls[1].route is None


def test_sequence_and_raises() -> None:
    mock = MockTransport()
    mock.get("/seq").sequence(
        Response(503),
        nextreq.ConnectError("mocked refusal"),
        lambda request: Response(200, json={"ok": True}),
    )
    mock.get("/boom").raises(nextreq.ReadTimeout("mocked timeout"))
    mock.get("/boom-class").raises(nextreq.ConnectError)
    with nextreq.Client(transport=mock, base_url=API) as client:
        assert client.get("/seq").status_code == 503
        with pytest.raises(nextreq.ConnectError, match="mocked refusal"):
            client.get("/seq")
        assert client.get("/seq").json() == {"ok": True}
        assert client.get("/seq").json() == {"ok": True}, "the last reply repeats"
        with pytest.raises(nextreq.ReadTimeout, match="mocked timeout") as info:
            client.get("/boom")
        assert info.value.request is not None
        with pytest.raises(nextreq.ConnectError):
            client.get("/boom-class")
    assert isinstance(mock.calls[1].exception, nextreq.ConnectError)
    with pytest.raises(ValueError):
        mock.get("/x").sequence()


def test_static_responses_are_fresh_for_every_call() -> None:
    mock = MockTransport()
    mock.get("/static").respond(json={"n": 1})
    shared = Response(200, json={"shared": True})
    mock.get("/shared").sequence(shared)
    with nextreq.Client(transport=mock, base_url=API) as client:
        first, second = client.get("/static"), client.get("/static")
        assert first is not second and first.json() == second.json() == {"n": 1}
        a, b = client.get("/shared"), client.get("/shared")
        assert a is not b and a is not shared and a.json() == b.json() == {"shared": True}
        assert a.request is not None and shared.request is None


def test_handler_must_return_a_response() -> None:
    mock = MockTransport()
    mock.get("/bad").reply(lambda request: {"not": "a response"})  # type: ignore[arg-type,return-value]
    with nextreq.Client(transport=mock, base_url=API) as client:
        with pytest.raises(TypeError, match=r"must return a nextreq\.Response"):
            client.get("/bad")


# -- MockTransport: the full client pipeline ------------------------------------


def test_redirects_cookies_and_retries_run_through_the_mock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []
    monkeypatch.setattr("nextreq.client._sleep", delays.append)
    mock = MockTransport()
    mock.get("/login").respond(
        302, headers=[("Location", "/home"), ("Set-Cookie", "session=s3; Path=/")]
    )
    home = mock.get("/home").respond(json={"page": "home"})
    flaky = mock.get("/flaky").sequence(
        Response(503, headers={"Retry-After": "2"}),
        nextreq.ConnectError("blip"),
        Response(200, text="recovered"),
    )
    with nextreq.Client(transport=mock, base_url=API, retries=3) as client:
        response = client.get("/login")
        assert response.json() == {"page": "home"}
        assert [r.status_code for r in response.history] == [302]
        assert home.last_request is not None
        assert home.last_request.headers["cookie"] == "session=s3"

        recovered = client.get("/flaky")
        assert recovered.text == "recovered" and recovered.retry_count == 2
    assert flaky.call_count == 3
    assert delays[0] == 2.0 and len(delays) == 2


def test_typed_parsing_and_raise_for_status_through_the_mock() -> None:
    class User(pydantic.BaseModel):
        id: int
        name: str

    mock = MockTransport()
    mock.get("/users/1").respond(json={"id": 1, "name": "Ada"})
    mock.get("/users/2").respond(json={"id": "x"})
    mock.get("/users/3").respond(404, json={"detail": "nope"})
    with nextreq.Client(transport=mock, base_url=API) as client:
        assert client.get("/users/1", model=User) == User(id=1, name="Ada")
        with pytest.raises(nextreq.DecodeError) as decode:
            client.get("/users/2", model=User)
        assert decode.value.url == f"{API}/users/2" and decode.value.status_code == 200
        with pytest.raises(nextreq.HTTPStatusError):
            client.get("/users/3", model=User)
        with pytest.raises(nextreq.HTTPStatusError):
            client.get("/users/3", raise_for_status=True)


def test_request_inspection() -> None:
    mock = MockTransport()
    route = mock.post("/submit").respond(201)
    with nextreq.Client(transport=mock, base_url=API, auth=("u", "p")) as client:
        client.post("/submit", json={"a": 1}, params={"dry": "1"}, headers={"X-Trace": "t"})
    request = route.last_request
    assert request is not None
    assert request.method == "POST" and request.url == f"{API}/submit?dry=1"
    assert request.content == b'{"a":1}'
    assert request.headers["content-type"] == "application/json"
    assert request.headers["x-trace"] == "t"
    assert request.headers["authorization"].startswith("Basic ")
    assert request.headers["user-agent"].startswith("nextreq/")


# -- MockTransport: SSE and streams ---------------------------------------------


def test_sse_stream_mocking() -> None:
    mock = MockTransport()
    mock.get("/events").respond(
        sse=[
            SSEEvent(event="start", data="go", id="1"),
            "plain string",
            {"n": 3},
            SSEEvent(data="multi\nline", retry=500),
        ]
    )
    with nextreq.Client(transport=mock, base_url=API) as client:
        with client.stream("GET", "/events") as response:
            assert response.headers["content-type"] == "text/event-stream"
            events = list(response.iter_sse())
        assert events == [
            SSEEvent(event="start", data="go", id="1"),
            SSEEvent(data="plain string", id="1"),
            SSEEvent(data='{"n":3}', id="1"),
            SSEEvent(data="multi\nline", id="1", retry=500),
        ]
        assert events[2].json() == {"n": 3}
        # The mocked stream is replayable for a second request.
        with client.stream("GET", "/events") as again:
            assert len(list(again.iter_sse())) == 4
        # Without stream(), the body is simply buffered.
        assert len(list(client.get("/events").iter_sse())) == 4


def test_byte_stream_mocking_delivers_chunks_incrementally() -> None:
    mock = MockTransport()
    mock.get("/download").respond(stream=[b"part-1;", b"part-2;", b"part-3;"])
    with nextreq.Client(transport=mock, base_url=API) as client:
        with client.stream("GET", "/download") as response:
            with pytest.raises(nextreq.ResponseNotRead):
                _ = response.content
            assert list(response.iter_bytes()) == [b"part-1;", b"part-2;", b"part-3;"]
        assert client.get("/download").content == b"part-1;part-2;part-3;"


def test_encode_sse_helper() -> None:
    assert encode_sse(["a", {"k": 1}, SSEEvent(event="e", data="d")]) == [
        b"data: a\n\n",
        b'data: {"k":1}\n\n',
        b"event: e\ndata: d\n\n",
    ]


# -- MockTransport with AsyncClient ---------------------------------------------


async def test_async_client_with_mock_transport() -> None:
    async def async_handler(request: Request) -> Response:
        return Response(200, json={"async": True, "path": request.url.path})

    mock = MockTransport()
    mock.get("/sync-static").respond(json={"kind": "static"})
    mock.get("/sync-handler").reply(lambda request: Response(200, text="sync handler"))
    mock.get("/async-handler").reply(async_handler)
    mock.get("/events").respond(sse=["one", "two", "three"])
    mock.get("/boom").raises(nextreq.ConnectError("async boom"))

    async with nextreq.AsyncClient(transport=mock, base_url=API) as client:
        assert (await client.get("/sync-static")).json() == {"kind": "static"}
        assert (await client.get("/sync-handler")).text == "sync handler"
        assert (await client.get("/async-handler")).json() == {
            "async": True,
            "path": "/async-handler",
        }
        async with client.stream("GET", "/events") as response:
            assert [e.data async for e in response.aiter_sse()] == ["one", "two", "three"]
        with pytest.raises(nextreq.ConnectError, match="async boom"):
            await client.get("/boom")
        responses = await client.gather(["/sync-static"] * 5 + ["/async-handler"])
        assert [r.status_code for r in responses] == [200] * 6
    assert mock.call_count == 11


def test_async_handler_used_with_sync_client_is_a_clear_error() -> None:
    async def async_handler(request: Request) -> Response:
        return Response(200)

    mock = MockTransport()
    mock.get("/a").reply(async_handler)
    with nextreq.Client(transport=mock, base_url=API) as client:
        with pytest.raises(TypeError, match="asynchronous"):
            client.get("/a")


def test_transport_and_app_are_mutually_exclusive() -> None:
    with pytest.raises(TypeError, match="either"):
        nextreq.Client(transport=MockTransport(), app=lambda environ, start_response: [])
    with pytest.raises(TypeError, match="either"):
        nextreq.AsyncClient(transport=MockTransport(), app=lambda scope, receive, send: None)


# -- ASGI: FastAPI ---------------------------------------------------------------


@pytest.fixture
def fastapi_app() -> Any:
    if fastapi is None:
        pytest.skip("fastapi is not installed")
    from fastapi.responses import PlainTextResponse, RedirectResponse, StreamingResponse

    app = fastapi.FastAPI()

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"message": "hello from fastapi"}

    @app.get("/items/{item_id}")
    async def read_item(item_id: int, q: str | None = None) -> dict[str, Any]:
        return {"item_id": item_id, "q": q}

    @app.post("/items", status_code=201)
    async def create_item(item: ItemIn) -> dict[str, Any]:
        return {"created": item.name, "price_with_tax": round(item.price * 1.2, 2)}

    @app.get("/headers")
    async def headers(request: FastAPIRequest) -> dict[str, Any]:
        return {
            "headers": dict(request.headers),
            "client": request.client.host if request.client else None,
            "url": str(request.url),
        }

    @app.get("/login")
    async def login() -> Any:
        response = RedirectResponse("/me", status_code=303)
        response.set_cookie("session", "fa-session")
        return response

    @app.get("/me")
    async def me(request: FastAPIRequest) -> dict[str, Any]:
        return {"session": request.cookies.get("session")}

    @app.get("/events")
    async def events() -> Any:
        async def generate() -> Any:
            for n in range(3):
                yield f"id: {n}\ndata: event-{n}\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")

    @app.get("/teapot")
    async def teapot() -> Any:
        return PlainTextResponse("short and stout", status_code=418)

    @app.get("/crash")
    async def crash() -> None:
        raise RuntimeError("fastapi exploded")

    if importlib.util.find_spec("python_multipart") is None:
        return app  # FastAPI needs python-multipart to declare form routes

    @app.post("/upload")
    async def upload(file: UploadFile, note: str = Form()) -> dict[str, Any]:
        data = await file.read()
        return {"filename": file.filename, "size": len(data), "note": note}

    return app


async def test_asgi_fastapi_basic(fastapi_app: Any) -> None:
    async with nextreq.AsyncClient(app=fastapi_app) as client:
        assert client.base_url == "http://testserver"
        response = await client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "hello from fastapi"}
        assert response.headers["content-type"] == "application/json"
        assert response.elapsed.total >= 0

        assert (await client.get("/items/42", params={"q": "search me"})).json() == {
            "item_id": 42,
            "q": "search me",
        }
        invalid = await client.get("/items/not-an-int")
        assert invalid.status_code == 422

        created = await client.post("/items", json={"name": "Widget", "price": 10})
        assert created.status_code == 201
        assert created.json() == {"created": "Widget", "price_with_tax": 12.0}

        assert (await client.get("/teapot")).text == "short and stout"
        assert (await client.get("/missing")).status_code == 404
        assert (await client.head("/")).content == b""


async def test_asgi_request_details_reach_the_app(fastapi_app: Any) -> None:
    async with nextreq.AsyncClient(app=fastapi_app, headers={"X-Client": "c"}) as client:
        seen = (await client.get("/headers", headers={"X-Request": "r"}, params={"a": "1"})).json()
    assert seen["headers"]["x-client"] == "c" and seen["headers"]["x-request"] == "r"
    assert seen["headers"]["host"] == "testserver"
    assert seen["headers"]["user-agent"].startswith("nextreq/")
    assert seen["client"] == "127.0.0.1"
    assert seen["url"] == "http://testserver/headers?a=1"


async def test_asgi_cookies_redirects_and_typed_parsing(fastapi_app: Any) -> None:
    class Me(pydantic.BaseModel):
        session: str

    async with nextreq.AsyncClient(app=fastapi_app) as client:
        response = await client.get("/login")
        assert response.json() == {"session": "fa-session"}
        assert [r.status_code for r in response.history] == [303]
        assert client.cookies["session"] == "fa-session"
        assert await client.get("/me", model=Me) == Me(session="fa-session")


async def test_asgi_multipart_upload(fastapi_app: Any) -> None:
    pytest.importorskip("python_multipart", reason="needed for FastAPI uploads")
    async with nextreq.AsyncClient(app=fastapi_app) as client:
        uploaded = await client.post(
            "/upload", files={"file": ("report.txt", b"12345")}, data={"note": "hi"}
        )
        assert uploaded.json() == {"filename": "report.txt", "size": 5, "note": "hi"}


async def test_asgi_sse_and_gather(fastapi_app: Any) -> None:
    async with nextreq.AsyncClient(app=fastapi_app) as client:
        async with client.stream("GET", "/events") as response:
            events = [event async for event in response.aiter_sse()]
        assert [(e.id, e.data) for e in events] == [
            ("0", "event-0"),
            ("1", "event-1"),
            ("2", "event-2"),
        ]

        responses = await client.gather([f"/items/{n}" for n in range(20)], max_concurrency=5)
        assert [r.json()["item_id"] for r in responses] == list(range(20))


async def test_asgi_application_exceptions(fastapi_app: Any) -> None:
    async with nextreq.AsyncClient(app=fastapi_app) as client:
        with pytest.raises(RuntimeError, match="fastapi exploded"):
            await client.get("/crash")
    quiet = ASGITransport(fastapi_app, raise_app_exceptions=False)
    async with nextreq.AsyncClient(transport=quiet, base_url="http://testserver") as client:
        assert (await client.get("/crash")).status_code == 500


async def test_asgi_raw_application_and_scope() -> None:
    scopes: list[dict[str, Any]] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        scopes.append(scope)
        message = await receive()
        body = message["body"]
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/plain"),
                    (b"x-multi", b"1"),
                    (b"x-multi", b"2"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"got:", "more_body": True})
        await send({"type": "http.response.body", "body": body})

    transport = ASGITransport(app, root_path="/api", client=("10.0.0.9", 4444))
    async with nextreq.AsyncClient(transport=transport, base_url="https://svc.test:8443") as client:
        response = await client.post("/caf%C3%A9/x", params={"k": "v"}, content=b"payload")
    assert response.text == "got:payload"
    assert response.headers.get_list("x-multi") == ["1", "2"]
    # Like a real server, the transport first offers the lifespan protocol
    # (which this bare application does not implement), then the request.
    assert [scope["type"] for scope in scopes] == ["lifespan", "http"]
    scope = scopes[1]
    assert scope["type"] == "http" and scope["asgi"]["version"] == "3.0"
    assert scope["method"] == "POST" and scope["scheme"] == "https"
    assert scope["path"] == "/café/x" and scope["raw_path"] == b"/caf%C3%A9/x"
    assert scope["query_string"] == b"k=v" and scope["root_path"] == "/api"
    assert scope["server"] == ("svc.test", 8443) and scope["client"] == ("10.0.0.9", 4444)
    assert (b"host", b"svc.test:8443") in scope["headers"]
    assert all(isinstance(k, bytes) and isinstance(v, bytes) for k, v in scope["headers"])


async def test_asgi_app_that_never_responds_is_an_error() -> None:
    async def silent(scope: dict[str, Any], receive: Any, send: Any) -> None:
        return None

    async with nextreq.AsyncClient(app=silent) as client:
        with pytest.raises(RuntimeError, match="without starting a response"):
            await client.get("/")


# -- WSGI: Flask -----------------------------------------------------------------


@pytest.fixture
def flask_app() -> Any:
    flask = pytest.importorskip("flask")
    app = flask.Flask(__name__)

    @app.get("/")
    def root() -> Any:
        return {"message": "hello from flask"}

    @app.get("/items/<int:item_id>")
    def read_item(item_id: int) -> Any:
        return {"item_id": item_id, "q": flask.request.args.get("q")}

    @app.post("/items")
    def create_item() -> Any:
        payload = flask.request.get_json()
        return {"created": payload["name"]}, 201

    @app.post("/form")
    def form() -> Any:
        upload = flask.request.files.get("file")
        return {
            "fields": flask.request.form.to_dict(),
            "file": upload.read().decode() if upload else None,
            "filename": upload.filename if upload else None,
        }

    @app.get("/headers")
    def headers() -> Any:
        return {
            "headers": {k.lower(): v for k, v in flask.request.headers.items()},
            "remote_addr": flask.request.remote_addr,
            "url": flask.request.url,
        }

    @app.get("/login")
    def login() -> Any:
        response = flask.redirect("/me")
        response.set_cookie("session", "flask-session")
        return response

    @app.get("/me")
    def me() -> Any:
        return {"session": flask.request.cookies.get("session")}

    @app.get("/stream")
    def stream() -> Any:
        def generate() -> Any:
            for n in range(3):
                yield f"data: chunk-{n}\n\n"

        return flask.Response(generate(), mimetype="text/event-stream")

    @app.get("/unicode/<name>")
    def unicode_path(name: str) -> Any:
        return {"name": name}

    @app.get("/crash")
    def crash() -> Any:
        raise RuntimeError("flask exploded")

    return app


def test_wsgi_flask_basic(flask_app: Any) -> None:
    with nextreq.Client(app=flask_app) as client:
        assert client.base_url == "http://testserver"
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "hello from flask"}
        assert response.headers["content-type"] == "application/json"

        assert client.get("/items/5", params={"q": "a b"}).json() == {"item_id": 5, "q": "a b"}
        assert client.get("/items/abc").status_code == 404
        created = client.post("/items", json={"name": "Gadget"})
        assert created.status_code == 201 and created.json() == {"created": "Gadget"}
        assert client.get("/unicode/caf%C3%A9").json() == {"name": "café"}
        assert client.delete("/").status_code == 405


def test_wsgi_forms_uploads_and_headers(flask_app: Any) -> None:
    with nextreq.Client(app=flask_app, headers={"X-Client": "c"}) as client:
        form = client.post("/form", data={"a": "1", "b": "two"}).json()
        assert form == {"fields": {"a": "1", "b": "two"}, "file": None, "filename": None}
        upload = client.post(
            "/form", data={"note": "n"}, files={"file": ("notes.txt", b"file body")}
        ).json()
        assert upload == {"fields": {"note": "n"}, "file": "file body", "filename": "notes.txt"}

        seen = client.get("/headers", params={"x": "1"}, headers={"X-Request": "r"}).json()
    assert seen["headers"]["x-client"] == "c" and seen["headers"]["x-request"] == "r"
    assert seen["headers"]["host"] == "testserver"
    assert seen["remote_addr"] == "127.0.0.1"
    assert seen["url"] == "http://testserver/headers?x=1"


def test_wsgi_cookies_redirects_sse_and_typed_parsing(flask_app: Any) -> None:
    class Me(pydantic.BaseModel):
        session: str

    with nextreq.Client(app=flask_app) as client:
        response = client.get("/login")
        assert response.json() == {"session": "flask-session"}
        assert [r.status_code for r in response.history] == [302]
        assert client.get("/me", model=Me) == Me(session="flask-session")
        with client.stream("GET", "/stream") as streamed:
            assert [e.data for e in streamed.iter_sse()] == ["chunk-0", "chunk-1", "chunk-2"]


def test_wsgi_application_exceptions(flask_app: Any) -> None:
    flask_app.config["PROPAGATE_EXCEPTIONS"] = True
    with nextreq.Client(app=flask_app) as client:
        with pytest.raises(RuntimeError, match="flask exploded"):
            client.get("/crash")
    quiet = WSGITransport(flask_app, raise_app_exceptions=False)
    with nextreq.Client(transport=quiet, base_url="http://testserver") as client:
        assert client.get("/crash").status_code == 500


def test_wsgi_raw_application_and_environ() -> None:
    environs: list[dict[str, Any]] = []

    def app(environ: dict[str, Any], start_response: Any) -> list[bytes]:
        environs.append(environ)
        body = environ["wsgi.input"].read()
        write = start_response("201 Created", [("Content-Type", "text/plain"), ("X-A", "1")])
        write(b"written-first;")
        return [b"then:", body]

    with nextreq.Client(
        transport=WSGITransport(app, script_name="/root"), base_url="https://svc.test:8443"
    ) as client:
        response = client.put("/a%20b", params={"k": "v"}, content=b"payload", headers={"X-H": "h"})
    assert response.status_code == 201 and response.text == "written-first;then:payload"
    assert response.headers["x-a"] == "1"
    environ = environs[0]
    assert environ["REQUEST_METHOD"] == "PUT" and environ["PATH_INFO"] == "/a b"
    assert environ["QUERY_STRING"] == "k=v" and environ["SCRIPT_NAME"] == "/root"
    assert environ["SERVER_NAME"] == "svc.test" and environ["SERVER_PORT"] == "8443"
    assert environ["wsgi.url_scheme"] == "https" and environ["CONTENT_LENGTH"] == "7"
    assert environ["HTTP_X_H"] == "h" and environ["HTTP_HOST"] == "svc.test:8443"


def test_wsgi_app_that_never_starts_a_response_is_an_error() -> None:
    with nextreq.Client(app=lambda environ, start_response: []) as client:
        with pytest.raises(RuntimeError, match="start_response"):
            client.get("/")


async def test_wsgi_app_through_async_client(flask_app: Any) -> None:
    """A blocking WSGI app runs in a worker thread under AsyncClient."""
    transport = WSGITransport(flask_app)
    async with nextreq.AsyncClient(transport=transport, base_url="http://testserver") as client:
        responses = await client.gather([f"/items/{n}" for n in range(10)])
    assert [r.json()["item_id"] for r in responses] == list(range(10))


def test_asgi_app_passed_to_sync_client_is_rejected(fastapi_app: Any) -> None:
    with pytest.raises(TypeError, match="ASGI"):
        nextreq.Client(app=fastapi_app)


# -- streaming and lifespan through the in-process transports ---------------------


async def test_asgi_response_is_delivered_as_it_is_produced() -> None:
    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse

    app = FastAPI()
    gate = asyncio.Event()
    produced: list[str] = []

    @app.get("/feed")
    async def feed() -> Any:
        async def generate() -> Any:
            produced.append("first")
            yield b"first\n"
            await gate.wait()  # only continues once the client has seen "first"
            produced.append("second")
            yield b"second\n"

        return StreamingResponse(generate(), media_type="text/plain")

    async with nextreq.AsyncClient(app=app, base_url="http://app") as client:
        async with client.stream("GET", "/feed") as response:
            assert response.status_code == 200
            chunks = response.aiter_bytes()
            assert await chunks.__anext__() == b"first\n"
            assert produced == ["first"]  # the rest has not been generated yet
            gate.set()
            assert [chunk async for chunk in chunks] == [b"second\n"]
        assert produced == ["first", "second"]


async def test_asgi_streaming_applies_backpressure_and_signals_disconnect() -> None:
    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse

    app = FastAPI()
    produced = 0

    @app.get("/endless")
    async def endless() -> Any:
        async def generate() -> Any:
            nonlocal produced
            while True:
                produced += 1
                yield b"x" * 1024

        return StreamingResponse(generate())

    async with nextreq.AsyncClient(app=app, base_url="http://app") as client:
        async with client.stream("GET", "/endless") as response:
            received = 0
            async for _ in response.aiter_bytes():
                received += 1
                if received == 5:
                    break
            await asyncio.sleep(0.05)
            # The generator is paused by the bounded queue, not running away.
            assert produced < 5 + 16
        # Closing the response disconnects the "client": the application stops
        # producing instead of streaming into the void.
        stopped_at = produced
        await asyncio.sleep(0.1)
        assert produced == stopped_at


async def test_asgi_server_sent_events_arrive_incrementally(fastapi_app: Any) -> None:
    async with nextreq.AsyncClient(app=fastapi_app, base_url="http://app") as client:
        async with client.stream("GET", "/events") as response:
            events = [event async for event in response.aiter_sse()]
    assert len(events) >= 2
    assert all(isinstance(event, nextreq.SSEEvent) for event in events)


async def test_asgi_application_sees_the_disconnect() -> None:
    seen: list[str] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"hello", "more_body": True})
        seen.append((await receive())["type"])  # blocks until the client goes away

    async with nextreq.AsyncClient(app=app, base_url="http://app") as client:
        async with client.stream("GET", "/") as response:
            assert await response.aiter_bytes().__anext__() == b"hello"
            assert seen == []
        assert seen == ["http.disconnect"]


async def test_asgi_error_in_the_middle_of_a_response() -> None:
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            return
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        raise RuntimeError("generator blew up")

    async with nextreq.AsyncClient(app=app, base_url="http://app") as client:
        with pytest.raises(RuntimeError, match="generator blew up"):
            await client.get("/")
    quiet = ASGITransport(app, raise_app_exceptions=False)
    async with nextreq.AsyncClient(transport=quiet, base_url="http://app") as client:
        response = await client.get("/")
        assert response.status_code == 200 and response.content == b"partial"


async def test_asgi_background_work_finishes_before_the_response_is_returned() -> None:
    from fastapi import FastAPI

    app = FastAPI()
    done: list[str] = []

    @app.post("/jobs")
    async def create(background: BackgroundTasks) -> dict[str, bool]:
        background.add_task(done.append, "sent email")
        return {"queued": True}

    async with nextreq.AsyncClient(app=app, base_url="http://app") as client:
        assert (await client.post("/jobs")).json() == {"queued": True}
        assert done == ["sent email"]


async def test_asgi_lifespan_runs_startup_and_shutdown() -> None:
    import contextlib

    from fastapi import FastAPI

    events: list[str] = []

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> Any:
        events.append("startup")
        yield {"database": "connected"}
        events.append("shutdown")

    app = FastAPI(lifespan=lifespan)

    @app.get("/db")
    async def db(request: FastAPIRequest) -> dict[str, str]:
        return {"database": request.state.database}

    @app.get("/ping")
    async def ping() -> dict[str, bool]:
        return {"pong": True}

    async with nextreq.AsyncClient(app=app, base_url="http://app") as client:
        assert events == []  # started with the first request
        assert (await client.get("/db")).json() == {"database": "connected"}
        assert (await client.get("/db")).status_code == 200
        assert events == ["startup"]  # once, not per request
    assert events == ["startup", "shutdown"]

    # lifespan=False leaves the handlers alone.
    events.clear()
    transport = ASGITransport(app, lifespan=False)
    async with nextreq.AsyncClient(transport=transport, base_url="http://app") as client:
        assert (await client.get("/ping")).json() == {"pong": True}
    assert events == []


async def test_asgi_lifespan_failures_and_unsupported_applications() -> None:
    import contextlib

    from fastapi import FastAPI

    @contextlib.asynccontextmanager
    async def broken(app: FastAPI) -> Any:
        raise RuntimeError("database unreachable")
        yield

    failing = FastAPI(lifespan=broken)
    async with nextreq.AsyncClient(app=failing, base_url="http://app") as client:
        with pytest.raises(RuntimeError, match="lifespan startup failed"):
            await client.get("/")

    async def bare(scope: dict[str, Any], receive: Any, send: Any) -> None:
        assert scope["type"] == "http"  # raises for the lifespan scope
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    # "auto" (the default) tolerates applications without lifespan support ...
    async with nextreq.AsyncClient(app=bare, base_url="http://app") as client:
        assert (await client.get("/")).status_code == 204
    # ... while lifespan=True insists on it.
    strict = ASGITransport(bare, lifespan=True)
    async with nextreq.AsyncClient(transport=strict, base_url="http://app") as client:
        with pytest.raises(RuntimeError, match="does not support the lifespan protocol"):
            await client.get("/")
    with pytest.raises(ValueError, match="lifespan must be"):
        ASGITransport(bare, lifespan="sometimes")  # type: ignore[arg-type]


def test_wsgi_response_is_consumed_lazily_and_closed_early() -> None:
    produced: list[int] = []
    closed: list[bool] = []

    def app(environ: dict[str, Any], start_response: Any) -> Any:
        def generate() -> Any:
            try:
                for n in range(1000):
                    produced.append(n)
                    yield f"chunk-{n}\n".encode()
            finally:
                closed.append(True)

        start_response("200 OK", [("Content-Type", "text/plain")])
        return generate()

    with nextreq.Client(app=app, base_url="http://app") as client:
        with client.stream("GET", "/") as response:
            assert response.status_code == 200
            chunks = response.iter_bytes()
            assert next(chunks) == b"chunk-0\n"
            assert next(chunks) == b"chunk-1\n"
            assert len(produced) <= 3  # nothing was generated ahead of the reader
        assert closed == [True]  # leaving the block closed the application's iterable
        assert len(produced) <= 3

        # Without stream=True the body is simply read to the end.
        produced.clear()
        assert client.get("/").text.count("chunk-") == 1000


def test_wsgi_streamed_server_sent_events(flask_app: Any) -> None:
    with nextreq.Client(app=flask_app, base_url="http://app") as client:
        with client.stream("GET", "/stream") as response:
            events = list(response.iter_sse())
    assert len(events) >= 2


def test_wsgi_legacy_write_callable_and_late_start_response() -> None:
    def writer(environ: dict[str, Any], start_response: Any) -> Any:
        write = start_response("200 OK", [("Content-Type", "text/plain")])
        write(b"written-")
        return [b"then-", b"iterated"]

    def late(environ: dict[str, Any], start_response: Any) -> Any:
        # start_response() is only called when the first chunk is requested.
        def generate() -> Any:
            start_response("201 Created", [("X-Late", "yes")])
            yield b"late"

        return generate()

    with nextreq.Client(app=writer, base_url="http://app") as client:
        assert client.get("/").content == b"written-then-iterated"
    with nextreq.Client(app=late, base_url="http://app") as client:
        response = client.get("/")
        assert (response.status_code, response.headers["x-late"], response.content) == (
            201,
            "yes",
            b"late",
        )
