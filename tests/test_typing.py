"""NXRQ-302: static typing guarantees.

This file is checked with ``mypy --strict`` *and* executed by pytest.
``assert_type`` makes mypy fail if an inferred type ever regresses, and every
``# type: ignore[...]`` marks a call that must be *rejected* (strict mode
reports unused ignores, so those lines fail the check if mypy starts
accepting them).
"""

from __future__ import annotations

import dataclasses
import sys
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pydantic

import nextreq
from nextreq import (
    URL,
    AsyncClient,
    Client,
    Elapsed,
    Headers,
    MockTransport,
    Request,
    Response,
    RetryPolicy,
    SSEEvent,
    Timeout,
    WebSocket,
)
from nextreq._nextreq import HttpClient, ResponseStream, SSEParser

if sys.version_info >= (3, 11):
    from typing import assert_type
else:
    from typing_extensions import assert_type

BASE = "https://api.example.test"


class User(pydantic.BaseModel):
    id: int
    name: str


@dataclasses.dataclass
class Item:
    sku: str
    qty: int


def make_mock() -> MockTransport:
    mock = MockTransport()
    mock.route(url=f"{BASE}/users/1").respond(json={"id": 1, "name": "Ada"})
    mock.route(url=f"{BASE}/users").respond(
        json=[{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bo"}]
    )
    mock.route(url=f"{BASE}/items/1").respond(json={"sku": "abc", "qty": 3})
    mock.route(url=f"{BASE}/index").respond(json={"a": {"id": 1, "name": "Ada"}})
    mock.route(url=f"{BASE}/events").respond(sse=[SSEEvent(data="one"), "two"])
    return mock


def test_sync_client_overloads() -> None:
    with Client(transport=make_mock(), base_url=BASE) as client:
        assert_type(client, Client)

        response = client.get("/users/1")
        assert_type(response, Response)
        assert_type(response.status_code, int)
        assert_type(response.headers, Headers)
        assert_type(response.url, URL)
        assert_type(response.content, bytes)
        assert_type(response.text, str)
        assert_type(response.elapsed, Elapsed)
        assert_type(response.retry_count, int)
        assert_type(response.history, list[Response])
        assert_type(response.raise_for_status(), Response)
        assert_type(response.json(), Any)
        assert_type(response.json(model=User), User)
        assert_type(client.get("/users").json(model=list[User]), list[User])

        user = client.get("/users/1", model=User)
        assert_type(user, User)
        assert user.name == "Ada"

        users = client.get("/users", model=list[User])
        assert_type(users, list[User])
        assert [u.id for u in users] == [1, 2]

        item = client.get("/items/1", model=Item)
        assert_type(item, Item)
        assert item.qty == 3

        index = client.get("/index", model=dict[str, User])
        assert_type(index, dict[str, User])
        assert index["a"].id == 1

        assert_type(client.post("/users/1", json={"a": 1}), Response)
        assert_type(client.post("/users/1", json={"a": 1}, model=User), User)
        assert_type(client.put("/users/1", model=User), User)
        assert_type(client.patch("/users/1", model=User), User)
        assert_type(client.delete("/users/1", model=User), User)
        assert_type(client.delete("/users/1"), Response)
        assert_type(client.head("/users/1"), Response)
        assert_type(client.options("/users/1"), Response)
        assert_type(client.request("GET", "/users/1"), Response)
        assert_type(client.request("GET", "/users/1", model=User), User)

        request = client.build_request("GET", "/users/1", params={"a": 1}, timeout=3.0)
        assert_type(request, Request)
        assert_type(client.send(request), Response)

        with client.stream("GET", "/events") as streamed:
            assert_type(streamed, Response)
            assert_type(streamed.iter_bytes(), Iterator[bytes])
            assert_type(streamed.iter_lines(), Iterator[str])
            events = list(streamed.iter_sse())
            assert_type(events, list[SSEEvent])
            assert [e.data for e in events] == ["one", "two"]


async def test_async_client_overloads() -> None:
    async with AsyncClient(transport=make_mock(), base_url=BASE) as client:
        assert_type(client, AsyncClient)

        response = await client.get("/users/1")
        assert_type(response, Response)

        user = await client.get("/users/1", model=User)
        assert_type(user, User)

        users = await client.get("/users", model=list[User])
        assert_type(users, list[User])
        assert len(users) == 2

        assert_type(await client.post("/users/1", model=User, json={"x": 1}), User)
        assert_type(await client.put("/users/1"), Response)
        assert_type(await client.patch("/items/1", model=Item), Item)
        assert_type(await client.delete("/users/1"), Response)
        assert_type(await client.head("/users/1"), Response)
        assert_type(await client.options("/users/1"), Response)
        assert_type(await client.request("GET", "/items/1", model=Item), Item)
        assert_type(await client.send(client.build_request("GET", "/users/1")), Response)

        gathered = await client.gather(["/users/1", "/items/1"], max_concurrency=2)
        assert_type(gathered, list[Response])
        assert [r.status_code for r in gathered] == [200, 200]

        mixed = await client.gather(["/users/1"], return_exceptions=True)
        assert_type(mixed, list[Response | BaseException])

        async with client.stream("GET", "/events") as streamed:
            assert_type(streamed, Response)
            assert_type(streamed.aiter_bytes(), AsyncIterator[bytes])
            assert_type(streamed.aiter_sse(), AsyncIterator[SSEEvent])
            assert [e.data async for e in streamed.aiter_sse()] == ["one", "two"]


def test_config_types() -> None:
    timeout = Timeout(5, connect=1.0)
    assert_type(timeout, Timeout)
    assert_type(timeout.connect, float | None)
    policy = RetryPolicy(3, retry_statuses={429, 503}, jitter=0.5)
    assert_type(policy.max_attempts, int)
    assert_type(policy.retry_statuses, frozenset[int])
    assert_type(policy.delay(1, "3"), float)
    assert_type(nextreq.parse_retry_after("3"), float | None)
    assert_type(SSEParser().feed(b"data: x\n\n"), list[tuple[str, str, str | None, int | None]])
    assert_type(nextreq.__version__, str)
    assert_type(nextreq.HTTP3_ENABLED, bool)

    client = Client(
        transport=make_mock(),
        timeout=timeout,
        retries=policy,
        headers={"X-A": "1"},
        params=[("a", "b")],
        cookies={"c": "d"},
        auth=("user", "pass"),
        on_request=lambda request: None,
        on_response=[lambda response: None],
    )
    assert_type(client.timeout, Timeout | None)
    assert_type(client.retries, RetryPolicy | None)
    client.close()


def _static_only(engine: HttpClient, stream: ResponseStream, ws: WebSocket) -> None:
    """Never executed (it would hit the network); checked by mypy only."""
    assert_type(nextreq.get("https://example.com"), Response)
    assert_type(nextreq.get("https://example.com", model=User), User)
    assert_type(nextreq.post("https://example.com", json={}, model=list[Item]), list[Item])
    assert_type(nextreq.put("https://example.com", verify=False), Response)
    assert_type(nextreq.patch("https://example.com", model=Item), Item)
    assert_type(nextreq.delete("https://example.com", timeout=None), Response)
    assert_type(nextreq.head("https://example.com"), Response)
    assert_type(nextreq.options("https://example.com"), Response)
    assert_type(nextreq.request("GET", "https://example.com", retries=3), Response)
    assert_type(nextreq.request("GET", "https://example.com", model=User), User)

    assert_type(stream.read(), bytes | None)
    assert_type(stream.read_all(), bytes)
    assert_type(engine.is_closed, bool)
    assert_type(ws.subprotocol, str | None)

    # Each of these must be a type error.
    nextreq.get("https://example.com", nonsense=True)  # type: ignore[call-overload]
    nextreq.get(42)  # type: ignore[call-overload]
    Client().get("https://example.com", timeout="soon")  # type: ignore[call-overload]
    Client(timeout="soon")  # type: ignore[arg-type]
    Client(retries="many")  # type: ignore[arg-type]
    Timeout(connect="x")  # type: ignore[arg-type]
    RetryPolicy(max_attempts="3")  # type: ignore[arg-type]
    wrong: Item = Client().get("https://example.com", model=User)  # type: ignore[assignment]
    del wrong


async def _static_only_async(client: AsyncClient) -> None:
    """Never executed; checked by mypy only."""
    async with client.websocket("wss://example.com/ws") as socket:
        assert_type(socket, WebSocket)
        assert_type(await socket.receive(), str | bytes)
        assert_type(await socket.receive_text(), str)
        assert_type(await socket.receive_bytes(), bytes)
    assert_type(await nextreq.gather(["https://example.com"]), list[Response])
    await client.get("https://example.com", follow_redirects="yes")  # type: ignore[call-overload]


def test_static_only_helpers_exist() -> None:
    assert callable(_static_only) and callable(_static_only_async)
