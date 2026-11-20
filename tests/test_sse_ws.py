"""NXRQ-403: Server-Sent Events and WebSockets."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

import nextreq
from nextreq import Response, SSEEvent
from nextreq._nextreq import SSEParser

from .conftest import SSE_FIXTURES, LiveServer, TLSServer

MIXED_EVENTS = [
    SSEEvent(event="message", data="first", id=None, retry=None),
    SSEEvent(event="update", data="line one\nline two", id="42", retry=1500),
    SSEEvent(event="message", data='{"n": 3}', id="42", retry=None),
    SSEEvent(event="message", data="café 😀", id="42", retry=None),
]

# -- the Rust parser, driven from Python --------------------------------------


def test_parser_basic_event() -> None:
    parser = SSEParser()
    assert parser.feed(b"data: hello\n\n") == [("message", "hello", None, None)]
    assert parser.last_event_id is None and parser.retry is None


def test_parser_whatwg_rules() -> None:
    parser = SSEParser()
    events = parser.feed(SSE_FIXTURES["mixed"])
    parser.finish()
    assert [SSEEvent(*e) for e in events] == MIXED_EVENTS
    assert parser.last_event_id == "42" and parser.retry == 1500


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 5, 7, 16, 64])
def test_parser_is_independent_of_chunk_boundaries(chunk_size: int) -> None:
    payload = SSE_FIXTURES["mixed"]
    parser = SSEParser()
    events: list[tuple[str, str, str | None, int | None]] = []
    for start in range(0, len(payload), chunk_size):
        events.extend(parser.feed(payload[start : start + chunk_size]))
    assert [SSEEvent(*e) for e in events] == MIXED_EVENTS


def test_parser_line_endings_and_bom() -> None:
    for newline in (b"\n", b"\r\n", b"\r"):
        parser = SSEParser()
        stream = b"\xef\xbb\xbf" + newline.join([b"data: a", b"data: b", b"", b"data: c", b"", b""])
        assert parser.feed(stream) == [
            ("message", "a\nb", None, None),
            ("message", "c", None, None),
        ]


def test_parser_field_edge_cases() -> None:
    parser = SSEParser()
    assert parser.feed(b"data:no-space\n\ndata:  two-spaces\n\ndata\n\n") == [
        ("message", "no-space", None, None),
        ("message", " two-spaces", None, None),
        ("message", "", None, None),
    ]
    # retry must be digits only; ids containing NUL are ignored; an empty id resets.
    assert parser.feed(b"retry: 5s\nid: a\x00b\ndata: x\n\nid: 7\ndata: y\n\nid\ndata: z\n\n") == [
        ("message", "x", None, None),
        ("message", "y", "7", None),
        ("message", "z", None, None),
    ]
    assert parser.retry is None


def test_sse_event_model() -> None:
    event = SSEEvent(event="update", data='{"a": 1}', id="9", retry=250)
    assert event.json() == {"a": 1}
    assert event.encode() == b'event: update\nid: 9\nretry: 250\ndata: {"a": 1}\n\n'
    assert SSEEvent().event == "message" and SSEEvent(data="x") == SSEEvent("message", "x")
    multi = SSEEvent(data="a\nb")
    assert multi.encode() == b"data: a\ndata: b\n\n"
    # encode() output parses back to the same event.
    parser = SSEParser()
    assert [SSEEvent(*e) for e in parser.feed(event.encode() + multi.encode())] == [
        event,
        SSEEvent(data="a\nb", id="9"),
    ]


# -- response.iter_sse() ------------------------------------------------------


def test_iter_sse(client: nextreq.Client) -> None:
    with client.stream("GET", "/sse", params={"count": 5}) as response:
        assert response.headers["content-type"] == "text/event-stream"
        events = list(response.iter_sse())
    assert len(events) == 5
    assert all(isinstance(e, SSEEvent) for e in events)
    assert [e.event for e in events] == ["tick"] * 5
    assert [e.id for e in events] == ["0", "1", "2", "3", "4"]
    assert [e.json() for e in events] == [{"n": n} for n in range(5)]
    assert all(e.retry is None for e in events)
    assert response.is_closed


@pytest.mark.parametrize("chunk", [0, 1, 3, 11])
def test_iter_sse_whatwg_fixture_over_http(client: nextreq.Client, chunk: int) -> None:
    with client.stream("GET", "/sse-fixture/mixed", params={"chunk": chunk}) as response:
        assert list(response.iter_sse()) == MIXED_EVENTS


def test_iter_sse_events_arrive_incrementally(client: nextreq.Client) -> None:
    """Events must be yielded as they arrive, not after the stream ends."""
    arrival: list[float] = []
    started = time.perf_counter()
    with client.stream("GET", "/sse", params={"count": 4, "delay": 0.1}) as response:
        for _ in response.iter_sse():
            arrival.append(time.perf_counter() - started)
    assert len(arrival) == 4
    assert arrival[0] < 0.25, "first event should not wait for the whole stream"
    assert arrival[-1] - arrival[0] >= 0.2


def test_iter_sse_many_events(client: nextreq.Client) -> None:
    with client.stream("GET", "/sse", params={"count": 5000}) as response:
        total = 0
        for index, event in enumerate(response.iter_sse()):
            assert event.id == str(index)
            total += 1
    assert total == 5000


def test_iter_sse_can_stop_early_and_cannot_restart(client: nextreq.Client) -> None:
    with client.stream("GET", "/sse", params={"count": 1000, "delay": 0.001}) as response:
        for event in response.iter_sse():
            if event.id == "2":
                break
    assert response.is_closed
    with pytest.raises(nextreq.StreamClosed):
        list(response.iter_sse())
    assert client.get("/json").status_code == 200


def test_iter_sse_on_a_buffered_response(client: nextreq.Client) -> None:
    response = client.get("/sse", params={"count": 3})
    assert [e.id for e in response.iter_sse()] == ["0", "1", "2"]
    assert [e.id for e in response.iter_sse()] == ["0", "1", "2"], (
        "buffered bodies can be re-parsed"
    )
    built = Response(200, content=b"data: a\n\ndata: b\n\n")
    assert [e.data for e in built.iter_sse()] == ["a", "b"]


def test_module_level_stream_with_sse(server: LiveServer) -> None:
    with nextreq.stream("GET", f"{server.url}/sse", params={"count": 2}) as response:
        assert [e.json()["n"] for e in response.iter_sse()] == [0, 1]


async def test_aiter_sse(aclient: nextreq.AsyncClient) -> None:
    async with aclient.stream("GET", "/sse", params={"count": 5}) as response:
        events = [event async for event in response.aiter_sse()]
    assert [e.id for e in events] == ["0", "1", "2", "3", "4"]
    assert [e.event for e in events] == ["tick"] * 5
    assert [e.json()["n"] for e in events] == list(range(5))


@pytest.mark.parametrize("chunk", [0, 1, 3, 11])
async def test_aiter_sse_whatwg_fixture_over_http(aclient: nextreq.AsyncClient, chunk: int) -> None:
    async with aclient.stream("GET", "/sse-fixture/mixed", params={"chunk": chunk}) as response:
        assert [event async for event in response.aiter_sse()] == MIXED_EVENTS


async def test_aiter_sse_incremental_and_early_exit(aclient: nextreq.AsyncClient) -> None:
    started = time.perf_counter()
    async with aclient.stream("GET", "/sse", params={"count": 50, "delay": 0.05}) as response:
        seen = 0
        async for event in response.aiter_sse():
            seen += 1
            if event.id == "2":
                break
    assert seen == 3
    assert time.perf_counter() - started < 1.5, "must not wait for all 50 events"
    with pytest.raises(nextreq.StreamClosed):
        [event async for event in response.aiter_sse()]
    assert (await aclient.get("/json")).status_code == 200


async def test_concurrent_sse_streams(aclient: nextreq.AsyncClient) -> None:
    async def consume(count: int) -> int:
        async with aclient.stream("GET", "/sse", params={"count": count, "delay": 0.001}) as r:
            return len([e async for e in r.aiter_sse()])

    assert await asyncio.gather(*(consume(n) for n in (10, 20, 30, 40))) == [10, 20, 30, 40]


async def test_sse_read_timeout_between_events(aclient: nextreq.AsyncClient) -> None:
    async with aclient.stream(
        "GET", "/sse", params={"count": 3, "delay": 1.0}, timeout=nextreq.Timeout(read=0.2)
    ) as response:
        with pytest.raises(nextreq.ReadTimeout):
            [event async for event in response.aiter_sse()]


# -- WebSockets ---------------------------------------------------------------


async def test_websocket_text_bytes_and_json(
    aclient: nextreq.AsyncClient, server: LiveServer
) -> None:
    async with aclient.websocket(f"{server.ws_url}/ws") as ws:
        assert isinstance(ws, nextreq.WebSocket)
        assert not ws.closed

        await ws.send_text("hello")
        assert await ws.receive() == "echo:hello"

        await ws.send("via send()")
        assert await ws.receive_text() == "echo:via send()"

        await ws.send_bytes(b"\x00\x01\xff")
        assert await ws.receive() == b"\x00\x01\xff"

        await ws.send(b"raw")
        assert await ws.receive_bytes() == b"raw"

        await ws.send_json({"n": 1, "name": "Zoë"})
        echoed = await ws.receive_text()
        assert json.loads(echoed.removeprefix("echo:")) == {"n": 1, "name": "Zoë"}

        await ws.send_json([1, 2, 3], mode="binary")
        assert await ws.receive_json() == [1, 2, 3]

        await ws.ping(b"p")
    assert ws.closed and ws.close_code == 1000


async def test_websocket_url_derived_from_base_url(aclient: nextreq.AsyncClient) -> None:
    """A relative URL is joined with the client's http:// base URL and upgraded to ws://."""
    async with aclient.websocket("/ws") as ws:
        await ws.send_text("relative")
        assert await ws.receive() == "echo:relative"


async def test_websocket_awaitable_form_and_explicit_close(
    aclient: nextreq.AsyncClient, server: LiveServer
) -> None:
    ws = await aclient.websocket(f"{server.ws_url}/ws")
    await ws.send_text("x")
    assert await ws.receive() == "echo:x"
    await ws.close(code=1000, reason="done")
    await ws.close()  # idempotent
    assert ws.closed and ws.close_code == 1000 and ws.close_reason == "done"
    with pytest.raises(nextreq.WebSocketDisconnect):
        await ws.receive()


async def test_websocket_server_initiated_close(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws") as ws:
        await ws.send_text("close-me")
        with pytest.raises(nextreq.WebSocketDisconnect) as info:
            await ws.receive()
        assert info.value.code == 4001 and info.value.reason == "server says bye"
        assert isinstance(info.value, nextreq.WebSocketError)
        assert isinstance(info.value, nextreq.TransportError)
        assert ws.closed and ws.close_code == 4001 and ws.close_reason == "server says bye"
        with pytest.raises(nextreq.WebSocketDisconnect):
            await ws.receive()


async def test_websocket_async_iteration_stops_on_close(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws-close") as ws:
        messages = [message async for message in ws]
    assert messages == ["one", "two"]
    assert ws.close_code == 1001 and ws.close_reason == "going away"


async def test_websocket_subprotocol_negotiation(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws", subprotocols=["json", "chat"]) as ws:
        assert ws.subprotocol in ("json", "chat")
        assert ws.response_headers["upgrade"].lower() == "websocket"
    async with aclient.websocket("/ws") as plain:
        assert plain.subprotocol is None


async def test_websocket_sends_client_headers_cookies_and_auth(server: LiveServer) -> None:
    async with nextreq.AsyncClient(
        base_url=server.url,
        headers={"X-Client": "c"},
        cookies={"session": "s1"},
        auth=nextreq.BearerAuth("tok"),
    ) as client:
        async with client.websocket("/ws-headers", headers={"X-Extra": "e"}) as ws:
            headers = await ws.receive_json()
    assert headers["x-client"] == "c" and headers["x-extra"] == "e"
    assert headers["authorization"] == "Bearer tok"
    assert headers["cookie"] == "session=s1"
    assert headers["user-agent"].startswith("nextreq/")
    assert headers["upgrade"].lower() == "websocket"


async def test_websocket_receive_timeout(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws") as ws:
        started = time.perf_counter()
        with pytest.raises(nextreq.ReadTimeout):
            await ws.receive(timeout=0.2)
        assert time.perf_counter() - started < 1.5
        # The connection survives a receive timeout.
        await ws.send_text("still alive")
        assert await ws.receive(timeout=2) == "echo:still alive"


async def test_websocket_concurrent_send_and_receive(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws") as ws:

        async def producer() -> None:
            for n in range(50):
                await ws.send_text(str(n))

        async def consumer() -> list[str]:
            return [str(await ws.receive()) for _ in range(50)]

        _, received = await asyncio.gather(producer(), consumer())
    assert received == [f"echo:{n}" for n in range(50)]


async def test_websocket_large_message(aclient: nextreq.AsyncClient) -> None:
    payload = bytes(range(256)) * 4096  # 1 MiB
    async with aclient.websocket("/ws") as ws:
        await ws.send_bytes(payload)
        assert await ws.receive() == payload


async def test_websocket_handshake_failures(
    aclient: nextreq.AsyncClient, server: LiveServer, unused_port: int
) -> None:
    with pytest.raises(nextreq.WebSocketError, match="HTTP 404"):
        async with aclient.websocket("/no-such-endpoint"):
            pass
    with pytest.raises(nextreq.WebSocketError, match="HTTP 200"):
        async with aclient.websocket("/json"):  # a plain HTTP endpoint
            pass
    with pytest.raises(nextreq.ConnectError):
        await aclient.websocket(f"ws://127.0.0.1:{unused_port}/ws")
    with pytest.raises(nextreq.UnsupportedProtocol):
        await aclient.websocket("ftp://127.0.0.1/ws")


async def test_websocket_not_available_on_custom_transports() -> None:
    async with nextreq.AsyncClient(transport=nextreq.MockTransport()) as client:
        with pytest.raises(nextreq.WebSocketError, match="custom transport"):
            client.websocket("ws://example.test/ws")
    closed = nextreq.AsyncClient()
    await closed.aclose()
    with pytest.raises(nextreq.ClientClosed):
        closed.websocket("ws://example.test/ws")


async def test_websocket_tls_handshake_uses_client_tls_settings(tls_server: TLSServer) -> None:
    """wss:// connections honour verify=...; an untrusted certificate must fail."""
    async with nextreq.AsyncClient() as strict:
        with pytest.raises(nextreq.ConnectError):
            await strict.websocket(f"wss://localhost:{tls_server.port}/ws")
    # With the CA trusted the TLS handshake succeeds and the (non-WebSocket)
    # test endpoint answers with a plain HTTP response instead.
    async with nextreq.AsyncClient(verify=tls_server.ca_path) as trusting:
        with pytest.raises(nextreq.WebSocketError, match="HTTP"):
            await trusting.websocket(f"wss://localhost:{tls_server.port}/ws")
