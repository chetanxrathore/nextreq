"""NXRQ-403: Server-Sent Events and WebSockets."""

from __future__ import annotations

import asyncio
import copy
import json
import pickle
import threading
import time
from typing import Any

import pytest

import nextreq
from nextreq import Response, SSEEvent
from nextreq._nextreq import SSEParser

from .conftest import SSE_FIXTURES, LiveServer, ProxyServer, TLSServer

MIXED_EVENTS = [
    SSEEvent(event="message", data="first", id=None, retry=None),
    SSEEvent(event="update", data="line one\nline two", id="42", retry=1500),
    SSEEvent(event="message", data='{"n": 3}', id="42", retry=None),
    SSEEvent(event="message", data="café 😀", id="42", retry=None),
]

# -- the Rust parser, driven from Python --------------------------------------


def test_parser_basic_event() -> None:
    parser = SSEParser()
    assert parser.feed(b"data: hello\n\n") == [SSEEvent("message", "hello", None, None)]
    assert parser.last_event_id is None and parser.retry is None


def test_parser_whatwg_rules() -> None:
    parser = SSEParser()
    events = parser.feed(SSE_FIXTURES["mixed"])
    parser.finish()
    assert events == MIXED_EVENTS
    assert all(isinstance(e, SSEEvent) for e in events)
    assert parser.last_event_id == "42" and parser.retry == 1500


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 5, 7, 16, 64])
def test_parser_is_independent_of_chunk_boundaries(chunk_size: int) -> None:
    payload = SSE_FIXTURES["mixed"]
    parser = SSEParser()
    events: list[SSEEvent] = []
    for start in range(0, len(payload), chunk_size):
        events.extend(parser.feed(payload[start : start + chunk_size]))
    assert events == MIXED_EVENTS


def test_parser_line_endings_and_bom() -> None:
    for newline in (b"\n", b"\r\n", b"\r"):
        parser = SSEParser()
        stream = b"\xef\xbb\xbf" + newline.join([b"data: a", b"data: b", b"", b"data: c", b"", b""])
        assert parser.feed(stream) == [SSEEvent(data="a\nb"), SSEEvent(data="c")]


def test_parser_field_edge_cases() -> None:
    parser = SSEParser()
    assert parser.feed(b"data:no-space\n\ndata:  two-spaces\n\ndata\n\n") == [
        SSEEvent(data="no-space"),
        SSEEvent(data=" two-spaces"),
        SSEEvent(data=""),
    ]
    # retry must be digits only; ids containing NUL are ignored; an empty id resets.
    assert parser.feed(b"retry: 5s\nid: a\x00b\ndata: x\n\nid: 7\ndata: y\n\nid\ndata: z\n\n") == [
        SSEEvent(data="x"),
        SSEEvent(data="y", id="7"),
        SSEEvent(data="z"),
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
    assert parser.feed(event.encode() + multi.encode()) == [event, SSEEvent(data="a\nb", id="9")]

    # Events are immutable value objects.
    assert repr(event) == """SSEEvent(event='update', data='{"a": 1}', id='9', retry=250)"""
    assert hash(event) == hash(SSEEvent("update", '{"a": 1}', "9", 250))
    assert event != SSEEvent("update", '{"a": 1}', "9", 251) and event != "update"
    assert len({event, SSEEvent("update", '{"a": 1}', "9", 250)}) == 1
    with pytest.raises(AttributeError):
        event.data = "changed"  # type: ignore[misc]
    assert pickle.loads(pickle.dumps(event)) == event
    assert copy.deepcopy(multi) == multi
    match event:
        case SSEEvent("update", data, "9", retry):
            assert data == '{"a": 1}' and retry == 250
        case _:
            raise AssertionError("structural pattern matching should work")


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


# -- WebSockets: compression, keep-alive, proxies ------------------------------


async def test_websocket_compression_is_opt_in(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws") as ws:
        assert ws.compressed is False
        assert "sec-websocket-extensions" not in ws.response_headers
        await ws.send_text("compressed?")
        assert await ws.receive() == "compressed:False"


async def test_websocket_permessage_deflate_roundtrip(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws", compression=True) as ws:
        assert ws.compressed is True
        assert "permessage-deflate" in ws.response_headers["sec-websocket-extensions"]
        await ws.send_text("compressed?")
        assert await ws.receive() == "compressed:True"

        # Highly compressible, then incompressible, then tiny: every message
        # must survive compression on the way out and decompression on the way in.
        repetitive = "nextreq " * 20_000
        await ws.send_text(repetitive)
        assert await ws.receive() == "echo:" + repetitive

        noisy = bytes((i * 7919) % 256 for i in range(200_000))
        await ws.send_bytes(noisy)
        assert await ws.receive() == noisy

        for n in range(20):  # the shared compression context carries over
            await ws.send_text(f"msg-{n}")
            assert await ws.receive() == f"echo:msg-{n}"


async def test_websocket_answers_server_pings(aclient: nextreq.AsyncClient) -> None:
    """The server drops the connection unless its pings are answered while idle."""
    async with aclient.websocket("/ws-heartbeat") as ws:
        await asyncio.sleep(0.7)  # several ping intervals with no application traffic
        await ws.send_text("still here")
        assert await ws.receive(timeout=5) == "echo:still here"


async def test_websocket_ping_payload_limit(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws") as ws:
        await ws.ping(b"x" * 125)
        with pytest.raises(nextreq.LocalProtocolError, match="125 bytes"):
            await ws.ping(b"x" * 126)


async def test_websocket_max_message_size(aclient: nextreq.AsyncClient) -> None:
    async with aclient.websocket("/ws", max_message_size=1024) as ws:
        await ws.send_bytes(b"x" * 512)
        assert await ws.receive() == b"x" * 512
        await ws.send_bytes(b"x" * 4096)
        with pytest.raises(nextreq.WebSocketError):
            await ws.receive(timeout=5)


async def test_websocket_through_an_http_proxy(server: LiveServer, proxy: ProxyServer) -> None:
    async with nextreq.AsyncClient(proxy=proxy.url, trust_env=False) as client:
        async with client.websocket(f"{server.ws_url}/ws") as ws:
            await ws.send_text("tunnelled")
            assert await ws.receive() == "echo:tunnelled"
    assert proxy.requests == [f"CONNECT 127.0.0.1:{server.port}"]


async def test_websocket_proxy_credentials(server: LiveServer, proxy: ProxyServer) -> None:
    proxy_url = f"http://user%40corp:p%3Ass@127.0.0.1:{proxy.port}"
    async with nextreq.AsyncClient(proxy=proxy_url, trust_env=False) as client:
        async with client.websocket(f"{server.ws_url}/ws") as ws:
            await ws.send_text("auth")
            assert await ws.receive() == "echo:auth"
    # base64("user@corp:p:ss")
    assert proxy.last_headers["proxy-authorization"] == "Basic dXNlckBjb3JwOnA6c3M="


async def test_websocket_proxy_from_environment(
    server: LiveServer, proxy: ProxyServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)
    monkeypatch.setenv("HTTP_PROXY", proxy.url)
    async with nextreq.AsyncClient() as client:
        async with client.websocket(f"{server.ws_url}/ws") as ws:
            await ws.send_text("env")
            assert await ws.receive() == "echo:env"
        assert len(proxy.requests) == 1

        # NO_PROXY exempts the host: the next connection goes direct.
        monkeypatch.setenv("NO_PROXY", "127.0.0.1")
        async with client.websocket(f"{server.ws_url}/ws") as ws:
            await ws.send_text("direct")
            assert await ws.receive() == "echo:direct"
        assert len(proxy.requests) == 1


async def test_websocket_proxy_refusal_is_a_proxy_error(server: LiveServer) -> None:
    # A plain HTTP server standing in for the proxy answers CONNECT with an error.
    async with nextreq.AsyncClient(proxy=server.url, trust_env=False) as client:
        with pytest.raises(nextreq.ProxyError, match="refused the tunnel"):
            async with client.websocket("ws://example.invalid/ws"):
                pass


async def test_websocket_over_tls(tls_server: TLSServer) -> None:
    async with nextreq.AsyncClient(verify=tls_server.ca_path) as client:
        async with client.websocket(f"wss://localhost:{tls_server.port}/ws-echo") as ws:
            await ws.send_text("secure")
            assert await ws.receive() == "tls:secure"
            await ws.send_bytes(b"\x00\xff" * 1000)
            assert await ws.receive() == b"\x00\xff" * 1000


# -- WebSockets: the blocking API ------------------------------------------------


def test_sync_websocket_text_bytes_and_json(client: nextreq.Client) -> None:
    with client.websocket("/ws") as ws:
        assert isinstance(ws, nextreq.SyncWebSocket)
        assert not ws.closed
        ws.send_text("hello")
        assert ws.receive() == "echo:hello"

        ws.send(b"raw")
        assert ws.receive_bytes() == b"raw"

        ws.send_json({"n": 1, "name": "Zoë"})
        assert json.loads(ws.receive_text().removeprefix("echo:")) == {"n": 1, "name": "Zoë"}

        ws.send_json([1, 2, 3], mode="binary")
        assert ws.receive_json() == [1, 2, 3]

        ws.ping(b"p")
    assert ws.closed and ws.close_code == 1000
    ws.close()  # safe to repeat
    with pytest.raises(nextreq.WebSocketDisconnect):
        ws.receive()
    with pytest.raises(nextreq.WebSocketError):
        ws.send_text("after close")


def test_sync_websocket_server_initiated_close_and_iteration(client: nextreq.Client) -> None:
    with client.websocket("/ws-close") as ws:
        assert list(ws) == ["one", "two"]
        assert ws.closed
        assert (ws.close_code, ws.close_reason) == (1001, "going away")

    with client.websocket("/ws") as ws:
        ws.send_text("close-me")
        with pytest.raises(nextreq.WebSocketDisconnect) as excinfo:
            ws.receive()
        assert excinfo.value.code == 4001
        assert excinfo.value.reason == "server says bye"


def test_sync_websocket_receive_timeout(client: nextreq.Client) -> None:
    with client.websocket("/ws") as ws:
        started = time.perf_counter()
        with pytest.raises(nextreq.ReadTimeout, match="waiting for a WebSocket message"):
            ws.receive(timeout=0.2)
        assert 0.15 < time.perf_counter() - started < 2.0
        # The connection is still usable after a timeout.
        ws.send_text("again")
        assert ws.receive(timeout=5) == "echo:again"


def test_sync_websocket_subprotocol_headers_and_compression(
    client: nextreq.Client, server: LiveServer
) -> None:
    with client.websocket("/ws", subprotocols=["json", "chat"], compression=True) as ws:
        assert ws.subprotocol in ("chat", "json")
        assert ws.compressed is True
        payload = "abc" * 50_000
        ws.send_text(payload)
        assert ws.receive() == "echo:" + payload

    with nextreq.Client(base_url=server.url, headers={"X-Client": "sync"}) as other:
        other.cookies["session"] = "abc"
        with other.websocket("/ws-headers", auth=("user", "pass")) as ws:
            seen = ws.receive_json()
    assert seen["x-client"] == "sync"
    assert seen["cookie"] == "session=abc"
    assert seen["authorization"].startswith("Basic ")


def test_sync_websocket_one_thread_receives_while_another_sends(client: nextreq.Client) -> None:
    with client.websocket("/ws") as ws:
        received: list[str | bytes] = []

        def reader() -> None:
            received.extend(ws.receive(timeout=10) for _ in range(20))

        thread = threading.Thread(target=reader)
        thread.start()
        time.sleep(0.05)  # let the reader block in receive()
        for n in range(20):
            ws.send_text(str(n))
        thread.join(10)
        assert not thread.is_alive()
    assert received == [f"echo:{n}" for n in range(20)]


def test_sync_websocket_through_a_proxy(server: LiveServer, proxy: ProxyServer) -> None:
    with nextreq.Client(proxy=proxy.url, trust_env=False) as client:
        with client.websocket(f"{server.ws_url}/ws") as ws:
            ws.send_text("sync tunnel")
            assert ws.receive() == "echo:sync tunnel"
    assert proxy.requests == [f"CONNECT 127.0.0.1:{server.port}"]


def test_sync_websocket_over_tls_through_a_proxy(tls_server: TLSServer, proxy: ProxyServer) -> None:
    """wss:// through a proxy: CONNECT first, then TLS to the origin inside the tunnel."""
    with nextreq.Client(verify=tls_server.ca_path, proxy=proxy.url, trust_env=False) as client:
        with client.websocket(f"wss://localhost:{tls_server.port}/ws-echo") as ws:
            ws.send_text("secure tunnel")
            assert ws.receive() == "tls:secure tunnel"
    assert proxy.requests == [f"CONNECT localhost:{tls_server.port}"]


def test_sync_websocket_errors(client: nextreq.Client, unused_port: int) -> None:
    with pytest.raises(nextreq.WebSocketError, match="HTTP 404"):
        client.websocket("/no-such-endpoint")
    with pytest.raises(nextreq.ConnectError):
        client.websocket(f"ws://127.0.0.1:{unused_port}/ws")
    with nextreq.Client(transport=nextreq.MockTransport()) as mocked:
        with pytest.raises(nextreq.WebSocketError, match="custom transport"):
            mocked.websocket("ws://example.com/ws")


# -- WebSockets: TLS proxies, refused handshakes and authentication ---------------


def test_sync_websocket_through_an_https_proxy(
    server: LiveServer, tls_server: TLSServer, tls_proxy: ProxyServer
) -> None:
    """TLS to the proxy, CONNECT inside it, then (for wss) TLS to the origin inside that."""
    assert tls_proxy.url.startswith("https://")
    with nextreq.Client(verify=tls_server.ca_path, proxy=tls_proxy.url, trust_env=False) as client:
        with client.websocket(f"wss://localhost:{tls_server.port}/ws-echo") as ws:
            ws.send_text("tls in tls")
            assert ws.receive() == "tls:tls in tls"
        with client.websocket(f"{server.ws_url}/ws") as ws:
            ws.send_text("plain inside tls")
            assert ws.receive() == "echo:plain inside tls"
    assert tls_proxy.requests == [
        f"CONNECT localhost:{tls_server.port}",
        f"CONNECT 127.0.0.1:{server.port}",
    ]


async def test_websocket_through_an_https_proxy(
    server: LiveServer, tls_server: TLSServer, tls_proxy: ProxyServer
) -> None:
    proxy_url = tls_proxy.url.replace("https://", "https://user:secret@")
    async with nextreq.AsyncClient(
        verify=tls_server.ca_path, proxy=proxy_url, trust_env=False
    ) as client:
        async with client.websocket(f"wss://localhost:{tls_server.port}/ws-echo") as ws:
            await ws.send_text("async tls in tls")
            assert await ws.receive() == "tls:async tls in tls"
    assert tls_proxy.last_headers["proxy-authorization"] == "Basic dXNlcjpzZWNyZXQ="

    # The proxy's own certificate is verified like any other.
    async with nextreq.AsyncClient(proxy=tls_proxy.url, trust_env=False) as client:
        with pytest.raises(nextreq.ConnectError):
            async with client.websocket(f"{server.ws_url}/ws"):
                pass


async def test_websocket_refusal_carries_the_response(aclient: nextreq.AsyncClient) -> None:
    with pytest.raises(nextreq.WebSocketError, match="HTTP 404") as refused:
        async with aclient.websocket("/no-such-endpoint"):
            pass
    response = refused.value.response
    assert response is not None and response.status_code == 404
    assert refused.value.request is not None and refused.value.request.url.scheme == "ws"

    with pytest.raises(nextreq.WebSocketError, match="HTTP 401") as denied:
        async with aclient.websocket("/ws-digest/ada/secret"):
            pass
    response = denied.value.response
    assert response is not None
    assert response.json() == {"authenticated": False}
    assert response.headers["www-authenticate"].startswith("Digest ")


async def test_websocket_digest_auth(aclient: nextreq.AsyncClient, server: LiveServer) -> None:
    auth = nextreq.DigestAuth("ada", "secret")
    async with aclient.websocket("/ws-digest/ada/secret", auth=auth) as ws:
        assert await ws.receive() == "authenticated"
        await ws.send_text("hi")
        assert await ws.receive() == "echo:hi"
    assert server.digest_requests == 2  # the challenge, then the answer

    # The challenge is remembered: the next handshake authenticates straight away.
    async with aclient.websocket("/ws-digest/ada/secret?x=1", auth=auth) as ws:
        assert await ws.receive() == "authenticated"
    assert server.digest_requests == 3

    with pytest.raises(nextreq.WebSocketError, match="HTTP 401") as denied:
        async with aclient.websocket(
            "/ws-digest/ada/secret", auth=nextreq.DigestAuth("ada", "wrong")
        ):
            pass
    assert denied.value.response is not None and denied.value.response.status_code == 401


def test_sync_websocket_digest_auth_from_the_client(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, auth=nextreq.DigestAuth("ada", "secret")) as client:
        with client.websocket("/ws-digest/ada/secret") as ws:
            assert ws.receive() == "authenticated"
        # The same credentials serve plain requests on the same client.
        assert client.get("/digest-auth/ada/secret").json()["authenticated"] is True
        with pytest.raises(nextreq.WebSocketError, match="HTTP 401"):
            client.websocket("/ws-digest/ada/other-password")


async def test_websocket_auth_flow_with_its_own_requests(
    aclient: nextreq.AsyncClient, server: LiveServer
) -> None:
    class TokenAuth(nextreq.Auth):
        """Fetches a token over HTTP before the handshake."""

        def auth_flow(self, request: nextreq.Request) -> Any:
            fetched = yield nextreq.Request("POST", f"{server.url}/echo", json={"token": "t0k3n"})
            token = fetched.json()["json"]["token"]
            request.headers["authorization"] = f"Bearer {token}"
            yield request

    class GivesUp(nextreq.Auth):
        def auth_flow(self, request: nextreq.Request) -> Any:
            yield nextreq.Request("GET", f"{server.url}/json")

    async with aclient.websocket("/ws-headers", auth=TokenAuth()) as ws:
        assert (await ws.receive_json())["authorization"] == "Bearer t0k3n"
    with nextreq.Client(base_url=server.url) as client:
        with client.websocket("/ws-headers", auth=TokenAuth()) as sync_ws:
            assert sync_ws.receive_json()["authorization"] == "Bearer t0k3n"
        with pytest.raises(nextreq.WebSocketError, match="auth flow ended"):
            client.websocket("/ws-headers", auth=GivesUp())
    with pytest.raises(nextreq.WebSocketError, match="auth flow ended"):
        async with aclient.websocket("/ws-headers", auth=GivesUp()):
            pass
