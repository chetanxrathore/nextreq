"""NXRQ-201 / NXRQ-301 / NXRQ-403: the asynchronous client and ``gather()``."""

from __future__ import annotations

import asyncio
import time

import pytest

import nextreq
from nextreq import Request, Response

from .conftest import COMPRESSIBLE, LiveServer, TLSServer

# -- basics -------------------------------------------------------------------


async def test_get_returns_response(aclient: nextreq.AsyncClient) -> None:
    response = await aclient.get("/json")
    assert isinstance(response, Response)
    assert response.status_code == 200
    assert response.json() == {"hello": "world"}
    assert response.http_version == "HTTP/1.1"
    assert response.elapsed.total > 0 and response.elapsed.ttfb is not None


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def test_all_verbs(aclient: nextreq.AsyncClient, method: str) -> None:
    response = await getattr(aclient, method.lower())("/echo")
    assert response.json()["method"] == method
    assert (await aclient.request(method, "/echo")).json()["method"] == method


async def test_head(aclient: nextreq.AsyncClient) -> None:
    response = await aclient.head("/echo")
    assert response.status_code == 200 and response.content == b""


async def test_bodies_params_and_headers(aclient: nextreq.AsyncClient) -> None:
    echoed = (
        await aclient.post(
            "/echo", params={"q": "x y"}, json={"a": [1, 2]}, headers={"X-Custom": "yes"}
        )
    ).json()
    assert echoed["query"] == {"q": ["x y"]}
    assert echoed["json"] == {"a": [1, 2]}
    assert echoed["headers"]["x-custom"] == "yes"
    assert (await aclient.post("/echo", data={"k": "v"})).json()["body"] == "k=v"
    upload = (await aclient.post("/upload", files={"f": ("a.txt", b"hi")}, data={"x": "1"})).json()
    assert upload["files"]["f"]["content"] == "hi" and upload["fields"] == {"x": "1"}


async def test_decompression(aclient: nextreq.AsyncClient) -> None:
    assert (await aclient.get("/encoded/gzip")).content == COMPRESSIBLE
    with pytest.raises(nextreq.DecodingError):
        await aclient.get("/corrupt-gzip")


async def test_cookies_redirects_and_auth(aclient: nextreq.AsyncClient) -> None:
    response = await aclient.get("/cookies/set?token=t1")
    assert response.json() == {"cookies": {"token": "t1"}}
    assert [r.status_code for r in response.history] == [302]
    assert aclient.cookies["token"] == "t1"
    assert (await aclient.get("/cookies")).json() == {"cookies": {"token": "t1"}}

    assert (await aclient.get("/basic-auth/u/p", auth=("u", "p"))).status_code == 200
    assert (await aclient.get("/basic-auth/u/p")).status_code == 401

    assert len((await aclient.get("/redirect/10")).history) == 10
    with pytest.raises(nextreq.TooManyRedirects):
        await aclient.get("/redirect/11")
    assert (await aclient.get("/redirect/2", follow_redirects=False)).status_code == 302
    kept = (await aclient.post("/redirect/1", params={"status": 307}, json={"k": 1})).json()
    assert kept["method"] == "POST" and kept["json"] == {"k": 1}
    dropped = (await aclient.post("/redirect/1", params={"status": 303}, json={"k": 1})).json()
    assert dropped["method"] == "GET" and dropped["json"] is None


async def test_raise_for_status(server: LiveServer) -> None:
    async with nextreq.AsyncClient(base_url=server.url, raise_for_status=True) as strict:
        with pytest.raises(nextreq.HTTPStatusError) as info:
            await strict.get("/status/404")
        assert info.value.status_code == 404
        assert (await strict.get("/status/404", raise_for_status=False)).status_code == 404
    async with nextreq.AsyncClient(base_url=server.url) as lenient:
        assert (await lenient.get("/status/500")).status_code == 500
        with pytest.raises(nextreq.HTTPStatusError):
            await lenient.get("/status/500", raise_for_status=True)


async def test_send_and_build_request(aclient: nextreq.AsyncClient) -> None:
    request = aclient.build_request("PUT", "/echo", content=b"payload")
    assert (await aclient.send(request)).json()["body"] == "payload"
    assert (await aclient.send(Request("GET", "/echo", params={"a": "b"}))).json()["query"] == {
        "a": ["b"]
    }


async def test_errors(aclient: nextreq.AsyncClient, unused_port: int) -> None:
    with pytest.raises(nextreq.ConnectError) as info:
        await aclient.get(f"http://127.0.0.1:{unused_port}/")
    assert info.value.request is not None
    with pytest.raises(nextreq.InvalidURL):
        await aclient.get("http://")
    with pytest.raises(nextreq.UnsupportedProtocol):
        await aclient.get("gopher://example.com/")
    with pytest.raises(nextreq.TransportError):
        await aclient.get("/drop")


async def test_closed_client(server: LiveServer) -> None:
    client = nextreq.AsyncClient(base_url=server.url)
    await client.aclose()
    await client.aclose()
    assert client.is_closed
    with pytest.raises(nextreq.ClientClosed):
        await client.get("/json")


async def test_http2_over_tls(tls_server: TLSServer) -> None:
    async with nextreq.AsyncClient(verify=tls_server.ca_path) as client:
        responses = await client.gather([f"{tls_server.url}/{n}" for n in range(20)])
    assert {r.http_version for r in responses} == {"HTTP/2"}
    assert [r.json()["path"] for r in responses] == [f"/{n}" for n in range(20)]


# -- streaming ----------------------------------------------------------------


async def test_stream_aiter_bytes(aclient: nextreq.AsyncClient) -> None:
    async with aclient.stream("GET", "/stream/5") as response:
        with pytest.raises(nextreq.ResponseNotRead):
            _ = response.content
        chunks = [chunk async for chunk in response.aiter_bytes()]
    assert b"".join(chunks) == b"chunk-0;chunk-1;chunk-2;chunk-3;chunk-4;"
    assert len(chunks) > 1
    with pytest.raises(nextreq.StreamConsumed):
        [chunk async for chunk in response.aiter_bytes()]


async def test_stream_aread_lines_text_and_chunk_size(aclient: nextreq.AsyncClient) -> None:
    async with aclient.stream("GET", "/stream/3") as response:
        assert await response.aread() == b"chunk-0;chunk-1;chunk-2;"
        assert response.text == "chunk-0;chunk-1;chunk-2;"
    async with aclient.stream("GET", "/lines") as response:
        assert [line async for line in response.aiter_lines()] == [
            "alpha",
            "beta",
            "gamma",
            "delta é",
            "last",
        ]
    async with aclient.stream("GET", "/lines") as response:
        assert (
            "".join([t async for t in response.aiter_text()])
            == "alpha\r\nbeta\ngamma\rdelta é\nlast"
        )
    async with aclient.stream("GET", "/stream/4") as response:
        sized = [chunk async for chunk in response.aiter_bytes(chunk_size=7)]
    assert all(len(c) == 7 for c in sized[:-1])
    assert b"".join(sized) == b"chunk-0;chunk-1;chunk-2;chunk-3;"


async def test_stream_closed_early_frees_the_connection_slot(server: LiveServer) -> None:
    async with nextreq.AsyncClient(
        base_url=server.url, max_connections=1, timeout=nextreq.Timeout(pool=2)
    ) as client:
        async with client.stream("GET", "/stream/50") as response:
            async for _ in response.aiter_bytes():
                break
        with pytest.raises(nextreq.StreamClosed):
            [chunk async for chunk in response.aiter_bytes()]
        assert (await client.get("/json")).status_code == 200


# -- concurrency: no deadlocks, no blocked loop, connections reused -------------


async def test_event_loop_is_not_blocked_during_io(aclient: nextreq.AsyncClient) -> None:
    """While a request is in flight, the asyncio loop must keep running."""
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.001)

    task = asyncio.create_task(ticker())
    try:
        started = time.perf_counter()
        response = await aclient.get("/delay/0.3")
        elapsed = time.perf_counter() - started
    finally:
        task.cancel()
    assert response.status_code == 200 and elapsed >= 0.3
    # A blocked loop would manage 0-1 ticks. The bar is low on purpose: timers
    # are coarse on Windows (~15 ms) and CI runners are slow.
    assert ticks >= 10, f"event loop only ran {ticks} ticks during a 300 ms request"


async def test_concurrent_requests_reuse_connections_and_never_deadlock(
    aclient: nextreq.AsyncClient, server: LiveServer
) -> None:
    """NXRQ-201 acceptance: 1,000 concurrent requests on a small pool."""
    semaphore = asyncio.Semaphore(50)

    async def one(n: int) -> int:
        async with semaphore:
            return int((await aclient.get("/slow", params={"delay": 0, "n": n})).json()["n"])

    results = await asyncio.wait_for(asyncio.gather(*(one(n) for n in range(1000))), timeout=60)
    assert results == list(range(1000))
    assert server.request_count == 1000
    # 1,000 requests must be served by at most `concurrency` pooled connections.
    assert len(server.peers) <= 50, f"{len(server.peers)} connections for 1000 requests"


async def test_max_connections_caps_in_flight_requests(server: LiveServer) -> None:
    async with nextreq.AsyncClient(base_url=server.url, max_connections=5) as client:
        responses = await asyncio.gather(
            *(client.get("/slow", params={"delay": 0.02, "n": n}) for n in range(60))
        )
    assert [r.json()["n"] for r in responses] == list(range(60))
    assert server.max_inflight <= 5
    assert len(server.peers) <= 5


async def test_cancellation_aborts_the_request_and_client_stays_usable(
    aclient: nextreq.AsyncClient,
) -> None:
    started = time.perf_counter()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(aclient.get("/delay/5"), timeout=0.2)
    assert time.perf_counter() - started < 2
    assert (await aclient.get("/json")).status_code == 200


async def test_sync_and_async_clients_can_coexist(server: LiveServer) -> None:
    """A blocking client used from a worker thread shares the same runtime."""
    async with nextreq.AsyncClient(base_url=server.url) as async_client:
        with nextreq.Client(base_url=server.url) as sync_client:
            sync_future = asyncio.to_thread(sync_client.get, "/delay/0.1")
            async_response, sync_response = await asyncio.gather(
                async_client.get("/delay/0.1"), sync_future
            )
    assert async_response.status_code == sync_response.status_code == 200


def test_async_client_works_across_event_loops(server: LiveServer) -> None:
    """The Tokio runtime outlives any single asyncio loop."""
    client = nextreq.AsyncClient(base_url=server.url)

    async def fetch() -> int:
        return (await client.get("/json")).status_code

    assert asyncio.run(fetch()) == 200
    assert asyncio.run(fetch()) == 200
    asyncio.run(client.aclose())


# -- gather() -----------------------------------------------------------------


async def test_gather_preserves_input_order(aclient: nextreq.AsyncClient) -> None:
    # Later requests finish first, so completion order is the reverse of input order.
    delays = [0.20, 0.15, 0.10, 0.05, 0.0]
    urls = [f"/slow?delay={delay}&n={n}" for n, delay in enumerate(delays)]
    responses = await aclient.gather(urls, max_concurrency=5)
    assert [r.json()["n"] for r in responses] == [0, 1, 2, 3, 4]
    assert all(isinstance(r, Response) for r in responses)


async def test_gather_respects_max_concurrency(
    aclient: nextreq.AsyncClient, server: LiveServer
) -> None:
    responses = await aclient.gather(
        [f"/slow?delay=0.02&n={n}" for n in range(40)], max_concurrency=4
    )
    assert [r.json()["n"] for r in responses] == list(range(40))
    assert 2 <= server.max_inflight <= 4

    server.reset()
    await aclient.gather([f"/slow?delay=0.02&n={n}" for n in range(30)])
    assert server.max_inflight <= 20, "default max_concurrency is 20"


async def test_gather_accepts_requests_urls_and_tuples(aclient: nextreq.AsyncClient) -> None:
    responses = await aclient.gather(
        [
            "/echo?kind=string",
            nextreq.URL("/echo?kind=url"),
            Request("POST", "/echo", json={"kind": "request"}),
            aclient.build_request("PUT", "/echo", content=b"built"),
            ("DELETE", "/echo"),
            ("PATCH", "/echo", {"json": {"kind": "tuple"}, "headers": {"X-T": "1"}}),
        ]
    )
    bodies = [r.json() for r in responses]
    assert [b["method"] for b in bodies] == ["GET", "GET", "POST", "PUT", "DELETE", "PATCH"]
    assert bodies[0]["query"] == {"kind": ["string"]} and bodies[1]["query"] == {"kind": ["url"]}
    assert bodies[2]["json"] == {"kind": "request"} and bodies[3]["body"] == "built"
    assert bodies[5]["json"] == {"kind": "tuple"} and bodies[5]["headers"]["x-t"] == "1"

    with pytest.raises(TypeError, match="gather"):
        await aclient.gather([42])  # type: ignore[list-item]


async def test_gather_empty_and_invalid_concurrency(aclient: nextreq.AsyncClient) -> None:
    assert await aclient.gather([]) == []
    with pytest.raises(ValueError):
        await aclient.gather(["/json"], max_concurrency=0)


async def test_gather_return_exceptions(aclient: nextreq.AsyncClient, unused_port: int) -> None:
    results = await aclient.gather(
        ["/json", f"http://127.0.0.1:{unused_port}/", "/status/500", "ftp://x/y", "/json"],
        return_exceptions=True,
    )
    assert isinstance(results[0], Response) and results[0].status_code == 200
    assert isinstance(results[1], nextreq.ConnectError)
    assert isinstance(results[2], Response) and results[2].status_code == 500
    assert isinstance(results[3], nextreq.UnsupportedProtocol)
    assert isinstance(results[4], Response)


async def test_gather_raises_first_error_and_cancels_the_rest(
    aclient: nextreq.AsyncClient, server: LiveServer, unused_port: int
) -> None:
    started = time.perf_counter()
    with pytest.raises(nextreq.ConnectError):
        await aclient.gather(
            [f"http://127.0.0.1:{unused_port}/", *[f"/delay/3?n={n}" for n in range(5)]],
            max_concurrency=6,
        )
    assert time.perf_counter() - started < 2, "pending requests should be cancelled, not awaited"
    assert (await aclient.get("/json")).status_code == 200


async def test_gather_applies_client_pipeline(server: LiveServer) -> None:
    """Redirects, cookies, retries and raise_for_status all apply inside gather()."""
    seen: list[str] = []
    async with nextreq.AsyncClient(
        base_url=server.url,
        retries=nextreq.RetryPolicy(3, backoff_factor=0),
        on_response=lambda r: seen.append(r.url.path),
    ) as client:
        responses = await client.gather(["/redirect/2", "/flaky/g1?fail=2", "/cookies/set?a=1"])
    assert [r.status_code for r in responses] == [200, 200, 200]
    assert len(responses[0].history) == 2
    assert responses[1].retry_count == 2
    assert client.cookies["a"] == "1"
    assert seen.count("/flaky/g1") == 3


async def test_module_level_gather(server: LiveServer) -> None:
    responses = await nextreq.gather(
        [f"{server.url}/slow?delay=0.01&n={n}" for n in range(10)], max_concurrency=3
    )
    assert [r.json()["n"] for r in responses] == list(range(10))
    assert server.max_inflight <= 3

    async with nextreq.AsyncClient(base_url=server.url) as client:
        mixed = await nextreq.gather(["/json", "ftp://x"], client=client, return_exceptions=True)
    assert isinstance(mixed[0], Response) and isinstance(mixed[1], nextreq.UnsupportedProtocol)
