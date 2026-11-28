"""Unix domain sockets: ``uds=`` sends every request to a socket file."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

import nextreq

from .conftest import LiveServer

unix_only = pytest.mark.skipif(sys.platform == "win32", reason="Unix domain sockets")


@pytest.fixture
def uds(server: LiveServer) -> str:
    assert server.uds is not None
    return server.uds


@unix_only
def test_requests_go_to_the_socket(server: LiveServer, uds: str) -> None:
    # The host in the URL is never resolved; it only names the Host header.
    with nextreq.Client(uds=uds, base_url="http://docker", trust_env=False) as client:
        assert client.get("/json").json() == {"hello": "world"}
        echoed = client.post("/echo", params={"a": 1}, json={"k": "v"}).json()
    assert echoed["headers"]["host"] == "docker"
    assert echoed["json"] == {"k": "v"}
    assert echoed["query"] == {"a": ["1"]}
    assert server.request_count == 2
    # Nothing came in over TCP.
    assert all(not peer for peer in server.peers)


@unix_only
def test_a_path_object_is_accepted(uds: str) -> None:
    with nextreq.Client(uds=Path(uds), trust_env=False) as client:
        assert client.get("http://localhost/json").status_code == 200


@unix_only
def test_streaming_redirects_and_keep_alive_over_the_socket(uds: str) -> None:
    with nextreq.Client(uds=uds, base_url="http://localhost", trust_env=False) as client:
        with client.stream("GET", "/stream/5") as response:
            assert len(list(response.iter_bytes())) >= 1
        followed = client.get("/redirect/2")
        assert followed.status_code == 200
        assert len(followed.history) == 2
        assert [client.get("/json").status_code for _ in range(20)] == [200] * 20


@unix_only
async def test_async_client_over_the_socket(server: LiveServer, uds: str) -> None:
    async with nextreq.AsyncClient(uds=uds, base_url="http://localhost", trust_env=False) as client:
        responses = await client.gather(["/json"] * 30, max_concurrency=10)
        assert [response.status_code for response in responses] == [200] * 30
        pages = [page.json()["page"] async for page in client.paginate("/pages?relative=1")]
        assert pages == [1, 2, 3]
    assert server.request_count == 33


@unix_only
def test_download_over_the_socket(server: LiveServer, uds: str, tmp_path: Path) -> None:
    with nextreq.Client(uds=uds, base_url="http://localhost", trust_env=False) as client:
        target = client.download("/file?size=200000", tmp_path / "f")
    assert target.read_bytes() == server.file_bytes(200000)


@unix_only
def test_websocket_over_the_socket(uds: str) -> None:
    with (
        nextreq.Client(uds=uds, trust_env=False) as client,
        client.websocket("ws://localhost/ws") as ws,
    ):
        ws.send("hello")
        assert ws.receive() == "echo:hello"


@unix_only
async def test_async_websocket_over_the_socket(uds: str) -> None:
    async with (
        nextreq.AsyncClient(uds=uds, trust_env=False) as client,
        client.websocket("ws://localhost/ws") as ws,
    ):
        await ws.send("hello")
        assert await ws.receive() == "echo:hello"


@unix_only
def test_the_proxy_environment_is_ignored(
    uds: str, monkeypatch: pytest.MonkeyPatch, unused_port: int
) -> None:
    """A socket is a direct connection; a proxy from the environment is not used."""
    for name in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(name, f"http://127.0.0.1:{unused_port}")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    with nextreq.Client(uds=uds, base_url="http://example.invalid") as client:
        assert client.get("/json").status_code == 200
        with client.websocket("ws://example.invalid/ws") as ws:
            ws.send("x")
            assert ws.receive() == "echo:x"


@unix_only
def test_a_missing_socket_is_a_connect_error(tmp_path: Path) -> None:
    missing = tmp_path / "nothing.sock"
    with nextreq.Client(uds=missing, trust_env=False) as client:
        with pytest.raises(nextreq.ConnectError):
            client.get("http://localhost/json")
        with pytest.raises(nextreq.ConnectError), client.websocket("ws://localhost/ws"):
            pass


def test_uds_cannot_be_combined_with_a_proxy() -> None:
    with pytest.raises(ValueError, match="uds"):
        nextreq.Client(uds="/tmp/x.sock", proxy="http://127.0.0.1:1")
    with pytest.raises(ValueError, match="uds"):
        nextreq.AsyncClient(uds="/tmp/x.sock", proxy={"all://": "http://127.0.0.1:1"})


@pytest.mark.skipif(sys.platform != "win32", reason="Windows has no Unix socket support here")
def test_uds_is_refused_on_windows() -> None:
    with pytest.raises(nextreq.UnsupportedProtocol, match="Unix domain sockets"):
        nextreq.Client(uds="C:\\temp\\x.sock")
