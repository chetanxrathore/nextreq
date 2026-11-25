"""Streamed request bodies: generators, async generators and open files are
sent chunk by chunk instead of being read into memory first."""

from __future__ import annotations

import asyncio
import hashlib
import io
import sys
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

import nextreq
from nextreq import Request, RequestStream

from .conftest import LiveServer

CHUNK = bytes(range(256)) * 256  # 64 KiB


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _chunks(count: int) -> Iterator[bytes]:
    for n in range(count):
        yield CHUNK[n:] + CHUNK[:n]


def _expected(count: int) -> bytes:
    return b"".join(_chunks(count))


@pytest.fixture
def big_file(tmp_path: Path) -> Path:
    path = tmp_path / "payload.bin"
    path.write_bytes(_expected(40))  # 2.5 MiB
    return path


# -- how bodies are classified ----------------------------------------------------


def test_only_bodies_that_benefit_are_streamed(big_file: Path) -> None:
    # In memory already: sent as one buffer, with a length.
    for content in (
        b"abc",
        bytearray(b"abc"),
        memoryview(b"abc"),
        [b"a", b"bc"],
        io.BytesIO(b"abc"),
    ):
        request = Request("POST", "https://example.com/", content=content)
        assert request.stream is None
        assert request.content == b"abc"
        assert request.headers["content-length"] == "3"

    # Produced on demand: streamed, and nothing is read at construction time.
    generator = _chunks(3)
    request = Request("POST", "https://example.com/", content=generator)
    assert isinstance(request.stream, RequestStream)
    assert request.content == b""
    assert request.stream.length is None and "content-length" not in request.headers
    assert not request.stream.replayable
    assert next(generator) == CHUNK  # untouched until now

    with big_file.open("rb") as handle:
        request = Request("POST", "https://example.com/", content=handle)
        assert request.stream is not None and request.stream.replayable
        assert request.stream.length == big_file.stat().st_size
        assert request.headers["content-length"] == str(big_file.stat().st_size)
        assert handle.tell() == 0  # not read yet
        assert "bytes" in repr(request.stream)

        # A file that was partly read is sent from its current position.
        handle.seek(100)
        partial = Request("POST", "https://example.com/", content=handle)
        assert partial.headers["content-length"] == str(big_file.stat().st_size - 100)


def test_request_read_buffers_a_streamed_body() -> None:
    request = Request("POST", "https://example.com/", content=_chunks(2))
    assert request.read() == _expected(2)
    assert request.stream is None
    assert request.headers["content-length"] == str(2 * len(CHUNK))


# -- the sync client -------------------------------------------------------------


def test_generator_body_is_sent_with_chunked_encoding(client: nextreq.Client) -> None:
    seen = client.post("/digest", content=_chunks(20)).json()
    assert seen["size"] == 20 * len(CHUNK)
    assert seen["sha256"] == _digest(_expected(20))
    assert seen["transfer_encoding"] == "chunked"
    assert seen["content_length"] is None


def test_explicit_content_length_is_respected_for_generators(client: nextreq.Client) -> None:
    seen = client.post(
        "/digest", content=_chunks(3), headers={"Content-Length": str(3 * len(CHUNK))}
    ).json()
    assert seen["content_length"] == str(3 * len(CHUNK))
    assert seen["transfer_encoding"] is None
    assert seen["sha256"] == _digest(_expected(3))


def test_file_body_is_sent_with_its_length(client: nextreq.Client, big_file: Path) -> None:
    with big_file.open("rb") as handle:
        seen = client.put("/digest", content=handle).json()
    assert seen["method"] == "PUT"
    assert seen["content_length"] == str(big_file.stat().st_size)
    assert seen["transfer_encoding"] is None
    assert seen["sha256"] == _digest(big_file.read_bytes())


def test_multipart_streams_files_and_keeps_fields(client: nextreq.Client, tmp_path: Path) -> None:
    report = tmp_path / "report.csv"
    report.write_bytes(b"id,name\n1,Ada\n")
    with report.open("rb") as handle:
        request = client.build_request(
            "POST",
            "/upload",
            data={"title": "Q3", "tags": ["a", "b"]},
            files={"report": handle, "note": ("note.txt", b"in memory", "text/plain")},
        )
        assert request.stream is not None and request.stream.length is not None
        assert request.headers["content-length"] == str(request.stream.length)
        seen = client.send(request).json()
    assert seen["fields"]["title"] == "Q3"
    assert seen["files"]["report"] == {
        "filename": "report.csv",
        "content_type": "text/csv",
        "size": 14,
        "content": "id,name\n1,Ada\n",
    }
    assert seen["files"]["note"]["content"] == "in memory"


def test_large_multipart_upload_arrives_intact(client: nextreq.Client, big_file: Path) -> None:
    with big_file.open("rb") as handle:
        request = client.build_request("POST", "/digest", files={"file": ("big.bin", handle)})
        assert request.stream is not None
        expected_body = request.stream.read()  # reads the file once to know the answer ...
        seen = client.send(request).json()  # ... and the upload rewinds and sends it again
    assert seen["size"] == len(expected_body) == request.stream.length
    assert seen["sha256"] == _digest(expected_body)
    assert seen["content_type"].startswith("multipart/form-data; boundary=")


@pytest.mark.skipif(sys.platform == "win32", reason="needs the resource module")
def test_uploading_a_large_file_does_not_load_it_into_memory(
    client: nextreq.Client, tmp_path: Path
) -> None:
    import resource

    size_mib = 96
    path = tmp_path / "large.bin"
    with path.open("wb") as out:
        for _ in range(size_mib):
            out.write(b"\x5a" * (1024 * 1024))
    unit = 1 if sys.platform == "darwin" else 1024  # ru_maxrss: bytes on macOS, KiB elsewhere
    client.get("/json")  # warm up, so the baseline includes the client's own buffers
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    with path.open("rb") as handle:
        seen = client.post("/digest", content=handle, timeout=60).json()
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    assert seen["size"] == size_mib * 1024 * 1024
    # The test server runs in this process too and also reads in chunks, so
    # peak memory may grow by a few buffers - far less than the file.
    assert after - before < 32 * 1024 * 1024, f"peak RSS grew by {(after - before) >> 20} MiB"


def test_seekable_bodies_are_replayed_for_retries(
    client: nextreq.Client, server: LiveServer, big_file: Path
) -> None:
    with big_file.open("rb") as handle:
        # PUT, because the default retry policy only repeats idempotent methods.
        response = client.put(
            "/digest", params={"key": "file-retry", "fail": 2}, content=handle, retries=3
        )
    assert response.status_code == 200
    assert response.retry_count == 2
    assert response.json()["attempt"] == 3
    assert response.json()["sha256"] == _digest(big_file.read_bytes())


def test_one_shot_bodies_are_not_retried(client: nextreq.Client, server: LiveServer) -> None:
    response = client.put(
        "/digest", params={"key": "gen-retry", "fail": 2}, content=_chunks(2), retries=3
    )
    # Retrying would need the body again; the first answer is returned instead.
    assert response.status_code == 503
    assert response.retry_count == 0
    assert server.flaky["gen-retry"] == 1


def test_re_iterable_bodies_can_be_replayed(client: nextreq.Client) -> None:
    class Rows:
        def __iter__(self) -> Iterator[bytes]:
            return _chunks(3)

    response = client.put("/digest", params={"key": "rows", "fail": 1}, content=Rows(), retries=2)
    assert response.status_code == 200 and response.retry_count == 1
    assert response.json()["sha256"] == _digest(_expected(3))


def test_redirects_with_streamed_bodies(client: nextreq.Client, big_file: Path) -> None:
    # 307 keeps the method and body: a seekable file is rewound and sent again.
    with big_file.open("rb") as handle:
        response = client.post("/digest", params={"redirect": 307}, content=handle)
    assert response.json()["sha256"] == _digest(big_file.read_bytes())
    assert [r.status_code for r in response.history] == [307]

    # A generator cannot be produced twice, which is reported clearly.
    with pytest.raises(nextreq.StreamConsumed, match="one-shot stream"):
        client.post("/digest", params={"redirect": 307}, content=_chunks(2))

    # 303 turns the request into a GET, so the body is not needed again.
    response = client.post("/digest", params={"redirect": 303}, content=_chunks(2))
    assert response.json()["method"] == "GET" and response.json()["size"] == 0


def test_an_error_in_the_body_iterator_is_raised_as_is(client: nextreq.Client) -> None:
    def broken() -> Iterator[bytes]:
        yield CHUNK
        raise ValueError("source went away")

    with pytest.raises(ValueError, match="source went away"):
        client.post("/digest", content=broken())
    with pytest.raises(TypeError, match="must be bytes, not str"):
        client.post("/digest", content=iter(["text"]))
    # The client (and its pool) is fine afterwards.
    assert client.post("/digest", content=_chunks(1)).json()["size"] == len(CHUNK)


def test_a_slow_producer_does_not_trip_the_write_timeout(client: nextreq.Client) -> None:
    def slow() -> Iterator[bytes]:
        for n in range(4):
            time.sleep(0.25)  # longer than the write timeout below
            yield bytes([n]) * 1000

    timeout = nextreq.Timeout(5, write=0.1)
    seen = client.post("/digest", content=slow(), timeout=timeout).json()
    assert seen["size"] == 4000


def test_streamed_bodies_work_with_streaming_responses(client: nextreq.Client) -> None:
    with client.stream("POST", "/digest", content=_chunks(10)) as response:
        assert response.status_code == 200
        response.read()
        assert response.json()["sha256"] == _digest(_expected(10))


def test_in_process_transports_receive_the_whole_body(big_file: Path) -> None:
    mock = nextreq.MockTransport()
    route = mock.post("https://api.test/upload").respond(json={"ok": True})
    with nextreq.Client(transport=mock) as client, big_file.open("rb") as handle:
        client.post("https://api.test/upload", content=handle)
        client.post("https://api.test/upload", content=_chunks(2))
    first, second = (call.request for call in route.calls)
    assert first.content == big_file.read_bytes()
    assert second.content == _expected(2)
    assert second.headers["content-length"] == str(2 * len(CHUNK))


def test_async_iterables_need_the_async_client(client: nextreq.Client) -> None:
    async def body() -> AsyncIterator[bytes]:
        yield b"x"

    with pytest.raises(TypeError, match="AsyncClient"):
        client.post("/digest", content=body())


# -- the async client ------------------------------------------------------------


async def test_async_generator_body(aclient: nextreq.AsyncClient) -> None:
    async def body() -> AsyncIterator[bytes]:
        for chunk in _chunks(20):
            await asyncio.sleep(0)
            yield chunk

    seen = (await aclient.post("/digest", content=body())).json()
    assert seen["sha256"] == _digest(_expected(20))
    assert seen["transfer_encoding"] == "chunked"


async def test_async_client_accepts_sync_iterables_and_files(
    aclient: nextreq.AsyncClient, big_file: Path
) -> None:
    seen = (await aclient.post("/digest", content=_chunks(5))).json()
    assert seen["sha256"] == _digest(_expected(5))
    with big_file.open("rb") as handle:
        seen = (await aclient.post("/digest", content=handle)).json()
        assert seen["content_length"] == str(big_file.stat().st_size)
        assert seen["sha256"] == _digest(big_file.read_bytes())
        handle.seek(0)
        seen = (await aclient.post("/upload", files={"f": ("small.bin", handle)})).json()
    assert seen["files"]["f"]["size"] == big_file.stat().st_size


async def test_async_uploads_run_concurrently(aclient: nextreq.AsyncClient) -> None:
    async def body(seed: int) -> AsyncIterator[bytes]:
        for n in range(8):
            await asyncio.sleep(0.001)
            yield bytes([seed]) * (1000 + n)

    responses = await asyncio.gather(
        *(aclient.post("/digest", content=body(seed)) for seed in range(12))
    )
    for seed, response in enumerate(responses):
        expected = b"".join(bytes([seed]) * (1000 + n) for n in range(8))
        assert response.json()["sha256"] == _digest(expected)


async def test_async_body_errors_retries_and_redirects(
    aclient: nextreq.AsyncClient, big_file: Path
) -> None:
    async def broken() -> AsyncIterator[bytes]:
        yield CHUNK
        raise ValueError("async source went away")

    with pytest.raises(ValueError, match="async source went away"):
        await aclient.post("/digest", content=broken())

    with big_file.open("rb") as handle:
        response = await aclient.put(
            "/digest", params={"key": "async-file", "fail": 1}, content=handle, retries=2
        )
        assert response.status_code == 200 and response.retry_count == 1
        handle.seek(0)
        response = await aclient.post("/digest", params={"redirect": 308}, content=handle)
        assert response.json()["sha256"] == _digest(big_file.read_bytes())

    async def once() -> AsyncIterator[bytes]:
        yield b"x"

    with pytest.raises(nextreq.StreamConsumed):
        await aclient.post("/digest", params={"redirect": 307}, content=once())
    response = await aclient.put(
        "/digest", params={"key": "async-gen", "fail": 1}, content=once(), retries=2
    )
    assert response.status_code == 503 and response.retry_count == 0


async def test_cancelling_an_upload_stops_the_producer(aclient: nextreq.AsyncClient) -> None:
    produced = 0

    async def endless() -> AsyncIterator[bytes]:
        nonlocal produced
        while True:
            produced += 1
            await asyncio.sleep(0.005)
            yield CHUNK

    task = asyncio.ensure_future(aclient.post("/digest", content=endless()))
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    stopped_at = produced
    await asyncio.sleep(0.1)
    assert produced <= stopped_at + 1  # the generator is no longer being pulled
    assert (await aclient.get("/json")).status_code == 200


async def test_async_in_process_transport_receives_the_whole_body() -> None:
    async def body() -> AsyncIterator[bytes]:
        yield b"part one, "
        yield b"part two"

    seen: dict[str, Any] = {}

    def handler(request: Request) -> nextreq.Response:
        seen["content"] = request.content
        return nextreq.Response(200)

    mock = nextreq.MockTransport()
    mock.post("https://api.test/items").reply(handler)
    async with nextreq.AsyncClient(transport=mock) as client:
        await client.post("https://api.test/items", content=body())
    assert seen["content"] == b"part one, part two"


async def test_async_client_reads_files_off_the_event_loop(
    aclient: nextreq.AsyncClient, big_file: Path
) -> None:
    import threading

    readers: set[int] = set()

    class SlowFile(io.FileIO):
        def read(self, size: int | None = -1) -> bytes:
            readers.add(threading.get_ident())
            time.sleep(0.005)  # a slow disk
            return super().read(size)

    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.001)

    heartbeat = asyncio.ensure_future(ticker())
    try:
        with SlowFile(big_file, "rb") as handle:
            seen = (await aclient.put("/digest", content=handle)).json()
        assert seen["sha256"] == _digest(big_file.read_bytes())
        assert seen["content_length"] == str(big_file.stat().st_size)
        assert readers and threading.get_ident() not in readers
        assert ticks >= 5  # the loop kept running while the file was being read

        readers.clear()
        with SlowFile(big_file, "rb") as handle:  # multipart: the file part is read the same way
            seen = (await aclient.post("/upload", files={"f": ("f.bin", handle)})).json()
        assert seen["files"]["f"]["size"] == big_file.stat().st_size
        assert readers and threading.get_ident() not in readers
    finally:
        heartbeat.cancel()
