"""``download()``: streamed to a ``.part`` file, renamed when complete, and
continued with ``Range`` / ``If-Range`` after a broken connection."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import nextreq
from nextreq import MockTransport

from .conftest import LiveServer

SIZE = 300_000


def _names(directory: Path) -> list[str]:
    return sorted(entry.name for entry in directory.iterdir())


def _ranges(server: LiveServer) -> list[str | None]:
    return [headers.get("range") for headers in server.file_requests]


# -- the plain case -----------------------------------------------------------


def test_download_writes_the_file(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    target = tmp_path / "file.bin"
    result = client.download("/file", target, params={"size": SIZE})
    assert result == target
    assert isinstance(result, Path)
    assert target.read_bytes() == server.file_bytes(SIZE)
    # No partial file or bookkeeping is left behind.
    assert _names(tmp_path) == ["file.bin"]
    # The bytes themselves are asked for, so offsets mean what they say.
    assert server.file_requests[0]["accept-encoding"] == "identity"
    assert "range" not in server.file_requests[0]


async def test_async_download_writes_the_file(
    server: LiveServer, aclient: nextreq.AsyncClient, tmp_path: Path
) -> None:
    size = 3 * 1024 * 1024 + 123  # several write batches and a remainder
    seen: list[tuple[int, int | None]] = []
    target = await aclient.download(
        f"/file?size={size}", tmp_path / "big.bin", on_progress=lambda *args: seen.append(args)
    )
    assert target.read_bytes() == server.file_bytes(size)
    assert _names(tmp_path) == ["big.bin"]
    assert seen[0] == (0, size)
    assert seen[-1] == (size, size)
    assert [done for done, _ in seen] == sorted({done for done, _ in seen})
    assert len(seen) >= 4


def test_download_accepts_a_string_path_and_replaces_an_old_file(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    target = tmp_path / "file.bin"
    target.write_bytes(b"an older copy")
    assert client.download(f"/file?size={SIZE}", str(target)) == target
    assert target.read_bytes() == server.file_bytes(SIZE)


def test_download_reports_progress(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    seen: list[tuple[int, int | None]] = []
    client.download(
        f"/file?size={SIZE}",
        tmp_path / "f",
        on_progress=lambda done, total: seen.append((done, total)),
    )
    assert seen[0] == (0, SIZE)
    assert seen[-1] == (SIZE, SIZE)
    assert all(total == SIZE for _, total in seen)
    assert [done for done, _ in seen] == sorted(done for done, _ in seen)


def test_download_of_an_error_status_writes_nothing(client: nextreq.Client, tmp_path: Path) -> None:
    with pytest.raises(nextreq.HTTPStatusError) as caught:
        client.download("/status/404", tmp_path / "missing.bin")
    assert caught.value.response.status_code == 404
    assert _names(tmp_path) == []


def test_module_level_download(server: LiveServer, tmp_path: Path) -> None:
    target = nextreq.download(f"{server.url}/file?size=5000", tmp_path / "f", trust_env=False)
    assert target.read_bytes() == server.file_bytes(5000)


def test_download_through_a_mock_transport(tmp_path: Path) -> None:
    mock = MockTransport()
    mock.get("https://example.com/data.csv").respond(text="a,b\n1,2\n")
    with nextreq.Client(transport=mock) as client:
        target = client.download("https://example.com/data.csv", tmp_path / "data.csv")
    assert target.read_text() == "a,b\n1,2\n"


def test_download_detects_a_short_body(tmp_path: Path) -> None:
    mock = MockTransport()
    mock.get("https://example.com/f").respond(headers={"content-length": "10"}, stream=[b"short"])
    with (
        nextreq.Client(transport=mock) as client,
        pytest.raises(nextreq.RemoteProtocolError, match="5 of 10 bytes"),
    ):
        client.download("https://example.com/f", tmp_path / "f")
    assert _names(tmp_path) == []


def test_a_compressed_transfer_is_decoded_and_not_resumable(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    """A server that compresses although it was asked not to."""
    target = client.download(f"/file?size={SIZE}&gzip=1", tmp_path / "f")
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _names(tmp_path) == ["f"]


# -- resuming a later call ----------------------------------------------------


def _interrupt(client: nextreq.Client, url: str, target: Path) -> int:
    """Start a download whose connection breaks; return the size of the part."""
    with pytest.raises(nextreq.TransportError):
        client.download(url, target)
    part = target.with_name(target.name + ".part")
    return part.stat().st_size if part.exists() else 0


def test_resume_continues_a_partial_file(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=resume"
    target = tmp_path / "file.bin"
    kept = _interrupt(client, url, target)
    assert 0 < kept <= 50000
    assert _names(tmp_path) == ["file.bin.part", "file.bin.part.json"]
    saved = json.loads((tmp_path / "file.bin.part.json").read_text())
    assert saved == {"url": f"{server.url}{url}", "validator": '"v1"'}

    seen: list[tuple[int, int | None]] = []
    client.download(url, target, resume=True, on_progress=lambda *args: seen.append(args))
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _names(tmp_path) == ["file.bin"]
    assert _ranges(server) == [None, f"bytes={kept}-"]
    assert server.file_requests[1]["if-range"] == '"v1"'
    # Progress starts where the part ended and knows the full size.
    assert seen[0] == (kept, SIZE)
    assert seen[-1] == (SIZE, SIZE)


def test_without_resume_a_partial_file_is_overwritten(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=fresh"
    target = tmp_path / "file.bin"
    assert _interrupt(client, url, target) > 0
    client.download(url, target)
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _ranges(server) == [None, None]
    assert _names(tmp_path) == ["file.bin"]


def test_resume_starts_again_when_the_file_changed(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=changed"
    target = tmp_path / "file.bin"
    kept = _interrupt(client, url, target)
    server.file_version = "2"

    client.download(url, target, resume=True)
    # The range was asked for, but ``If-Range`` no longer matched, so the
    # server sent the new file from the start; nothing of the old one is kept.
    assert _ranges(server) == [None, f"bytes={kept}-"]
    assert target.read_bytes() == server.file_bytes(SIZE, "2")
    assert target.read_bytes() != server.file_bytes(SIZE, "1")


def test_resume_with_last_modified_as_the_validator(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=date&validator=date"
    target = tmp_path / "file.bin"
    kept = _interrupt(client, url, target)
    assert kept > 0
    client.download(url, target, resume=True)
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert server.file_requests[1]["if-range"] == "Wed, 01 Jan 2025 00:00:00 GMT"


@pytest.mark.parametrize("validator", ["weak", "none"])
def test_a_part_without_a_strong_validator_is_not_kept(
    server: LiveServer, client: nextreq.Client, tmp_path: Path, validator: str
) -> None:
    """Without one, nothing proves that the rest belongs to the same file."""
    url = f"/file?size={SIZE}&drop_after=50000&key={validator}&validator={validator}"
    target = tmp_path / "file.bin"
    assert _interrupt(client, url, target) == 0
    assert _names(tmp_path) == []
    client.download(url, target, resume=True)
    assert _ranges(server) == [None, None]
    assert target.read_bytes() == server.file_bytes(SIZE)


def test_resume_when_the_server_ignores_ranges(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=noranges&no_ranges=1"
    target = tmp_path / "file.bin"
    kept = _interrupt(client, url, target)
    client.download(url, target, resume=True)
    assert _ranges(server) == [None, f"bytes={kept}-"]  # answered with a full 200
    assert target.read_bytes() == server.file_bytes(SIZE)


def test_resume_ignores_a_part_from_another_url(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    target = tmp_path / "file.bin"
    assert _interrupt(client, f"/file?size={SIZE}&drop_after=50000&key=other", target) > 0
    client.download(f"/file?size={SIZE}&other=1", target, resume=True)
    assert _ranges(server) == [None, None]
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _names(tmp_path) == ["file.bin"]


def test_resume_restarts_when_the_part_is_longer_than_the_file(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}"
    target = tmp_path / "file.bin"
    (tmp_path / "file.bin.part").write_bytes(b"x" * (SIZE + 10))
    (tmp_path / "file.bin.part.json").write_text(
        json.dumps({"url": f"{server.url}{url}", "validator": '"v1"'})
    )
    client.download(url, target, resume=True)
    # 416 Range Not Satisfiable, then the whole file.
    assert _ranges(server) == [f"bytes={SIZE + 10}-", None]
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _names(tmp_path) == ["file.bin"]


@pytest.mark.parametrize("meta", ["not json", "[1, 2]", '{"url": 5}', '{"validator": ""}'])
def test_resume_ignores_unusable_bookkeeping(
    server: LiveServer, client: nextreq.Client, tmp_path: Path, meta: str
) -> None:
    target = tmp_path / "file.bin"
    (tmp_path / "file.bin.part").write_bytes(b"x" * 100)
    (tmp_path / "file.bin.part.json").write_text(meta)
    client.download(f"/file?size={SIZE}", target, resume=True)
    assert _ranges(server) == [None]
    assert target.read_bytes() == server.file_bytes(SIZE)


def test_a_mismatched_content_range_is_an_error(tmp_path: Path) -> None:
    url = "https://example.com/f"
    (tmp_path / "f.part").write_bytes(b"12345")
    (tmp_path / "f.part.json").write_text(json.dumps({"url": url, "validator": '"v"'}))
    mock = MockTransport()
    mock.get(url).respond(206, content=b"67890", headers={"content-range": "bytes 0-4/10"})
    with (
        nextreq.Client(transport=mock) as client,
        pytest.raises(nextreq.RemoteProtocolError, match="Content-Range"),
    ):
        client.download(url, tmp_path / "f", resume=True)
    # The part is untouched, so nothing was appended at the wrong offset.
    assert (tmp_path / "f.part").read_bytes() == b"12345"


# -- picking up a broken connection within one call ---------------------------


def test_retries_continue_a_broken_download(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=twice&drops=2"
    policy = nextreq.RetryPolicy(3, backoff_factor=0.01)
    target = client.download(url, tmp_path / "file.bin", retries=policy)
    assert target.read_bytes() == server.file_bytes(SIZE)
    ranges = _ranges(server)
    assert len(ranges) == 3
    assert ranges[0] is None
    first, second = (int(str(value).removeprefix("bytes=").rstrip("-")) for value in ranges[1:])
    assert 0 < first < second < SIZE
    assert _names(tmp_path) == ["file.bin"]


async def test_async_retries_continue_a_broken_download(
    server: LiveServer, aclient: nextreq.AsyncClient, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=async&drops=2"
    policy = nextreq.RetryPolicy(3, backoff_factor=0.01)
    target = await aclient.download(url, tmp_path / "file.bin", retries=policy)
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert [value is None for value in _ranges(server)] == [True, False, False]
    assert _names(tmp_path) == ["file.bin"]


async def test_async_resume_continues_a_partial_file(
    server: LiveServer, aclient: nextreq.AsyncClient, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=async-resume"
    target = tmp_path / "file.bin"
    with pytest.raises(nextreq.TransportError):
        await aclient.download(url, target)
    kept = (tmp_path / "file.bin.part").stat().st_size
    assert 0 < kept <= 50000
    await aclient.download(url, target, resume=True)
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _ranges(server) == [None, f"bytes={kept}-"]


def test_retries_start_again_without_a_validator(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    """Nothing to check the rest against, so the retry asks for the whole file."""
    url = f"/file?size={SIZE}&drop_after=50000&key=weak-retry&validator=weak"
    policy = nextreq.RetryPolicy(2, backoff_factor=0.01)
    target = client.download(url, tmp_path / "file.bin", retries=policy)
    assert _ranges(server) == [None, None]
    assert target.read_bytes() == server.file_bytes(SIZE)
    assert _names(tmp_path) == ["file.bin"]


def test_retries_run_out_and_leave_the_part_for_later(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=always&drops=99"
    policy = nextreq.RetryPolicy(2, backoff_factor=0.01)
    with pytest.raises(nextreq.TransportError):
        client.download(url, tmp_path / "file.bin", retries=policy)
    assert len(server.file_requests) == 3  # the first try and two retries
    assert _names(tmp_path) == ["file.bin.part", "file.bin.part.json"]
    assert 0 < (tmp_path / "file.bin.part").stat().st_size < SIZE


def test_a_broken_download_is_not_continued_without_retries(
    server: LiveServer, client: nextreq.Client, tmp_path: Path
) -> None:
    url = f"/file?size={SIZE}&drop_after=50000&key=once"
    with pytest.raises(nextreq.TransportError):
        client.download(url, tmp_path / "file.bin")
    assert len(server.file_requests) == 1
