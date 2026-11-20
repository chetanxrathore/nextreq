"""Using nextreq across ``os.fork()``.

The I/O runtime's threads do not exist in a forked child. The extension is
told about the fork (``os.register_at_fork``) and starts a fresh runtime and
fresh connection pools there, so clients created before the fork keep working.
"""

from __future__ import annotations

import asyncio
import os
import select
import signal
import sys
import warnings

import pytest

import nextreq

from .conftest import LiveServer

pytestmark = pytest.mark.skipif(
    not hasattr(os, "fork") or sys.platform == "win32", reason="requires os.fork()"
)


def _run_in_forked_child(work: object, timeout: float = 30.0) -> str:
    """Fork, run ``work()`` in the child and return what it reported."""
    read_fd, write_fd = os.pipe()
    with warnings.catch_warnings():
        # CPython warns that forking a multi-threaded process is risky; that
        # risk is exactly what these tests exercise.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:  # the child
        report = "child did not finish"
        try:
            os.close(read_fd)
            report = str(work())  # type: ignore[operator]
        except BaseException as exc:
            report = f"{type(exc).__name__}: {exc}"
        finally:
            os.write(write_fd, report.encode())
            os._exit(0)
    os.close(write_fd)
    try:
        ready, _, _ = select.select([read_fd], [], [], timeout)
        if not ready:
            os.kill(pid, signal.SIGKILL)
            return "timed out waiting for the child"
        return os.read(read_fd, 65536).decode()
    finally:
        os.close(read_fd)
        os.waitpid(pid, 0)


def test_client_created_before_fork_works_in_the_child(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url) as client:
        # Start the runtime and leave a pooled keep-alive connection behind.
        assert client.get("/json").status_code == 200

        def child() -> str:
            first = client.get("/json", timeout=10)
            second = client.get("/json", timeout=10)
            return f"{first.status_code} {second.status_code}"

        assert _run_in_forked_child(child) == "200 200"
        # The parent is unaffected and keeps using its own pool.
        assert client.get("/json").status_code == 200


def test_module_level_helpers_and_new_clients_work_in_the_child(server: LiveServer) -> None:
    assert nextreq.get(f"{server.url}/json").status_code == 200  # shared engine, in the parent

    def child() -> str:
        shared = nextreq.get(f"{server.url}/json", timeout=10).status_code
        with nextreq.Client(base_url=server.url) as fresh:
            created = fresh.get("/json", timeout=10).status_code
        return f"{shared} {created}"

    assert _run_in_forked_child(child) == "200 200"


def test_async_client_and_streaming_work_in_the_child(server: LiveServer) -> None:
    assert nextreq.get(f"{server.url}/json").status_code == 200

    def child() -> str:
        async def main() -> str:
            async with nextreq.AsyncClient(base_url=server.url) as client:
                responses = await client.gather(["/json"] * 10, max_concurrency=5)
                async with client.stream("GET", "/sse") as response:
                    events = [event async for event in response.aiter_sse()]
                async with client.websocket("/ws") as ws:
                    await ws.send_text("forked")
                    echoed = await ws.receive(timeout=10)
            return f"{sum(r.status_code == 200 for r in responses)} {len(events) > 0} {echoed}"

        return asyncio.run(main())

    assert _run_in_forked_child(child) == "10 True echo:forked"


def test_grandchild_processes_work_too(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url) as client:
        assert client.get("/json").status_code == 200

        def child() -> str:
            assert client.get("/json", timeout=10).status_code == 200
            return _run_in_forked_child(lambda: client.get("/json", timeout=10).status_code)

        assert _run_in_forked_child(child) == "200"
