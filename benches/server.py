"""Fallback benchmark target server (pure Python, asyncio).

`run_benchmarks.py` prefers the Rust server in `benches/server.rs` because it
is fast enough never to be the bottleneck. This asyncio implementation serves
exactly the same endpoints and is used when no Rust toolchain is available:

* ``GET /json``          - small JSON document
* ``GET /user/{id}``     - a richer JSON object for typed deserialization
* ``GET /large?size=N``  - N bytes of payload (default 1 MiB)
* ``GET /sse?count=N``   - N Server-Sent Events, streamed with chunked encoding
* ``GET /health``        - liveness probe

Run standalone with ``python benches/server.py --port 8765``. With ``--port 0``
an ephemeral port is chosen and announced on stdout as ``LISTENING <port>``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from urllib.parse import parse_qs

SMALL_JSON = (
    b'{"id":1,"name":"Ada Lovelace","email":"ada@example.com","active":true,'
    b'"score":98.6,"tags":["math","engine","notes"]}'
)
DEFAULT_LARGE_SIZE = 1024 * 1024
MAX_LARGE_SIZE = 64 * 1024 * 1024
SSE_EVENTS_PER_CHUNK = 16


def user_json(user_id: int) -> bytes:
    """Mirror of ``user_json`` in server.rs (same shape and field types)."""
    orders = [
        {
            "order_id": user_id * 1000 + n,
            "sku": f"SKU-{(user_id * 7 + n * 13) % 100_000:05d}",
            "quantity": 1 + (n % 5),
            "unit_price": round(9.99 + n * 3.5, 2),
            "shipped": n % 2 == 0,
        }
        for n in range(10)
    ]
    document = {
        "id": user_id,
        "username": f"user{user_id}",
        "email": f"user{user_id}@example.com",
        "full_name": f"Benchmark User {user_id}",
        "active": user_id % 2 == 0,
        "score": round(50.0 + (user_id % 50), 2),
        "created_at": "2026-01-02T03:04:05+00:00",
        "address": {
            "street": f"{user_id} Main Street",
            "city": "Springfield",
            "country": "US",
            "zip": f"{user_id % 100_000:05d}",
        },
        "tags": ["alpha", "beta", "gamma", "delta"],
        "preferences": {"theme": "dark", "language": "en", "notifications": True},
        "orders": orders,
    }
    return json.dumps(document, separators=(",", ":")).encode()


def sse_chunk(start: int, end: int) -> bytes:
    return "".join(
        f'id: {n}\nevent: tick\ndata: {{"seq":{n},"symbol":"NXRQ",'
        f'"price":{100.0 + (n % 500) * 0.25:.2f},"volume":{1000 + (n % 97) * 13}}}\n\n'
        for n in range(start, end)
    ).encode()


def response(status: str, content_type: str, body: bytes) -> bytes:
    head = (
        f"HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {len(body)}\r\n\r\n"
    )
    return head.encode("ascii") + body


class BenchProtocol(asyncio.Protocol):
    """A minimal keep-alive HTTP/1.1 server: GET requests without bodies."""

    def __init__(self) -> None:
        self.transport: asyncio.Transport | None = None
        self.buffer = b""
        self.streaming: asyncio.Task[None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self.transport = transport

    def connection_lost(self, exc: Exception | None) -> None:
        if self.streaming is not None:
            self.streaming.cancel()

    def data_received(self, data: bytes) -> None:
        self.buffer += data
        while self.streaming is None:
            end = self.buffer.find(b"\r\n\r\n")
            if end < 0:
                return
            head, self.buffer = self.buffer[:end], self.buffer[end + 4 :]
            self.handle(head)

    def handle(self, head: bytes) -> None:
        assert self.transport is not None
        request_line = head.split(b"\r\n", 1)[0].decode("latin-1")
        try:
            method, target, _version = request_line.split(" ", 2)
        except ValueError:
            self.transport.write(response("400 Bad Request", "text/plain", b"bad request"))
            return
        if method not in ("GET", "HEAD"):
            self.transport.write(
                response("405 Method Not Allowed", "text/plain", b"method not allowed")
            )
            return
        path, _, query = target.partition("?")
        params = parse_qs(query)
        if path == "/json":
            self.transport.write(response("200 OK", "application/json", SMALL_JSON))
        elif path.startswith("/user/"):
            try:
                body = user_json(int(path[len("/user/") :]))
            except ValueError:
                self.transport.write(response("400 Bad Request", "text/plain", b"invalid user id"))
                return
            self.transport.write(response("200 OK", "application/json", body))
        elif path == "/large":
            size = min(int(params.get("size", [DEFAULT_LARGE_SIZE])[0]), MAX_LARGE_SIZE)
            self.transport.write(response("200 OK", "application/octet-stream", b"x" * size))
        elif path == "/sse":
            count = int(params.get("count", ["1000"])[0])
            self.streaming = asyncio.ensure_future(self.stream_events(count))
        elif path == "/health":
            self.transport.write(response("200 OK", "text/plain", b"ok"))
        else:
            self.transport.write(response("404 Not Found", "text/plain", b"not found"))

    async def stream_events(self, count: int) -> None:
        assert self.transport is not None
        self.transport.write(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n"
            b"cache-control: no-cache\r\ntransfer-encoding: chunked\r\n\r\n"
        )
        for start in range(0, count, SSE_EVENTS_PER_CHUNK):
            chunk = sse_chunk(start, min(start + SSE_EVENTS_PER_CHUNK, count))
            self.transport.write(f"{len(chunk):x}\r\n".encode("ascii") + chunk + b"\r\n")
            if self.transport.get_write_buffer_size() > 1 << 20:
                await asyncio.sleep(0)  # let the socket drain
        self.transport.write(b"0\r\n\r\n")
        self.streaming = None
        if self.buffer:
            self.data_received(b"")


async def serve(port: int) -> None:
    loop = asyncio.get_running_loop()
    server = await loop.create_server(BenchProtocol, "127.0.0.1", port, backlog=1024)
    bound = server.sockets[0].getsockname()[1]
    print(f"LISTENING {bound}", flush=True)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    try:
        asyncio.run(serve(args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
