"""Local test servers: HTTP/SSE/WebSocket (aiohttp), TLS + HTTP/2 (hypercorn),
a stalled TCP listener for timeout tests, and a tiny forward proxy.

Everything binds to 127.0.0.1 on an ephemeral port, so the suite needs no
network access and has no cross-test port collisions.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import gzip
import hashlib
import json
import os
import re
import shutil
import socket
import ssl
import sys
import tempfile
import threading
import zlib
from collections import Counter
from collections.abc import Iterator
from typing import Any

import pytest
from aiohttp import WSMsgType, web

import nextreq

# -- helpers ------------------------------------------------------------------


def zstd_compress(data: bytes) -> bytes | None:
    try:
        from compression import zstd  # Python 3.14+

        return zstd.compress(data)
    except ImportError:
        pass
    try:
        import zstandard

        return zstandard.ZstdCompressor().compress(data)
    except ImportError:
        return None


def brotli_compress(data: bytes) -> bytes | None:
    try:
        import brotli
    except ImportError:
        return None
    return bytes(brotli.compress(data))


SSE_FIXTURES: dict[str, bytes] = {
    "mixed": (
        b": this is a comment\n"
        b"data: first\n\n"
        b"event: update\nid: 42\nretry: 1500\ndata: line one\ndata: line two\n\n"
        b'data: {"n": 3}\r\n\r\n'
        b"event: ignored-because-no-data\n\n"
        b"data: caf\xc3\xa9 \xf0\x9f\x98\x80\n\n"
        b"data: unterminated\n"
    ),
}

COMPRESSIBLE = json.dumps({"message": "hello " * 200, "items": list(range(200))}).encode()


class LiveServer:
    """An aiohttp application running on a background thread."""

    def __init__(self) -> None:
        self.flaky: Counter[str] = Counter()
        #: Digest auth: highest nonce count seen per issued nonce.
        self.digest_nonces: dict[str, int] = {}
        self.digest_requests = 0
        self.peers: set[Any] = set()
        self.inflight = 0
        self.max_inflight = 0
        self.request_count = 0
        #: Downloads: the version of the file being served, and the headers
        #: of every request for it.
        self.file_version = "1"
        self.file_requests: list[dict[str, str]] = []
        #: Path of the Unix domain socket the server also listens on.
        self.uds: str | None = None
        self._uds_dir: str | None = None
        if sys.platform != "win32":
            # A short path: macOS limits socket paths to 104 bytes.
            self._uds_dir = tempfile.mkdtemp(prefix="nextreq-")
            self.uds = os.path.join(self._uds_dir, "http.sock")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self.port: int = self._sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.ws_url = f"ws://127.0.0.1:{self.port}"

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="live-server", daemon=True)
        self._thread.start()
        assert self._ready.wait(10), "test server failed to start"

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        runner = web.AppRunner(self._build_app(), access_log=None)
        loop.run_until_complete(runner.setup())
        loop.run_until_complete(web.SockSite(runner, self._sock).start())
        if self.uds is not None:
            loop.run_until_complete(web.UnixSite(runner, self.uds).start())
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(runner.cleanup())
            loop.close()

    def stop(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(5)
        if self._uds_dir is not None:
            shutil.rmtree(self._uds_dir, ignore_errors=True)

    def reset(self) -> None:
        self.flaky.clear()
        self.digest_nonces.clear()
        self.digest_requests = 0
        self.peers.clear()
        self.inflight = 0
        self.max_inflight = 0
        self.request_count = 0
        self.file_version = "1"
        self.file_requests.clear()

    # -- application ------------------------------------------------------

    def _build_app(self) -> web.Application:
        @web.middleware
        async def track(request: web.Request, handler: Any) -> web.StreamResponse:
            self.request_count += 1
            if request.transport is not None:
                self.peers.add(request.transport.get_extra_info("peername"))
            return await handler(request)

        app = web.Application(middlewares=[track], client_max_size=256 * 1024 * 1024)
        r = app.router
        r.add_get("/json", self.json_)
        r.add_route("*", "/echo", self.echo)
        r.add_route("*", "/status/{code}", self.status)
        r.add_route("*", "/redirect/{n}", self.redirect)
        r.add_route("*", "/redirect-to", self.redirect_to)
        r.add_get("/cookies/set", self.cookies_set)
        r.add_get("/cookies", self.cookies)
        r.add_route("*", "/delay/{seconds}", self.delay)
        r.add_get("/slow", self.slow)
        r.add_get("/encoded/{encoding}", self.encoded)
        r.add_get("/corrupt-gzip", self.corrupt_gzip)
        r.add_get("/stream/{n}", self.stream)
        r.add_get("/lines", self.lines)
        r.add_get("/slow-body", self.slow_body)
        r.add_get("/sse", self.sse)
        r.add_get("/sse-fixture/{name}", self.sse_fixture)
        r.add_route("*", "/flaky/{key}", self.flaky_)
        r.add_get("/drop", self.drop)
        r.add_route("*", "/drop-then-ok/{key}", self.drop_then_ok)
        r.add_post("/upload", self.upload)
        r.add_route("*", "/digest", self.digest)
        r.add_get("/basic-auth/{user}/{password}", self.basic_auth)
        r.add_route("*", "/digest-auth/{user}/{password}", self.digest_auth)
        r.add_get("/users/{user_id}", self.user)
        r.add_get("/users", self.users)
        r.add_get("/large", self.large)
        r.add_get("/latin1-header", self.latin1_header)
        r.add_get("/ws", self.ws)
        r.add_get("/ws-headers", self.ws_headers)
        r.add_get("/ws-heartbeat", self.ws_heartbeat)
        r.add_get("/ws-close", self.ws_close)
        r.add_get("/ws-digest/{user}/{password}", self.ws_digest)
        r.add_get("/file", self.file)
        r.add_get("/pages", self.pages)
        r.add_get("/cursor", self.cursor)
        r.add_get("/ndjson", self.ndjson)
        return app

    async def json_(self, request: web.Request) -> web.Response:
        return web.json_response({"hello": "world"})

    # -- downloads --------------------------------------------------------

    @staticmethod
    def file_bytes(size: int, version: str = "1") -> bytes:
        """The content of the downloadable file: ``size`` bytes that differ per version."""
        block = b"".join(
            hashlib.sha256(f"{version}:{n}".encode()).digest() for n in range(size // 32 + 1)
        )
        return block[:size]

    async def file(self, request: web.Request) -> web.StreamResponse:
        """A file with ``Range`` / ``If-Range`` support.

        Query: ``size``; ``validator`` (``etag``, ``weak``, ``date`` or ``none``);
        ``no_ranges`` to ignore ``Range``; ``gzip`` to compress the transfer;
        ``drop_after=N`` with ``key`` to cut the first ``drops`` connections
        after N bytes of the body.
        """
        query = request.query
        size = int(query.get("size", "100000"))
        version = self.file_version
        body = self.file_bytes(size, version)
        self.file_requests.append({k.lower(): v for k, v in request.headers.items()})

        headers = {"Accept-Ranges": "bytes"}
        kind = query.get("validator", "etag")
        current: str | None = None
        if kind == "etag":
            current = headers["ETag"] = f'"v{version}"'
        elif kind == "weak":
            headers["ETag"] = f'W/"v{version}"'
        elif kind == "date":
            current = headers["Last-Modified"] = f"Wed, 0{version} Jan 2025 00:00:00 GMT"

        status, start = 200, 0
        wanted = request.headers.get("Range")
        honoured = "no_ranges" not in query and request.headers.get("If-Range", current) == current
        if wanted and honoured:
            start = int(wanted.removeprefix("bytes=").rstrip("-"))
            if start >= size:
                return web.Response(status=416, headers={"Content-Range": f"bytes */{size}"})
            status = 206
            headers["Content-Range"] = f"bytes {start}-{size - 1}/{size}"
        payload = body[start:]
        if "gzip" in query:
            payload = gzip.compress(payload)
            headers["Content-Encoding"] = "gzip"

        response = web.StreamResponse(status=status, headers=headers)
        response.content_length = len(payload)
        await response.prepare(request)
        counter = f"file:{query.get('key', '')}"
        if "drop_after" in query and self.flaky[counter] < int(query.get("drops", "1")):
            self.flaky[counter] += 1
            await response.write(payload[: int(query["drop_after"])])
            await asyncio.sleep(0.1)  # let the client read what was sent
            if request.transport is not None:
                request.transport.abort()
            return response
        for offset in range(0, len(payload), 16384):
            await response.write(payload[offset : offset + 16384])
        await response.write_eof()
        return response

    # -- pagination and JSON Lines ----------------------------------------

    async def pages(self, request: web.Request) -> web.Response:
        """``Link``-header pagination: ``page`` of ``total`` (default 3)."""
        query = request.query
        page = int(query.get("page", "1"))
        total = int(query.get("total", "3"))
        headers = {}
        if page < total or "loop" in query:
            following = page if "loop" in query else page + 1
            target = request.rel_url.update_query({"page": str(following)})
            link = str(target) if "relative" in query else f"{self.url}{target}"
            headers["Link"] = f'<{self.url}/pages?page=1>; rel="first", <{link}>; rel="next"'
        payload = {
            "page": page,
            "items": [f"item-{page}-{n}" for n in range(2)],
            "query_string": request.query_string,
            "authorization": request.headers.get("Authorization"),
            "x_test": request.headers.get("X-Test"),
        }
        return web.json_response(payload, headers=headers)

    async def cursor(self, request: web.Request) -> web.Response:
        """Cursor pagination in the body: "" -> "a" -> "b" -> end."""
        cursor = request.query.get("cursor", "")
        following = {"": "a", "a": "b"}.get(cursor)
        payload = {
            "cursor": cursor,
            "next_cursor": following,
            "query_string": request.query_string,
        }
        return web.json_response(payload)

    async def ndjson(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "application/x-ndjson"})
        await response.prepare(request)
        # Lines split across chunks, a blank line, CRLF, a line separator
        # inside a string (legal in JSON) and no newline at the end.
        parts = (
            b'{"id": 1, "name": "Ada"}\n{"id": 2, ',
            b'"name": "Gr\xc3',
            b'\xa9te"}\r\n\n  \n',
            '{"id": 3, "name": "a\u2028b"}\n'.encode(),
            b'{"id": 4, "name": "last"}',
        )
        for part in parts:
            await response.write(part)
            await asyncio.sleep(0.005)
        await response.write_eof()
        return response

    async def echo(self, request: web.Request) -> web.Response:
        body = await request.read()
        query: dict[str, list[str]] = {}
        for key, value in request.query.items():
            query.setdefault(key, []).append(value)
        payload: dict[str, Any] = {
            "method": request.method,
            "path": request.path,
            "query": query,
            "query_string": request.query_string,
            "headers": {k.lower(): v for k, v in request.headers.items()},
            "body": body.decode("utf-8", "replace"),
            "body_b64": base64.b64encode(body).decode("ascii"),
            "body_length": len(body),
            "http_version": f"{request.version.major}.{request.version.minor}",
        }
        try:
            payload["json"] = json.loads(body) if body else None
        except ValueError:
            payload["json"] = None
        return web.json_response(payload)

    async def status(self, request: web.Request) -> web.Response:
        code = int(request.match_info["code"])
        headers = {}
        if "retry_after" in request.query:
            headers["Retry-After"] = request.query["retry_after"]
        return web.json_response({"status": code}, status=code, headers=headers)

    async def redirect(self, request: web.Request) -> web.Response:
        n = int(request.match_info["n"])
        status = int(request.query.get("status", "302"))
        location = f"/redirect/{n - 1}?status={status}" if n > 1 else "/echo"
        return web.Response(status=status, headers={"Location": location})

    async def redirect_to(self, request: web.Request) -> web.Response:
        status = int(request.query.get("status", "302"))
        return web.Response(status=status, headers={"Location": request.query["url"]})

    async def cookies_set(self, request: web.Request) -> web.Response:
        response = web.Response(status=302, headers={"Location": "/cookies"})
        for name, value in request.query.items():
            response.headers.add("Set-Cookie", f"{name}={value}; Path=/")
        return response

    async def cookies(self, request: web.Request) -> web.Response:
        return web.json_response({"cookies": dict(request.cookies)})

    async def delay(self, request: web.Request) -> web.Response:
        seconds = float(request.match_info["seconds"])
        await asyncio.sleep(seconds)
        return web.json_response({"delayed": seconds})

    async def slow(self, request: web.Request) -> web.Response:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            await asyncio.sleep(float(request.query.get("delay", "0.05")))
        finally:
            self.inflight -= 1
        return web.json_response({"n": int(request.query.get("n", "0"))})

    async def encoded(self, request: web.Request) -> web.Response:
        encoding = request.match_info["encoding"]
        if encoding == "gzip":
            body: bytes | None = gzip.compress(COMPRESSIBLE)
        elif encoding == "deflate":
            body = zlib.compress(COMPRESSIBLE)
        elif encoding == "br":
            body = brotli_compress(COMPRESSIBLE)
        elif encoding == "zstd":
            body = zstd_compress(COMPRESSIBLE)
        else:
            body = None
        if body is None:
            return web.Response(status=501)
        return web.Response(
            body=body,
            headers={"Content-Encoding": encoding, "Content-Type": "application/json"},
        )

    async def corrupt_gzip(self, request: web.Request) -> web.Response:
        return web.Response(
            body=b"this is definitely not gzip data" * 4,
            headers={"Content-Encoding": "gzip", "Content-Type": "application/json"},
        )

    async def stream(self, request: web.Request) -> web.StreamResponse:
        n = int(request.match_info["n"])
        response = web.StreamResponse(headers={"Content-Type": "application/octet-stream"})
        await response.prepare(request)
        for i in range(n):
            await response.write(f"chunk-{i};".encode())
            await asyncio.sleep(0.005)
        await response.write_eof()
        return response

    async def lines(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/plain; charset=utf-8"})
        await response.prepare(request)
        # Deliberately split a CRLF and a multi-byte character across chunks.
        for part in (b"alpha\r", b"\nbeta\ngam", b"ma\rdelt", b"a \xc3", b"\xa9\n", b"last"):
            await response.write(part)
            await asyncio.sleep(0.005)
        await response.write_eof()
        return response

    async def slow_body(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/plain"})
        await response.prepare(request)
        await response.write(b"first chunk")
        with contextlib.suppress(asyncio.CancelledError, ConnectionError):
            await asyncio.sleep(float(request.query.get("stall", "5")))
            await response.write_eof()
        return response

    async def sse(self, request: web.Request) -> web.StreamResponse:
        count = int(request.query.get("count", "5"))
        pause = float(request.query.get("delay", "0"))
        response = web.StreamResponse(
            headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
        )
        await response.prepare(request)
        for i in range(count):
            await response.write(f'id: {i}\nevent: tick\ndata: {{"n": {i}}}\n\n'.encode())
            if pause:
                await asyncio.sleep(pause)
        await response.write_eof()
        return response

    async def sse_fixture(self, request: web.Request) -> web.StreamResponse:
        payload = SSE_FIXTURES[request.match_info["name"]]
        size = int(request.query.get("chunk", "0")) or len(payload)
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        for start in range(0, len(payload), size):
            await response.write(payload[start : start + size])
            await asyncio.sleep(0)
        await response.write_eof()
        return response

    async def flaky_(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        fail = int(request.query.get("fail", "2"))
        status = int(request.query.get("status", "503"))
        self.flaky[key] += 1
        attempt = self.flaky[key]
        if attempt <= fail:
            headers = {}
            if "retry_after" in request.query:
                headers["Retry-After"] = request.query["retry_after"]
            return web.json_response({"attempt": attempt}, status=status, headers=headers)
        return web.json_response({"attempt": attempt, "ok": True})

    async def drop(self, request: web.Request) -> web.Response:
        if request.transport is not None:
            request.transport.abort()
        return web.Response()

    async def drop_then_ok(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        self.flaky[key] += 1
        if self.flaky[key] == 1 and request.transport is not None:
            request.transport.abort()
            return web.Response()
        return web.json_response({"attempt": self.flaky[key], "ok": True})

    async def upload(self, request: web.Request) -> web.Response:
        reader = await request.multipart()
        fields: dict[str, Any] = {}
        files: dict[str, Any] = {}
        async for part in reader:
            data = await part.read(decode=False)  # type: ignore[union-attr]
            name = part.name or ""  # type: ignore[union-attr]
            filename = part.filename  # type: ignore[union-attr]
            if filename:
                files[name] = {
                    "filename": filename,
                    "content_type": part.headers.get("Content-Type"),  # type: ignore[union-attr]
                    "size": len(data),
                    "content": data.decode("latin-1"),
                }
            else:
                fields[name] = data.decode("utf-8")
        return web.json_response({"fields": fields, "files": files})

    async def digest(self, request: web.Request) -> web.Response:
        """Consume the body in chunks (never holding it whole) and describe it.

        ``?key=K&fail=N`` answers the first N requests for K with a 503, and
        ``?redirect=STATUS`` answers with that redirect back to this endpoint.
        """
        size = 0
        sha256 = hashlib.sha256()
        async for chunk in request.content.iter_chunked(64 * 1024):
            size += len(chunk)
            sha256.update(chunk)
        payload = {
            "method": request.method,
            "size": size,
            "sha256": sha256.hexdigest(),
            "content_length": request.headers.get("Content-Length"),
            "transfer_encoding": request.headers.get("Transfer-Encoding"),
            "content_type": request.headers.get("Content-Type"),
        }
        if "redirect" in request.query:
            return web.Response(
                status=int(request.query["redirect"]), headers={"Location": "/digest"}
            )
        if "key" in request.query:
            self.flaky[request.query["key"]] += 1
            payload["attempt"] = self.flaky[request.query["key"]]
            if payload["attempt"] <= int(request.query.get("fail", "1")):
                return web.json_response(payload, status=503)
        return web.json_response(payload)

    async def digest_auth(self, request: web.Request) -> web.Response:
        """A Digest-protected resource (RFC 7616), verified independently of the client.

        Query: ``algorithm`` (default MD5), ``qop`` (default ``auth``; empty for
        RFC 2069; ``auth-int`` also signs the body), ``expire=1`` (every nonce is
        accepted only once, then stale),
        ``extra=1`` (also offer Basic in the same header).
        """
        self.digest_requests += 1
        body = await request.read()
        user, password = request.match_info["user"], request.match_info["password"]
        algorithm = request.query.get("algorithm", "MD5")
        qop = request.query.get("qop", "auth")
        realm = "nextreq tests"

        def challenge(stale: bool = False) -> web.Response:
            nonce = os.urandom(12).hex()
            self.digest_nonces[nonce] = 0
            value = f'Digest realm="{realm}", nonce="{nonce}", opaque="0pa\\"que", algorithm={algorithm}'
            if qop:
                value += f', qop="{qop}"'
            if stale:
                value += ", stale=true"
            if "extra" in request.query:
                value = 'Basic realm="other", ' + value
            return web.json_response(
                {"authenticated": False}, status=401, headers={"WWW-Authenticate": value}
            )

        header = request.headers.get("Authorization", "")
        if not header.lower().startswith("digest "):
            return challenge()
        given = {
            key.lower(): value[1:-1].replace('\\"', '"') if value.startswith('"') else value
            for key, value in re.findall(r'([\w-]+)=("(?:[^"\\]|\\.)*"|[^\s,]+)', header[7:])
        }
        nonce = given.get("nonce", "")
        if nonce not in self.digest_nonces:
            return challenge(stale=True)
        base = algorithm.upper().removesuffix("-SESS")
        hash_name = {"MD5": "md5", "SHA": "sha1", "SHA-256": "sha256", "SHA-512": "sha512"}[base]

        def digest(text: str) -> str:
            return hashlib.new(hash_name, text.encode()).hexdigest()

        ha1 = digest(f"{user}:{realm}:{password}")
        if algorithm.upper().endswith("-SESS"):
            ha1 = digest(f"{ha1}:{nonce}:{given.get('cnonce', '')}")
        ha2 = digest(f"{request.method}:{given.get('uri', '')}")
        if qop and given.get("qop") not in [option.strip() for option in qop.split(",")]:
            return challenge()  # a quality of protection that was not offered
        if given.get("qop") == "auth-int":
            body_hash = hashlib.new(hash_name, body).hexdigest()
            ha2 = digest(f"{request.method}:{given.get('uri', '')}:{body_hash}")
        if qop:
            count = int(given.get("nc", "0"), 16)
            if count <= self.digest_nonces[nonce]:
                return challenge()  # a replayed nonce count
            expected = digest(f"{ha1}:{nonce}:{given['nc']}:{given['cnonce']}:{given['qop']}:{ha2}")
        else:
            count = self.digest_nonces[nonce] + 1
            expected = digest(f"{ha1}:{nonce}:{ha2}")
        valid = (
            given.get("response") == expected
            and given.get("uri") == request.raw_path
            and given.get("username") == user
            and given.get("opaque") == '0pa"que'
        )
        if not valid:
            return challenge()
        self.digest_nonces[nonce] = count
        if "expire" in request.query:
            del self.digest_nonces[nonce]
        return web.json_response(
            {
                "authenticated": True,
                "nc": given.get("nc"),
                "qop": given.get("qop"),
                "body": body.decode("latin-1"),
                "user": user,
            }
        )

    async def basic_auth(self, request: web.Request) -> web.Response:
        expected = base64.b64encode(
            f"{request.match_info['user']}:{request.match_info['password']}".encode()
        ).decode()
        if request.headers.get("Authorization") == f"Basic {expected}":
            return web.json_response({"authenticated": True})
        return web.json_response(
            {"authenticated": False}, status=401, headers={"WWW-Authenticate": 'Basic realm="t"'}
        )

    @staticmethod
    def _user(user_id: int) -> dict[str, Any]:
        return {
            "id": user_id,
            "name": f"User {user_id}",
            "email": f"user{user_id}@example.com",
            "active": user_id % 2 == 0,
            "tags": ["a", "b"],
            "address": {"city": "Paris", "zip": "75001"},
            "created_at": "2026-01-02T03:04:05+00:00",
        }

    async def user(self, request: web.Request) -> web.Response:
        raw = request.match_info["user_id"]
        if raw == "missing":
            return web.json_response({"detail": "not found"}, status=404)
        if raw == "invalid":
            return web.json_response({"id": "not-a-number", "name": 5})
        if raw == "notjson":
            return web.Response(text="<html>oops</html>", content_type="text/html")
        return web.json_response(self._user(int(raw)))

    async def users(self, request: web.Request) -> web.Response:
        count = int(request.query.get("count", "3"))
        return web.json_response([self._user(i) for i in range(1, count + 1)])

    async def large(self, request: web.Request) -> web.Response:
        size = int(request.query.get("size", "1048576"))
        return web.Response(body=b"x" * size, content_type="application/octet-stream")

    async def latin1_header(self, request: web.Request) -> web.Response:
        return web.Response(text="ok", headers={"X-Plain": "value", "X-Count": "3"})

    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        socket_ = web.WebSocketResponse(protocols=("chat", "json"))
        await socket_.prepare(request)
        async for message in socket_:
            if message.type == WSMsgType.TEXT:
                if message.data == "close-me":
                    await socket_.close(code=4001, message=b"server says bye")
                    break
                if message.data == "compressed?":
                    # Non-zero when permessage-deflate was negotiated.
                    await socket_.send_str(f"compressed:{bool(socket_.compress)}")
                    continue
                await socket_.send_str(f"echo:{message.data}")
            elif message.type == WSMsgType.BINARY:
                await socket_.send_bytes(message.data)
        return socket_

    async def ws_heartbeat(self, request: web.Request) -> web.WebSocketResponse:
        """Pings every 0.2 s and drops the connection if a pong is missed."""
        socket_ = web.WebSocketResponse(heartbeat=0.2)
        await socket_.prepare(request)
        async for message in socket_:
            if message.type == WSMsgType.TEXT:
                await socket_.send_str(f"echo:{message.data}")
        return socket_

    async def ws_headers(self, request: web.Request) -> web.WebSocketResponse:
        socket_ = web.WebSocketResponse()
        await socket_.prepare(request)
        await socket_.send_json({k.lower(): v for k, v in request.headers.items()})
        await socket_.close()
        return socket_

    async def ws_digest(self, request: web.Request) -> web.StreamResponse:
        """A WebSocket endpoint behind Digest auth: refused with 401 until answered."""
        verdict = await self.digest_auth(request)
        if verdict.status != 200:
            return verdict
        socket_ = web.WebSocketResponse()
        await socket_.prepare(request)
        await socket_.send_str("authenticated")
        async for message in socket_:
            if message.type == WSMsgType.TEXT:
                await socket_.send_str(f"echo:{message.data}")
        return socket_

    async def ws_close(self, request: web.Request) -> web.WebSocketResponse:
        socket_ = web.WebSocketResponse()
        await socket_.prepare(request)
        await socket_.send_str("one")
        await socket_.send_str("two")
        await socket_.close(code=1001, message=b"going away")
        return socket_


@pytest.fixture(scope="session")
def _live_server() -> Iterator[LiveServer]:
    server = LiveServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture
def server(_live_server: LiveServer) -> LiveServer:
    """The shared HTTP/SSE/WebSocket test server, with counters reset."""
    _live_server.reset()
    return _live_server


@pytest.fixture
def client(server: LiveServer) -> Iterator[nextreq.Client]:
    with nextreq.Client(base_url=server.url, trust_env=False) as c:
        yield c


@pytest.fixture
async def aclient(server: LiveServer) -> Any:
    async with nextreq.AsyncClient(base_url=server.url, trust_env=False) as c:
        yield c


# -- stalled TCP listener -----------------------------------------------------


class StalledServer:
    """Accepts TCP connections, then never reads or writes anything."""

    def __init__(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # A tiny receive buffer makes large uploads stall quickly.
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(64)
        self.port: int = self._sock.getsockname()[1]
        self.http_url = f"http://127.0.0.1:{self.port}"
        self.https_url = f"https://127.0.0.1:{self.port}"
        self._connections: list[socket.socket] = []
        self._closing = False
        self._thread = threading.Thread(target=self._accept, name="stalled-server", daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while not self._closing:
            try:
                connection, _ = self._sock.accept()
            except OSError:
                return
            self._connections.append(connection)

    def close(self) -> None:
        self._closing = True
        with contextlib.suppress(OSError):
            self._sock.close()
        for connection in self._connections:
            with contextlib.suppress(OSError):
                connection.close()


@pytest.fixture
def stalled() -> Iterator[StalledServer]:
    server = StalledServer()
    yield server
    server.close()


# -- TLS + HTTP/2 -------------------------------------------------------------


class TLSServer:
    """A hypercorn server speaking HTTP/2 and HTTP/1.1 over TLS."""

    def __init__(self, tmp_path: Any) -> None:
        import trustme

        self.ca = trustme.CA()
        certificate = self.ca.issue_cert("localhost", "127.0.0.1")
        self.ca_path = str(tmp_path / "ca.pem")
        self.cert_path = str(tmp_path / "server.pem")
        self.key_path = str(tmp_path / "server.key")
        self.ca.cert_pem.write_to_path(self.ca_path)
        certificate.private_key_pem.write_to_path(self.key_path)
        with open(self.cert_path, "wb") as handle:
            for blob in certificate.cert_chain_pems:
                handle.write(blob.bytes())

        # A client certificate signed by the same CA, for mTLS-style config tests.
        client_cert = self.ca.issue_cert("client@example.com")
        self.client_cert_path = str(tmp_path / "client.pem")
        client_cert.private_key_and_cert_chain_pem.write_to_path(self.client_cert_path)

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port: int = probe.getsockname()[1]
        self.url = f"https://localhost:{self.port}"
        self._loop: asyncio.AbstractEventLoop | None = None
        self._shutdown: asyncio.Event | None = None
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="tls-server", daemon=True)

    @staticmethod
    async def _app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "websocket" and scope["path"] == "/ws-echo":
            await receive()  # websocket.connect
            await send({"type": "websocket.accept"})
            while (message := await receive())["type"] == "websocket.receive":
                if message.get("text") is not None:
                    await send({"type": "websocket.send", "text": "tls:" + message["text"]})
                else:
                    await send({"type": "websocket.send", "bytes": message["bytes"]})
            return
        if scope["type"] != "http":
            return
        body = json.dumps(
            {
                "http_version": scope["http_version"],
                "path": scope["path"],
                "scheme": scope["scheme"],
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})

    def _run(self) -> None:
        from hypercorn.asyncio import serve
        from hypercorn.config import Config

        config = Config()
        config.bind = [f"127.0.0.1:{self.port}"]
        config.certfile = self.cert_path
        config.keyfile = self.key_path
        config.alpn_protocols = ["h2", "http/1.1"]
        config.accesslog = None
        config.errorlog = None
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        self._shutdown = asyncio.Event()

        async def main() -> None:
            assert self._shutdown is not None
            self._ready.set()
            await serve(self._app, config, shutdown_trigger=self._shutdown.wait)  # type: ignore[arg-type]

        try:
            loop.run_until_complete(main())
        finally:
            loop.close()

    def start(self) -> None:
        self._thread.start()
        assert self._ready.wait(10)
        # Wait until the port actually accepts connections.
        for _ in range(200):
            with (
                contextlib.suppress(OSError),
                socket.create_connection(("127.0.0.1", self.port), 0.2),
            ):
                return
            threading.Event().wait(0.02)
        raise RuntimeError("TLS test server did not start")

    def stop(self) -> None:
        if self._loop is not None and self._shutdown is not None:
            self._loop.call_soon_threadsafe(self._shutdown.set)
        self._thread.join(5)


@pytest.fixture(scope="session")
def tls_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TLSServer]:
    pytest.importorskip("trustme")
    pytest.importorskip("hypercorn")
    server = TLSServer(tmp_path_factory.mktemp("tls"))
    server.start()
    yield server
    server.stop()


# -- forward proxy ------------------------------------------------------------


class ProxyServer:
    """A minimal HTTP forward proxy that records the requests it relays.

    Plain ``http://`` targets are forwarded; ``CONNECT`` opens a raw tunnel.
    With ``tls`` the proxy itself is reached over TLS (an ``https://`` proxy).
    """

    def __init__(self, tls: ssl.SSLContext | None = None) -> None:
        self._tls = tls
        self.requests: list[str] = []
        #: Headers of the most recent request, lower-cased names.
        self.last_headers: dict[str, str] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self.port: int = self._sock.getsockname()[1]
        self.url = (
            f"http://127.0.0.1:{self.port}" if tls is None else f"https://localhost:{self.port}"
        )
        self._thread = threading.Thread(target=self._run, name="proxy-server", daemon=True)
        self._thread.start()
        assert self._ready.wait(10)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            request_line, _, rest = head.partition(b"\r\n")
            method, target, _version = request_line.decode("latin-1").split(" ", 2)
            self.requests.append(f"{method} {target}")
            self.last_headers = {
                name.strip().lower(): value.strip()
                for name, _, value in (
                    line.decode("latin-1").partition(":") for line in rest.split(b"\r\n") if line
                )
            }
            if method == "CONNECT":
                await self._tunnel(target, reader, writer)
                return
            if not target.startswith("http://"):
                writer.write(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n")
                await writer.drain()
                return
            without_scheme = target[len("http://") :]
            authority, _, path = without_scheme.partition("/")
            host, _, port = authority.partition(":")
            headers = [
                line
                for line in rest.split(b"\r\n")
                if line and not line.lower().startswith((b"proxy-", b"connection:"))
            ]
            content_length = 0
            for line in headers:
                if line.lower().startswith(b"content-length:"):
                    content_length = int(line.split(b":", 1)[1])
            body = await reader.readexactly(content_length) if content_length else b""
            upstream_reader, upstream_writer = await asyncio.open_connection(host, int(port or 80))
            upstream_writer.write(
                f"{method} /{path} HTTP/1.1\r\n".encode("latin-1")
                + b"\r\n".join(headers)
                + b"\r\nconnection: close\r\nvia: 1.1 test-proxy\r\n\r\n"
                + body
            )
            await upstream_writer.drain()
            while chunk := await upstream_reader.read(65536):
                writer.write(chunk)
                await writer.drain()
            upstream_writer.close()
        except (asyncio.IncompleteReadError, ConnectionError, ValueError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def _tunnel(
        self, target: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        host, _, port = target.rpartition(":")
        upstream_reader, upstream_writer = await asyncio.open_connection(host, int(port))
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await writer.drain()

        async def pipe(source: asyncio.StreamReader, sink: asyncio.StreamWriter) -> None:
            try:
                while chunk := await source.read(65536):
                    sink.write(chunk)
                    await sink.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                with contextlib.suppress(Exception):
                    sink.close()

        await asyncio.gather(pipe(reader, upstream_writer), pipe(upstream_reader, writer))

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        server = loop.run_until_complete(
            asyncio.start_server(self._handle, sock=self._sock, ssl=self._tls)
        )
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            server.close()
            # Let the accept loop and any in-flight handlers finish cancelling
            # before the loop is closed (the Windows proactor keeps a pending
            # accept task around otherwise).
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    def close(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(5)


@pytest.fixture
def proxy() -> Iterator[ProxyServer]:
    server = ProxyServer()
    yield server
    server.close()


@pytest.fixture
def tls_proxy(tls_server: TLSServer) -> Iterator[ProxyServer]:
    """An ``https://`` proxy whose certificate is signed by ``tls_server.ca_path``."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tls_server.cert_path, tls_server.key_path)
    server = ProxyServer(tls=context)
    yield server
    server.close()


@pytest.fixture
def unused_port() -> int:
    """A port on which nothing is listening (connections are refused)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port
