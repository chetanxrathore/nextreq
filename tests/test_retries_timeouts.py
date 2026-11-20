"""NXRQ-202 / NXRQ-401: timeouts, proxies, retries and ``Retry-After``."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any

import pytest

import nextreq
from nextreq import Response, RetryPolicy, Timeout

from .conftest import LiveServer, ProxyServer, StalledServer

# -- Timeout configuration ----------------------------------------------------


def test_timeout_defaults_are_safe() -> None:
    timeout = Timeout()
    assert (timeout.connect, timeout.read, timeout.write, timeout.pool) == (5.0, 30.0, 30.0, 10.0)
    assert timeout.as_tuple() == (5.0, 30.0, 30.0, 10.0)
    assert repr(timeout) == "Timeout(connect=5.0, read=30.0, write=30.0, pool=10.0)"
    with nextreq.Client() as client:
        assert client.timeout == Timeout()
    async_client = nextreq.AsyncClient()
    assert async_client.timeout == Timeout()


def test_timeout_construction_forms() -> None:
    assert Timeout(2).as_tuple() == (2.0, 2.0, 2.0, 2.0)
    assert Timeout(2.5, connect=1).as_tuple() == (1.0, 2.5, 2.5, 2.5)
    assert Timeout(None).as_tuple() == (None, None, None, None)
    assert Timeout(None, read=9).as_tuple() == (None, 9.0, None, None)
    assert Timeout(read=60).as_tuple() == (5.0, 60.0, 30.0, 10.0)
    assert Timeout(connect=None, pool=1).as_tuple() == (None, 30.0, 30.0, 1.0)
    assert Timeout(timeout=4).as_tuple() == (4.0, 4.0, 4.0, 4.0)
    assert Timeout(Timeout(3, write=7)) == Timeout(3, write=7)
    assert Timeout(3) == Timeout(3.0) and Timeout(3) != Timeout(4) and Timeout(3) != 3
    assert hash(Timeout(3)) == hash(Timeout(3.0))


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan")])
def test_timeout_rejects_non_positive_values(bad: float) -> None:
    with pytest.raises(ValueError):
        Timeout(bad)
    with pytest.raises(ValueError):
        Timeout(read=bad)


def test_timeout_rejects_bad_arguments() -> None:
    with pytest.raises(TypeError):
        Timeout("fast")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Timeout(1, 2)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        Timeout(total=5)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        Timeout(1, timeout=2)  # type: ignore[misc]


def test_client_timeout_forms(server: LiveServer) -> None:
    with nextreq.Client(timeout=7) as client:
        assert client.timeout == Timeout(7)
        client.timeout = Timeout(read=1)
        assert client.timeout is not None and client.timeout.read == 1.0
    with nextreq.Client(base_url=server.url, timeout=None) as client:
        assert client.timeout is None
        assert client.get("/json").status_code == 200
        request = client.build_request("GET", "/json", timeout=2)
        assert request.timeout == Timeout(2)
        assert client.build_request("GET", "/json").timeout is None


# -- typed timeout exceptions -------------------------------------------------


def test_timeout_exception_hierarchy() -> None:
    for exc in (
        nextreq.ConnectTimeout,
        nextreq.ReadTimeout,
        nextreq.WriteTimeout,
        nextreq.PoolTimeout,
    ):
        assert issubclass(exc, nextreq.TimeoutException)
        assert issubclass(exc, nextreq.TransportError)
        assert issubclass(exc, nextreq.NextReqError)
        assert issubclass(exc, TimeoutError), "catchable as the builtin TimeoutError"


def test_read_timeout_waiting_for_headers(client: nextreq.Client) -> None:
    started = time.perf_counter()
    with pytest.raises(nextreq.ReadTimeout) as info:
        client.get("/delay/5", timeout=Timeout(read=0.2))
    elapsed = time.perf_counter() - started
    assert 0.15 <= elapsed < 2.0
    assert info.value.request is not None and info.value.request.url.path == "/delay/5"
    assert "0.200s" in str(info.value)
    # The pool is not poisoned by the timed-out request.
    assert client.get("/json").status_code == 200


def test_read_timeout_per_request_float_and_client_default(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, timeout=0.2) as client:
        with pytest.raises(nextreq.ReadTimeout):
            client.get("/delay/5")
        # A per-request override wins over the client default.
        assert client.get("/delay/0.4", timeout=5).status_code == 200
    with nextreq.Client(base_url=server.url) as client, pytest.raises(TimeoutError):
        client.get("/delay/5", timeout=0.2)


def test_read_timeout_between_body_chunks(client: nextreq.Client) -> None:
    """The read timeout applies to each read, not to the whole response."""
    started = time.perf_counter()
    with pytest.raises(nextreq.ReadTimeout, match="reading the response body"):
        client.get("/slow-body", timeout=Timeout(read=0.2))
    assert time.perf_counter() - started < 2.0

    with client.stream("GET", "/slow-body", timeout=Timeout(read=0.2)) as response:
        chunks = response.iter_bytes()
        assert next(chunks) == b"first chunk"
        with pytest.raises(nextreq.ReadTimeout):
            next(chunks)


def test_read_timeout_is_per_read_not_total(client: nextreq.Client) -> None:
    # 20 chunks, 5 ms apart: ~100 ms in total, but every individual read is fast.
    response = client.get("/stream/20", timeout=Timeout(read=0.08))
    assert response.content.count(b"chunk-") == 20


def test_connect_timeout_during_tls_handshake(stalled: StalledServer) -> None:
    """The connect phase covers DNS + TCP + TLS; a silent peer stalls the handshake."""
    started = time.perf_counter()
    with pytest.raises(nextreq.ConnectTimeout) as info:
        nextreq.get(stalled.https_url, timeout=Timeout(connect=0.2), verify=False)
    assert 0.15 <= time.perf_counter() - started < 2.0
    assert isinstance(info.value, nextreq.TimeoutException)
    assert info.value.request is not None


def test_connect_timeout_is_independent_of_read_timeout(stalled: StalledServer) -> None:
    started = time.perf_counter()
    with pytest.raises(nextreq.ConnectTimeout):
        nextreq.get(stalled.https_url, timeout=Timeout(connect=0.2, read=30), verify=False)
    assert time.perf_counter() - started < 2.0
    # Over plain HTTP the TCP connect succeeds, so the same peer is a *read* timeout.
    with pytest.raises(nextreq.ReadTimeout):
        nextreq.get(stalled.http_url, timeout=Timeout(connect=5, read=0.2))


def test_write_timeout_when_peer_stops_reading(stalled: StalledServer) -> None:
    payload = b"x" * (32 * 1024 * 1024)
    started = time.perf_counter()
    with pytest.raises(nextreq.WriteTimeout) as info:
        nextreq.post(stalled.http_url, content=payload, timeout=Timeout(write=0.3, read=30))
    assert 0.25 <= time.perf_counter() - started < 5.0
    assert "upload" in str(info.value)


def test_pool_timeout_when_all_connections_are_busy(server: LiveServer) -> None:
    with nextreq.Client(
        base_url=server.url, max_connections=1, timeout=Timeout(pool=0.2)
    ) as client:
        with client.stream("GET", "/stream/200") as held:
            started = time.perf_counter()
            with pytest.raises(nextreq.PoolTimeout) as info:
                client.get("/json")
            assert 0.15 <= time.perf_counter() - started < 2.0
            assert "max_connections" in str(info.value)
            assert not held.is_closed
        # Releasing the stream frees the slot.
        assert client.get("/json").status_code == 200


def test_timeout_none_disables_timeouts(client: nextreq.Client) -> None:
    assert client.get("/delay/0.3", timeout=None).status_code == 200


async def test_async_typed_timeouts(server: LiveServer, stalled: StalledServer) -> None:
    async with nextreq.AsyncClient(base_url=server.url, verify=False) as client:
        with pytest.raises(nextreq.ReadTimeout):
            await client.get("/delay/5", timeout=Timeout(read=0.2))
        with pytest.raises(nextreq.ReadTimeout):
            await client.get("/slow-body", timeout=0.2)
        with pytest.raises(nextreq.ConnectTimeout):
            await client.get(stalled.https_url, timeout=Timeout(connect=0.2))
        with pytest.raises(nextreq.WriteTimeout):
            await client.post(
                stalled.http_url, content=b"x" * (32 * 1024 * 1024), timeout=Timeout(write=0.3)
            )
        assert (await client.get("/json")).status_code == 200

    async with nextreq.AsyncClient(
        base_url=server.url, max_connections=2, timeout=Timeout(pool=0.2)
    ) as limited:
        results = await limited.gather(
            [f"/delay/1?n={n}" for n in range(3)], max_concurrency=3, return_exceptions=True
        )
    kinds = sorted(type(r).__name__ for r in results)
    assert kinds == ["PoolTimeout", "Response", "Response"]


# -- proxies ------------------------------------------------------------------


def test_explicit_proxy(server: LiveServer, proxy: ProxyServer) -> None:
    with nextreq.Client(proxy=proxy.url, trust_env=False) as client:
        response = client.get(f"{server.url}/echo", params={"via": "proxy"})
    assert response.status_code == 200
    assert response.json()["headers"]["via"] == "1.1 test-proxy"
    assert proxy.requests == [f"GET {server.url}/echo?via=proxy"]


def test_proxy_mapping_by_scheme(server: LiveServer, proxy: ProxyServer) -> None:
    with nextreq.Client(proxy={"http://": proxy.url}, trust_env=False) as client:
        assert client.post(f"{server.url}/echo", json={"a": 1}).json()["json"] == {"a": 1}
    assert proxy.requests == [f"POST {server.url}/echo"]
    proxy.requests.clear()
    with nextreq.Client(proxy={"https://": proxy.url}, trust_env=False) as client:
        assert client.get(f"{server.url}/json").status_code == 200
    assert proxy.requests == [], "an https-only proxy must not intercept http:// URLs"
    with pytest.raises(ValueError, match="proxy scope"):
        nextreq.Client(proxy={"ftp://": proxy.url})


def test_proxy_from_environment(
    server: LiveServer, proxy: ProxyServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in (
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.setenv("HTTP_PROXY", proxy.url)

    with nextreq.Client() as client:  # trust_env=True is the default
        assert client.get(f"{server.url}/json").status_code == 200
    assert proxy.requests == [f"GET {server.url}/json"]

    proxy.requests.clear()
    with nextreq.Client(trust_env=False) as client:
        assert client.get(f"{server.url}/json").status_code == 200
    assert proxy.requests == [], "trust_env=False must ignore HTTP_PROXY"


def test_no_proxy_bypasses_the_environment_proxy(
    server: LiveServer, proxy: ProxyServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTP_PROXY", proxy.url)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.delenv("no_proxy", raising=False)
    with nextreq.Client() as client:
        assert client.get(f"{server.url}/json").status_code == 200
        assert proxy.requests == []
        # "localhost" is not listed in NO_PROXY, so it is still proxied.
        assert client.get(f"http://localhost:{server.port}/json").status_code == 200
    assert proxy.requests == [f"GET http://localhost:{server.port}/json"]


def test_unreachable_proxy_raises_a_transport_error(server: LiveServer, unused_port: int) -> None:
    with nextreq.Client(proxy=f"http://127.0.0.1:{unused_port}", trust_env=False) as client:
        with pytest.raises(nextreq.TransportError) as info:
            client.get(f"{server.url}/json")
    assert isinstance(info.value, (nextreq.ProxyError, nextreq.ConnectError))
    with pytest.raises(nextreq.InvalidURL, match="proxy"):
        nextreq.Client(proxy="://not-a-proxy")


async def test_async_client_uses_proxy(server: LiveServer, proxy: ProxyServer) -> None:
    async with nextreq.AsyncClient(proxy=proxy.url, trust_env=False) as client:
        assert (await client.get(f"{server.url}/json")).json() == {"hello": "world"}
    assert proxy.requests == [f"GET {server.url}/json"]


# -- RetryPolicy --------------------------------------------------------------


def test_retry_policy_defaults() -> None:
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.backoff_factor == 0.5 and policy.max_backoff == 30.0
    assert policy.jitter == 1.0
    assert policy.retry_statuses == frozenset({429, 502, 503, 504})
    assert policy.retry_methods == frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"})
    assert policy.respect_retry_after is True and policy.max_retry_after == 120.0
    assert "max_attempts=3" in repr(policy)
    assert policy == RetryPolicy(3) and policy != RetryPolicy(4) and policy != 3


def test_retry_policy_method_and_status_checks() -> None:
    policy = RetryPolicy(retry_statuses={500, 503}, retry_methods=["post", "GET"])
    assert policy.allows_status(500) and policy.allows_status(503) and not policy.allows_status(429)
    assert policy.allows_method("POST") and policy.allows_method("get")
    assert not policy.allows_method("DELETE")
    assert not RetryPolicy().allows_method("POST"), "POST is not idempotent"
    assert not RetryPolicy().allows_method("PATCH")


def test_retry_policy_backoff_without_jitter() -> None:
    policy = RetryPolicy(backoff_factor=0.5, jitter=False, max_backoff=3.0)
    assert [policy.delay(n) for n in range(1, 6)] == [0.5, 1.0, 2.0, 3.0, 3.0]
    assert RetryPolicy(backoff_factor=0).delay(4) == 0.0


def test_retry_policy_jitter_bounds() -> None:
    full = RetryPolicy(backoff_factor=1.0, jitter=True)
    samples = [full.delay(3) for _ in range(300)]
    assert all(0.0 <= s <= 4.0 for s in samples)
    assert max(samples) - min(samples) > 1.0, "full jitter should spread delays out"

    partial = RetryPolicy(backoff_factor=1.0, jitter=0.25)
    assert partial.jitter == 0.25
    assert all(3.0 <= partial.delay(3) <= 4.0 for _ in range(300))


def test_retry_policy_honours_retry_after() -> None:
    policy = RetryPolicy(backoff_factor=0.5, jitter=False, max_retry_after=10)
    assert policy.delay(1, "3") == 3.0
    assert policy.delay(1, "9999") == 10.0, "capped by max_retry_after"
    assert policy.delay(2, "not-a-date") == 1.0, "falls back to backoff"
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=5), usegmt=True)
    assert 3.0 <= policy.delay(1, future) <= 5.5
    past = format_datetime(datetime.now(timezone.utc) - timedelta(hours=1), usegmt=True)
    assert policy.delay(1, past) == 0.0
    ignoring = RetryPolicy(backoff_factor=0.5, jitter=False, respect_retry_after=False)
    assert ignoring.delay(1, "3") == 0.5


def test_retry_policy_validation() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(backoff_factor=-1)
    with pytest.raises(ValueError):
        RetryPolicy(jitter=1.5)
    with pytest.raises(ValueError):
        RetryPolicy(retry_statuses=[99])
    with pytest.raises(TypeError):
        RetryPolicy(retry_methods="GET")  # type: ignore[arg-type]
    with pytest.raises((OverflowError, TypeError, ValueError)):
        RetryPolicy(-1)
    with pytest.raises(ValueError):
        nextreq.Client(retries=-1)
    with pytest.raises(TypeError):
        nextreq.Client(retries="3")  # type: ignore[arg-type]


def test_parse_retry_after() -> None:
    assert nextreq.parse_retry_after("120") == 120.0
    assert nextreq.parse_retry_after(" 7 ") == 7.0
    assert nextreq.parse_retry_after("soon") is None
    assert nextreq.parse_retry_after("") is None
    assert nextreq.parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
    parsed = nextreq.parse_retry_after(future)
    assert parsed is not None and 28.0 <= parsed <= 30.5


# -- retries against the live server ------------------------------------------


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record backoff sleeps instead of actually sleeping."""
    recorded: list[float] = []

    async def fake_asleep(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr("nextreq.client._sleep", recorded.append)
    monkeypatch.setattr("nextreq.client._asleep", fake_asleep)
    return recorded


def test_no_retries_by_default(client: nextreq.Client, sleeps: list[float]) -> None:
    response = client.get("/flaky/default")
    assert response.status_code == 503 and response.retry_count == 0
    assert client.retries is None and sleeps == []


def test_retries_int_recovers_from_503(server: LiveServer, sleeps: list[float]) -> None:
    with nextreq.Client(base_url=server.url, retries=3) as client:
        response = client.get("/flaky/int", params={"fail": 2})
    assert response.status_code == 200
    assert response.json() == {"attempt": 3, "ok": True}
    assert response.retry_count == 2
    assert len(sleeps) == 2
    assert client.retries == RetryPolicy(3)


@pytest.mark.parametrize("status", [429, 502, 503, 504])
def test_default_retry_statuses(server: LiveServer, sleeps: list[float], status: int) -> None:
    with nextreq.Client(base_url=server.url, retries=2) as client:
        response = client.get(f"/flaky/s{status}", params={"fail": 1, "status": status})
    assert response.status_code == 200 and response.retry_count == 1


@pytest.mark.parametrize("status", [400, 404, 500, 501])
def test_other_statuses_are_not_retried(
    server: LiveServer, sleeps: list[float], status: int
) -> None:
    with nextreq.Client(base_url=server.url, retries=3) as client:
        response = client.get(f"/flaky/n{status}", params={"fail": 5, "status": status})
    assert response.status_code == status and response.retry_count == 0
    assert server.flaky[f"n{status}"] == 1 and sleeps == []


def test_exhausted_retries_return_the_last_response(
    server: LiveServer, sleeps: list[float]
) -> None:
    with nextreq.Client(base_url=server.url, retries=2) as client:
        response = client.get("/flaky/exhaust", params={"fail": 10})
        assert response.status_code == 503
        assert response.retry_count == 2
        assert server.flaky["exhaust"] == 3, "1 initial attempt + 2 retries"
        with pytest.raises(nextreq.HTTPStatusError):
            client.get("/flaky/exhaust2", params={"fail": 10}, raise_for_status=True)


def test_non_idempotent_methods_are_not_retried(server: LiveServer, sleeps: list[float]) -> None:
    with nextreq.Client(base_url=server.url, retries=3) as client:
        post = client.post("/flaky/post", params={"fail": 2})
        assert post.status_code == 503 and post.retry_count == 0
        assert server.flaky["post"] == 1
        for method in ("PUT", "DELETE", "OPTIONS"):
            response = client.request(method, f"/flaky/{method}", params={"fail": 1})
            assert response.status_code == 200 and response.retry_count == 1


def test_retry_methods_can_be_extended(server: LiveServer, sleeps: list[float]) -> None:
    policy = RetryPolicy(3, retry_methods=["POST"], retry_statuses=[500], backoff_factor=0)
    with nextreq.Client(base_url=server.url, retries=policy) as client:
        response = client.post("/flaky/custom", params={"fail": 2, "status": 500}, json={"a": 1})
        assert response.status_code == 200 and response.retry_count == 2
        # ...and GET is no longer retried, nor is 503.
        assert client.get("/flaky/custom-get", params={"fail": 1, "status": 500}).retry_count == 0
        assert client.post("/flaky/custom-503", params={"fail": 1}).status_code == 503


def test_per_request_retries_override(server: LiveServer, sleeps: list[float]) -> None:
    with nextreq.Client(base_url=server.url) as client:
        assert client.get("/flaky/a", params={"fail": 1}).status_code == 503
        assert client.get("/flaky/b", params={"fail": 1}, retries=1).status_code == 200
        policy = RetryPolicy(5, backoff_factor=0)
        assert client.get("/flaky/c", params={"fail": 4}, retries=policy).retry_count == 4
    with nextreq.Client(base_url=server.url, retries=3) as client:
        assert client.get("/flaky/d", params={"fail": 1}, retries=0).status_code == 503
        assert client.get("/flaky/e", params={"fail": 1}, retries=None).status_code == 503


def test_backoff_sequence_is_exponential(server: LiveServer, sleeps: list[float]) -> None:
    policy = RetryPolicy(4, backoff_factor=0.25, jitter=False)
    with nextreq.Client(base_url=server.url, retries=policy) as client:
        response = client.get("/flaky/backoff", params={"fail": 4})
    assert response.status_code == 200 and response.retry_count == 4
    assert sleeps == [0.25, 0.5, 1.0, 2.0]


def test_retry_after_delta_seconds_is_honoured(server: LiveServer, sleeps: list[float]) -> None:
    policy = RetryPolicy(3, backoff_factor=0.25, jitter=False)
    with nextreq.Client(base_url=server.url, retries=policy) as client:
        response = client.get("/flaky/ra", params={"fail": 2, "status": 429, "retry_after": "7"})
    assert response.status_code == 200 and response.retry_count == 2
    assert sleeps == [7.0, 7.0]


def test_retry_after_http_date_is_honoured(server: LiveServer, sleeps: list[float]) -> None:
    when = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=20), usegmt=True)
    policy = RetryPolicy(2, backoff_factor=0.25, jitter=False)
    with nextreq.Client(base_url=server.url, retries=policy) as client:
        response = client.get("/flaky/date", params={"fail": 1, "retry_after": when})
    assert response.retry_count == 1
    assert len(sleeps) == 1 and 17.0 <= sleeps[0] <= 20.5


def test_retry_after_is_capped_and_can_be_ignored(server: LiveServer, sleeps: list[float]) -> None:
    capped = RetryPolicy(1, max_retry_after=2.0, jitter=False)
    with nextreq.Client(base_url=server.url, retries=capped) as client:
        client.get("/flaky/cap", params={"fail": 1, "retry_after": "3600"})
    assert sleeps == [2.0]

    sleeps.clear()
    ignoring = RetryPolicy(1, respect_retry_after=False, backoff_factor=0.5, jitter=False)
    with nextreq.Client(base_url=server.url, retries=ignoring) as client:
        client.get("/flaky/ignore", params={"fail": 1, "retry_after": "3600"})
    assert sleeps == [0.5]


def test_real_backoff_actually_waits(server: LiveServer) -> None:
    policy = RetryPolicy(2, backoff_factor=0.1, jitter=False)
    with nextreq.Client(base_url=server.url, retries=policy) as client:
        started = time.perf_counter()
        response = client.get("/flaky/real", params={"fail": 2})
        elapsed = time.perf_counter() - started
    assert response.retry_count == 2
    assert elapsed >= 0.3, "0.1s + 0.2s of backoff"


def test_transport_errors_are_retried(server: LiveServer, sleeps: list[float]) -> None:
    with nextreq.Client(base_url=server.url, retries=2) as client:
        response = client.get("/drop-then-ok/t1")
    assert response.status_code == 200 and response.retry_count == 1
    assert response.json() == {"attempt": 2, "ok": True}
    assert len(sleeps) == 1


def test_transport_error_retries_exhaust_and_reraise(unused_port: int, sleeps: list[float]) -> None:
    with nextreq.Client(retries=2) as client:
        with pytest.raises(nextreq.ConnectError) as info:
            client.get(f"http://127.0.0.1:{unused_port}/")
    assert len(sleeps) == 2
    assert getattr(info.value, "retry_count", None) == 2


def test_timeouts_are_retried_for_idempotent_methods(
    server: LiveServer, sleeps: list[float]
) -> None:
    with nextreq.Client(base_url=server.url, retries=1, timeout=Timeout(read=0.15)) as client:
        with pytest.raises(nextreq.ReadTimeout):
            client.get("/delay/3")
        assert len(sleeps) == 1
        with pytest.raises(nextreq.ReadTimeout):
            client.post("/delay/3")  # POST is not idempotent, so it is never retried
    assert len(sleeps) == 1


def test_unretryable_errors_fail_fast(sleeps: list[float]) -> None:
    with nextreq.Client(retries=5) as client:
        with pytest.raises(nextreq.UnsupportedProtocol):
            client.get("ftp://example.com/")
        with pytest.raises(nextreq.InvalidURL):
            client.get("http://")
    assert sleeps == []


def test_retries_follow_redirect_chains(server: LiveServer, sleeps: list[float]) -> None:
    with nextreq.Client(base_url=server.url, retries=2) as client:
        response = client.get("/redirect-to", params={"url": "/flaky/redir?fail=1"})
    assert response.status_code == 200 and response.retry_count == 1
    assert [r.status_code for r in response.history] == [302]


def test_streamed_requests_can_be_retried(server: LiveServer, sleeps: list[float]) -> None:
    with nextreq.Client(base_url=server.url, retries=2) as client:
        with client.stream("GET", "/flaky/stream", params={"fail": 1}) as response:
            assert response.status_code == 200 and response.retry_count == 1
            assert response.read() == b'{"attempt": 2, "ok": true}'


async def test_async_retries(server: LiveServer, sleeps: list[float]) -> None:
    policy = RetryPolicy(3, backoff_factor=0.25, jitter=False)
    async with nextreq.AsyncClient(base_url=server.url, retries=policy) as client:
        response = await client.get("/flaky/async", params={"fail": 2})
        assert response.status_code == 200 and response.retry_count == 2
        assert sleeps == [0.25, 0.5]

        sleeps.clear()
        honoured = await client.get("/flaky/async-ra", params={"fail": 1, "retry_after": "4"})
        assert honoured.retry_count == 1 and sleeps == [4.0]

        dropped = await client.get("/drop-then-ok/async-drop")
        assert dropped.status_code == 200 and dropped.retry_count == 1

        post = await client.post("/flaky/async-post", params={"fail": 1})
        assert post.status_code == 503 and post.retry_count == 0

        override: Response = await client.get("/flaky/async-off", params={"fail": 1}, retries=0)
        assert override.status_code == 503


async def test_async_backoff_does_not_block_the_loop(server: LiveServer) -> None:
    policy = RetryPolicy(2, backoff_factor=0.1, jitter=False)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.005)

    task = asyncio.create_task(ticker())
    try:
        async with nextreq.AsyncClient(base_url=server.url, retries=policy) as client:
            response = await client.get("/flaky/loop", params={"fail": 2})
    finally:
        task.cancel()
    assert response.retry_count == 2
    assert ticks >= 8, "asyncio.sleep-based backoff must yield to the event loop"


def test_retry_hooks_see_every_attempt(server: LiveServer, sleeps: list[float]) -> None:
    statuses: list[int] = []
    requests: list[Any] = []
    with nextreq.Client(
        base_url=server.url,
        retries=3,
        on_request=requests.append,
        on_response=lambda r: statuses.append(r.status_code),
    ) as client:
        client.get("/flaky/hooks", params={"fail": 2})
    assert statuses == [503, 503, 200]
    assert len(requests) == 3
