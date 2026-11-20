"""NXRQ-501: request/response hooks, timing metrics and OpenTelemetry."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

import nextreq
from nextreq import MockTransport, Request, Response, otel

from .conftest import LiveServer

API = "https://api.example.test"

# -- hooks --------------------------------------------------------------------


def test_sync_hooks_fire_in_order(client: nextreq.Client) -> None:
    events: list[str] = []
    client.add_request_hook(lambda request: events.append(f"request:{request.method}"))
    client.add_response_hook(lambda response: events.append(f"response:{response.status_code}"))
    client.get("/json")
    client.post("/status/404")
    assert events == ["request:GET", "response:200", "request:POST", "response:404"]


def test_hooks_accept_a_callable_or_a_list(server: LiveServer) -> None:
    seen: list[str] = []
    with nextreq.Client(
        base_url=server.url,
        on_request=lambda request: seen.append("single"),
        on_response=[lambda r: seen.append("first"), lambda r: seen.append("second")],
    ) as client:
        client.get("/json")
    assert seen == ["single", "first", "second"]


def test_request_hook_can_modify_the_request(server: LiveServer) -> None:
    def sign(request: Request) -> None:
        request.headers["X-Signature"] = (
            f"{request.method}:{request.url.path}:{len(request.content)}"
        )
        request.extensions["signed"] = True

    captured: list[Response] = []
    with nextreq.Client(
        base_url=server.url, on_request=sign, on_response=captured.append
    ) as client:
        echoed = client.post("/echo", content=b"12345").json()
    assert echoed["headers"]["x-signature"] == "POST:/echo:5"
    assert captured[0].request is not None and captured[0].request.extensions == {"signed": True}


def test_response_hook_sees_headers_timings_and_can_raise(server: LiveServer) -> None:
    timings: list[float] = []

    def record(response: Response) -> None:
        timings.append(response.elapsed.total)
        if response.status_code == 418:
            raise RuntimeError("no tea allowed")

    with nextreq.Client(base_url=server.url, on_response=record) as client:
        assert client.get("/delay/0.05").status_code == 200
        with pytest.raises(RuntimeError, match="no tea"):
            client.get("/status/418")
    assert timings[0] >= 0.05 and len(timings) == 2


def test_hooks_fire_for_every_redirect_hop(client: nextreq.Client) -> None:
    paths: list[str] = []
    statuses: list[int] = []
    client.add_request_hook(lambda request: paths.append(request.url.path))
    client.add_response_hook(lambda response: statuses.append(response.status_code))
    client.get("/redirect/2")
    assert paths == ["/redirect/2", "/redirect/1", "/echo"]
    assert statuses == [302, 302, 200]


def test_error_hook_receives_transport_failures(unused_port: int) -> None:
    failures: list[tuple[str, str]] = []
    responses: list[Response] = []
    with nextreq.Client(
        on_error=lambda request, exc: failures.append((request.method, type(exc).__name__)),
        on_response=responses.append,
    ) as client:
        with pytest.raises(nextreq.ConnectError):
            client.get(f"http://127.0.0.1:{unused_port}/")
    assert failures == [("GET", "ConnectError")]
    assert responses == []


async def test_async_hooks_may_be_sync_or_async(server: LiveServer) -> None:
    events: list[str] = []

    async def async_request_hook(request: Request) -> None:
        await asyncio.sleep(0)
        request.headers["X-Async-Hook"] = "yes"
        events.append("async-request")

    def sync_request_hook(request: Request) -> None:
        events.append("sync-request")

    async def async_response_hook(response: Response) -> None:
        await asyncio.sleep(0)
        events.append(f"async-response:{response.status_code}")

    async def async_error_hook(request: Request, exc: BaseException) -> None:
        await asyncio.sleep(0)
        events.append(f"async-error:{type(exc).__name__}")

    async with nextreq.AsyncClient(
        base_url=server.url,
        on_request=[async_request_hook, sync_request_hook],
        on_response=[async_response_hook, lambda r: events.append("sync-response")],
        on_error=async_error_hook,
    ) as client:
        echoed = (await client.get("/echo")).json()
        assert echoed["headers"]["x-async-hook"] == "yes"
        with pytest.raises(nextreq.UnsupportedProtocol):
            await client.get("ftp://example.test/")
    assert events == [
        "async-request",
        "sync-request",
        "async-response:200",
        "sync-response",
        "async-request",
        "sync-request",
        "async-error:UnsupportedProtocol",
    ]


async def test_hooks_run_for_each_request_in_gather(server: LiveServer) -> None:
    count = 0

    def bump(response: Response) -> None:
        nonlocal count
        count += 1

    async with nextreq.AsyncClient(base_url=server.url, on_response=bump) as client:
        await client.gather(["/json"] * 25, max_concurrency=5)
    assert count == 25


def test_hooks_work_with_mock_transport() -> None:
    mock = MockTransport()
    mock.get(f"{API}/x").respond(json={"ok": True})
    log: list[str] = []
    with nextreq.Client(
        transport=mock,
        on_request=lambda request: log.append(str(request.url)),
        on_response=lambda response: log.append(str(response.status_code)),
    ) as client:
        client.get(f"{API}/x")
    assert log == [f"{API}/x", "200"]


# -- timing metrics -----------------------------------------------------------


def test_elapsed_for_streamed_and_mocked_responses(client: nextreq.Client) -> None:
    with client.stream("GET", "/stream/3") as response:
        elapsed = response.elapsed
        assert elapsed.total > 0 and elapsed.ttfb is not None
        assert elapsed.download is None, "a stream has not been downloaded when headers arrive"
        response.read()

    mock = MockTransport()
    mock.get(f"{API}/x").respond()
    with nextreq.Client(transport=mock) as mocked:
        timings = mocked.get(f"{API}/x").elapsed
    assert timings.total >= 0
    assert timings.as_dict() == {
        "total": timings.total,
        "pool": None,
        "connect": None,
        "ttfb": None,
        "download": None,
    }
    assert Response(200).elapsed.total == 0.0


async def test_async_elapsed_phases(aclient: nextreq.AsyncClient) -> None:
    first = await aclient.get("/delay/0.05")
    assert first.elapsed.connect is not None and first.elapsed.ttfb is not None
    assert first.elapsed.ttfb >= 0.05 and first.elapsed.total >= first.elapsed.ttfb
    second = await aclient.get("/json")
    assert second.elapsed.connect is None


# -- W3C trace context without OpenTelemetry ----------------------------------


def test_generate_and_parse_traceparent() -> None:
    header = otel.generate_traceparent()
    assert re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-01", header)
    parsed = otel.parse_traceparent(header)
    assert parsed is not None
    trace_id, parent_id, sampled = parsed
    assert len(trace_id) == 32 and len(parent_id) == 16 and sampled is True

    continued = otel.generate_traceparent(trace_id, sampled=False)
    assert continued.startswith(f"00-{trace_id}-") and continued.endswith("-00")
    assert otel.parse_traceparent(continued) == (trace_id, continued.split("-")[2], False)
    assert otel.generate_traceparent() != otel.generate_traceparent()

    for bad in (
        "",
        "garbage",
        "00-" + "0" * 32 + "-" + "1" * 16 + "-01",
        "ff-" + "a" * 32 + "-" + "b" * 16 + "-01",
    ):
        assert otel.parse_traceparent(bad) is None
    with pytest.raises(ValueError):
        otel.generate_traceparent("not-hex")
    with pytest.raises(ValueError):
        otel.generate_traceparent("0" * 32)


def test_traceparent_hook_injects_a_header(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, on_request=otel.traceparent_hook) as client:
        headers = client.get("/echo").json()["headers"]
        assert otel.parse_traceparent(headers["traceparent"]) is not None
        explicit = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
        kept = client.get("/echo", headers={"traceparent": explicit}).json()["headers"]
        assert kept["traceparent"] == explicit, "an existing header is never overwritten"


# -- OpenTelemetry spans ------------------------------------------------------


@pytest.fixture
def spans() -> Any:
    """An isolated tracer provider with an in-memory exporter."""
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    exporter.provider = provider  # type: ignore[attr-defined]
    return exporter


def test_otel_is_available() -> None:
    pytest.importorskip("opentelemetry.trace")
    assert otel.is_available() is True


def test_otel_records_client_spans_and_injects_traceparent(server: LiveServer, spans: Any) -> None:
    from opentelemetry.trace import SpanKind, StatusCode

    with otel.instrument(
        nextreq.Client(base_url=server.url), tracer_provider=spans.provider
    ) as client:
        echoed = client.get("/echo", params={"q": "1"}).json()
        client.post("/status/503")

    finished = spans.get_finished_spans()
    assert [span.name for span in finished] == ["GET", "POST"]
    ok, failed = finished
    assert ok.kind == SpanKind.CLIENT
    assert ok.attributes["http.request.method"] == "GET"
    assert ok.attributes["url.full"] == f"{server.url}/echo?q=1"
    assert ok.attributes["server.address"] == "127.0.0.1"
    assert ok.attributes["server.port"] == server.port
    assert ok.attributes["url.scheme"] == "http"
    assert ok.attributes["http.response.status_code"] == 200
    assert ok.attributes["network.protocol.version"] == "1.1"
    assert ok.status.status_code == StatusCode.UNSET
    assert ok.instrumentation_scope.name == "nextreq"
    assert ok.instrumentation_scope.version == nextreq.__version__

    # The header the server saw points at exactly this span.
    parsed = otel.parse_traceparent(echoed["headers"]["traceparent"])
    assert parsed is not None
    assert parsed[0] == format(ok.context.trace_id, "032x")
    assert parsed[1] == format(ok.context.span_id, "016x")

    assert failed.attributes["http.response.status_code"] == 503
    assert failed.attributes["error.type"] == "503"
    assert failed.status.status_code == StatusCode.ERROR


def test_otel_spans_join_the_active_trace(server: LiveServer, spans: Any) -> None:
    tracer = spans.provider.get_tracer("test")
    with otel.instrument(
        nextreq.Client(base_url=server.url), tracer_provider=spans.provider
    ) as client:
        with tracer.start_as_current_span("parent-operation") as parent:
            headers = client.get("/echo").json()["headers"]
    child = next(span for span in spans.get_finished_spans() if span.name == "GET")
    assert child.parent is not None and child.parent.span_id == parent.get_span_context().span_id
    assert child.context.trace_id == parent.get_span_context().trace_id
    parsed = otel.parse_traceparent(headers["traceparent"])
    assert parsed is not None and parsed[0] == format(parent.get_span_context().trace_id, "032x")


def test_otel_records_transport_errors(unused_port: int, spans: Any) -> None:
    from opentelemetry.trace import StatusCode

    client = otel.instrument(nextreq.Client(), tracer_provider=spans.provider)
    with client, pytest.raises(nextreq.ConnectError):
        client.get(f"http://user:secret@127.0.0.1:{unused_port}/path")
    (span,) = spans.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["error.type"] == "ConnectError"
    assert "secret" not in span.attributes["url.full"], "credentials are redacted"
    assert [event.name for event in span.events] == ["exception"]
    assert span.end_time is not None


def test_otel_one_span_per_redirect_hop_and_retry(server: LiveServer, spans: Any) -> None:
    policy = nextreq.RetryPolicy(2, backoff_factor=0)
    with otel.instrument(
        nextreq.Client(base_url=server.url, retries=policy), tracer_provider=spans.provider
    ) as client:
        client.get("/redirect/2")
        client.get("/flaky/otel", params={"fail": 1})
    finished = spans.get_finished_spans()
    assert [s.attributes["http.response.status_code"] for s in finished] == [
        302,
        302,
        200,
        503,
        200,
    ]
    assert len({span.context.span_id for span in finished}) == 5


async def test_otel_with_async_client_and_gather(server: LiveServer, spans: Any) -> None:
    async with otel.instrument(
        nextreq.AsyncClient(base_url=server.url), tracer_provider=spans.provider
    ) as client:
        responses = await client.gather([f"/echo?n={n}" for n in range(10)], max_concurrency=4)
    finished = spans.get_finished_spans()
    assert len(finished) == 10
    sent = {r.json()["headers"]["traceparent"].split("-")[2] for r in responses}
    assert sent == {format(span.context.span_id, "016x") for span in finished}


def test_otel_hooks_can_be_wired_manually(spans: Any) -> None:
    hooks = otel.OpenTelemetryHooks(tracer_provider=spans.provider)
    mock = MockTransport()
    mock.get(f"{API}/x").respond(204)
    with nextreq.Client(
        transport=mock,
        on_request=hooks.on_request,
        on_response=hooks.on_response,
        on_error=hooks.on_error,
    ) as client:
        client.get(f"{API}/x")
    (span,) = spans.get_finished_spans()
    assert span.attributes["http.response.status_code"] == 204
    assert span.attributes["server.address"] == "api.example.test"
    assert span.attributes["server.port"] == 443
    assert mock.last_request is not None and "traceparent" in mock.last_request.headers


def test_otel_missing_dependency_gives_a_helpful_error(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = otel.importlib.import_module

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("opentelemetry"):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(otel.importlib, "import_module", fake_import)
    assert otel.is_available() is False
    with pytest.raises(ImportError, match=r"nextreq\[otel\]"):
        otel.instrument(nextreq.Client())
    # The dependency-free hook still works.
    request = Request("GET", "https://example.test/")
    otel.traceparent_hook(request)
    assert otel.parse_traceparent(request.headers["traceparent"]) is not None
