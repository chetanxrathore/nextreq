"""Benchmark nextreq against requests, httpx and aiohttp.

Usage::

    maturin develop --release
    python benches/run_benchmarks.py            # full run, writes benches/RESULTS.md
    python benches/run_benchmarks.py --quick    # smoke run (smaller workloads, 1 round)

Methodology
-----------
* A local target server (Rust/hyper, see ``benches/server.rs``; falls back to
  the asyncio server in ``benches/server.py``) serves every workload over
  loopback, so there is no internet jitter.
* Every (scenario, library) pair runs in a **fresh subprocess**, several
  rounds each. That isolates imports and connection pools and makes peak RSS
  meaningful per library.
* Inside a process the workload ("trial", e.g. 500 requests) is first repeated
  for a fixed warm-up time, then timed several times with
  ``time.perf_counter``; the trial with the median throughput represents that
  process. The reported row is the process with the median throughput; every
  round is kept in the raw JSON.
* Latency is measured per request around the full exchange (send -> body read).
* Peak RSS is ``ru_maxrss`` of the worker process at exit: interpreter +
  library + workload.

Scenarios
---------
1. ``sequential``  - sequential sync GETs over one keep-alive connection
2. ``fanout``      - async fan-out, N requests with bounded concurrency
3. ``typed``       - GET + deserialize into a Pydantic v2 model
4. ``sse``         - consume a Server-Sent Events stream into event objects
5. ``large``       - (bonus) sequential 1 MiB downloads
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BENCH_DIR = Path(__file__).resolve().parent
ROOT = BENCH_DIR.parent

# ---------------------------------------------------------------------------
# Scenario catalogue
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    key: str
    title: str
    description: str
    unit: str
    libraries: tuple[str, ...]
    params: dict[str, int]
    quick_params: dict[str, int]
    has_latency: bool = True


SCENARIOS: dict[str, Scenario] = {
    "sequential": Scenario(
        key="sequential",
        title="Sequential sync GETs",
        description="{n} GET requests for a small JSON document, one after another, "
        "over a single keep-alive connection.",
        unit="req/s",
        libraries=("nextreq", "requests", "httpx", "aiohttp"),
        params={"n": 500},
        quick_params={"n": 100},
    ),
    "fanout": Scenario(
        key="fanout",
        title="High-concurrency async fan-out",
        description="{n} GET requests with at most {concurrency} in flight.",
        unit="req/s",
        libraries=("nextreq", "nextreq-gather", "aiohttp", "httpx", "requests"),
        params={"n": 1000, "concurrency": 50},
        quick_params={"n": 200, "concurrency": 20},
    ),
    "typed": Scenario(
        key="typed",
        title="Typed JSON deserialization",
        description="{n} GET requests for a ~1.3 kB JSON document, each validated into a "
        "Pydantic v2 model (nested models, a list of 10 sub-objects, a datetime).",
        unit="req/s",
        libraries=("nextreq", "requests", "httpx", "aiohttp"),
        params={"n": 500},
        quick_params={"n": 100},
    ),
    "sse": Scenario(
        key="sse",
        title="SSE streaming throughput",
        description="A stream of {events} Server-Sent Events, consumed into event objects "
        "(event, data, id, retry).",
        unit="events/s",
        libraries=("nextreq", "requests", "httpx", "aiohttp"),
        params={"events": 10_000},
        quick_params={"events": 2_000},
        has_latency=False,
    ),
    "large": Scenario(
        key="large",
        title="Large payload downloads (bonus)",
        description="{n} sequential GET requests, each downloading a {size_mib} MiB body.",
        unit="req/s",
        libraries=("nextreq", "requests", "httpx", "aiohttp"),
        params={"n": 100, "size_mib": 1},
        quick_params={"n": 20, "size_mib": 1},
    ),
}

LABELS: dict[tuple[str, str], str] = {
    ("sequential", "aiohttp"): "aiohttp (awaited sequentially)",
    ("fanout", "nextreq"): "nextreq (asyncio.gather + Semaphore)",
    ("fanout", "nextreq-gather"): "nextreq (client.gather)",
    ("fanout", "requests"): "requests (thread pool)",
    ("typed", "aiohttp"): "aiohttp (awaited sequentially)",
    ("sse", "requests"): "requests + hand-written parser",
    ("sse", "httpx"): "httpx + hand-written parser",
    ("sse", "aiohttp"): "aiohttp + hand-written parser",
    ("large", "aiohttp"): "aiohttp (awaited sequentially)",
}


def label_for(scenario: str, library: str) -> str:
    return LABELS.get((scenario, library), library)


# ---------------------------------------------------------------------------
# Worker-side helpers (run inside the per-library subprocess)
# ---------------------------------------------------------------------------


def peak_rss_mb() -> float | None:
    """Peak resident set size of this process in MiB (None if unavailable)."""
    try:
        import resource
    except ImportError:  # Windows
        try:
            import psutil
        except ImportError:
            return None
        return float(psutil.Process().memory_info().peak_wset) / (1024 * 1024)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports kibibytes.
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return float("nan")
    rank = max(1, round(fraction * len(sorted_values) + 0.5))
    return sorted_values[min(rank, len(sorted_values)) - 1]


def build_user_model() -> Any:
    """The Pydantic v2 model matching ``GET /user/{id}``."""
    import pydantic

    class Address(pydantic.BaseModel):
        street: str
        city: str
        country: str
        zip: str

    class Preferences(pydantic.BaseModel):
        theme: str
        language: str
        notifications: bool

    class Order(pydantic.BaseModel):
        order_id: int
        sku: str
        quantity: int
        unit_price: float
        shipped: bool

    class User(pydantic.BaseModel):
        id: int
        username: str
        email: str
        full_name: str
        active: bool
        score: float
        created_at: datetime.datetime
        address: Address
        tags: list[str]
        preferences: Preferences
        orders: list[Order]

    return User


class PlainEvent:
    """What a hand-written SSE consumer produces: the same four fields as
    ``nextreq.SSEEvent``."""

    __slots__ = ("data", "event", "id", "retry")

    def __init__(self, event: str, data: str, id: str | None, retry: int | None) -> None:
        self.event = event
        self.data = data
        self.id = id
        self.retry = retry


class LineParser:
    """A straightforward pure-Python SSE parser fed one decoded line at a time.

    This is the code people write on top of ``iter_lines()`` (and what small
    helper libraries such as sseclient / httpx-sse do): field parsing, data
    accumulation, last-event-ID tracking and dispatch on the blank line.
    """

    __slots__ = ("data", "event", "last_id", "retry")

    def __init__(self) -> None:
        self.event = "message"
        self.data: list[str] = []
        self.last_id: str | None = None
        self.retry: int | None = None

    def feed(self, line: str) -> PlainEvent | None:
        if not line:
            if not self.data:
                self.event = "message"
                self.retry = None
                return None
            dispatched = PlainEvent(self.event, "\n".join(self.data), self.last_id, self.retry)
            self.event = "message"
            self.data = []
            self.retry = None
            return dispatched
        if line[0] == ":":
            return None
        name, _, value = line.partition(":")
        if value[:1] == " ":
            value = value[1:]
        if name == "data":
            self.data.append(value)
        elif name == "event":
            self.event = value
        elif name == "id":
            if "\0" not in value:
                self.last_id = value or None
        elif name == "retry" and value.isdigit():
            self.retry = int(value)
        return None


def consume_lines(lines: Iterator[str]) -> int:
    parser = LineParser()
    count = 0
    feed = parser.feed
    for line in lines:
        event = feed(line)
        if event is not None:
            _ = event.data
            count += 1
    return count


# One trial returns: (operations, seconds, per-request latencies in ms or None)
Outcome = tuple[int, float, "list[float] | None"]

# Set by ``worker_main`` from the command line.
TRIALS = 5
WARMUP_SECONDS = 0.5


def _median_trial(outcomes: list[Outcome]) -> Outcome:
    outcomes.sort(key=lambda outcome: outcome[0] / outcome[1])
    return outcomes[len(outcomes) // 2]


def measure(trial: Callable[[], Outcome]) -> Outcome:
    """Warm up for ``WARMUP_SECONDS`` (at least one full trial), then run
    ``TRIALS`` measured trials and return the one with the median throughput.

    A single trial of 500 local requests lasts only tens of milliseconds, so
    one cold measurement mostly captures process start-up effects (CPU
    frequency ramp, scheduler placement, cold caches) rather than the client.
    """
    deadline = time.perf_counter() + WARMUP_SECONDS
    trial()
    while time.perf_counter() < deadline:
        trial()
    return _median_trial([trial() for _ in range(TRIALS)])


async def ameasure(trial: Callable[[], Awaitable[Outcome]]) -> Outcome:
    """Async twin of :func:`measure`."""
    deadline = time.perf_counter() + WARMUP_SECONDS
    await trial()
    while time.perf_counter() < deadline:
        await trial()
    return _median_trial([await trial() for _ in range(TRIALS)])


def sync_loop_trial(call: Callable[[str], Any], urls: list[str]) -> Callable[[], Outcome]:
    """A trial that calls ``call(url)`` for each URL, timing every call."""

    def trial() -> Outcome:
        latencies: list[float] = []
        record = latencies.append
        clock = time.perf_counter
        started = clock()
        for url in urls:
            begin = clock()
            call(url)
            record((clock() - begin) * 1000.0)
        return len(urls), clock() - started, latencies

    return trial


def async_loop_trial(
    call: Callable[[str], Awaitable[Any]], urls: list[str]
) -> Callable[[], Awaitable[Outcome]]:
    """A trial that awaits ``call(url)`` for each URL, one at a time."""

    async def trial() -> Outcome:
        latencies: list[float] = []
        clock = time.perf_counter
        started = clock()
        for url in urls:
            begin = clock()
            await call(url)
            latencies.append((clock() - begin) * 1000.0)
        return len(urls), clock() - started, latencies

    return trial


# -- scenario 1: sequential sync GETs ------------------------------------------


def run_sequential(library: str, base: str, n: int) -> Outcome:
    urls = [f"{base}/json"] * n
    if library == "nextreq":
        import nextreq

        with nextreq.Client() as client:
            return measure(sync_loop_trial(lambda u: client.get(u).content, urls))
    if library == "requests":
        import requests

        with requests.Session() as session:
            return measure(sync_loop_trial(lambda u: session.get(u).content, urls))
    if library == "httpx":
        import httpx

        with httpx.Client() as client:
            return measure(sync_loop_trial(lambda u: client.get(u).content, urls))
    if library == "aiohttp":
        import aiohttp

        async def main() -> Outcome:
            async with aiohttp.ClientSession() as session:

                async def fetch(u: str) -> None:
                    async with session.get(u) as response:
                        await response.read()

                return await ameasure(async_loop_trial(fetch, urls))

        return asyncio.run(main())
    raise ValueError(library)


# -- scenario 2: async fan-out ---------------------------------------------------


def fanout_trial(
    request: Callable[[str], Awaitable[Any]], url: str, n: int, concurrency: int
) -> Callable[[], Awaitable[Outcome]]:
    """The harness every asyncio library shares: gather() + a Semaphore."""

    async def trial() -> Outcome:
        semaphore = asyncio.Semaphore(concurrency)
        latencies: list[float] = []
        clock = time.perf_counter

        async def one() -> None:
            async with semaphore:
                begin = clock()
                await request(url)
                latencies.append((clock() - begin) * 1000.0)

        started = clock()
        await asyncio.gather(*(one() for _ in range(n)))
        return n, clock() - started, latencies

    return trial


async def _fanout(library: str, base: str, n: int, concurrency: int) -> Outcome:
    url = f"{base}/json"
    if library == "nextreq":
        import nextreq

        async with nextreq.AsyncClient() as client:

            async def nextreq_get(u: str) -> None:
                _ = (await client.get(u)).content

            return await ameasure(fanout_trial(nextreq_get, url, n, concurrency))
    if library == "nextreq-gather":
        import nextreq

        async with nextreq.AsyncClient() as client:
            urls = [url] * n

            async def gather_trial() -> Outcome:
                started = time.perf_counter()
                responses = await client.gather(urls, max_concurrency=concurrency)
                seconds = time.perf_counter() - started
                for response in responses:
                    _ = response.content
                # Measured per request by the Rust core (pool wait + exchange).
                return n, seconds, [r.elapsed.total * 1000.0 for r in responses]

            return await ameasure(gather_trial)
    if library == "aiohttp":
        import aiohttp

        connector = aiohttp.TCPConnector(limit=concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:

            async def aiohttp_get(u: str) -> None:
                async with session.get(u) as response:
                    await response.read()

            return await ameasure(fanout_trial(aiohttp_get, url, n, concurrency))
    if library == "httpx":
        import httpx

        # Default limits (100 connections) are httpx's fastest configuration here.
        async with httpx.AsyncClient() as client:

            async def httpx_get(u: str) -> None:
                _ = (await client.get(u)).content

            return await ameasure(fanout_trial(httpx_get, url, n, concurrency))
    raise ValueError(library)


def _fanout_threads(base: str, n: int, concurrency: int) -> Outcome:
    """requests has no async API; the idiomatic fan-out is a thread pool."""
    import requests
    from requests.adapters import HTTPAdapter

    url = f"{base}/json"
    with requests.Session() as session, ThreadPoolExecutor(max_workers=concurrency) as pool:
        session.mount(
            "http://", HTTPAdapter(pool_connections=concurrency, pool_maxsize=concurrency)
        )
        clock = time.perf_counter

        def one(_: int) -> float:
            begin = clock()
            _ = session.get(url).content
            return (clock() - begin) * 1000.0

        def trial() -> Outcome:
            started = clock()
            latencies = list(pool.map(one, range(n)))
            return n, clock() - started, latencies

        return measure(trial)


def run_fanout(library: str, base: str, n: int, concurrency: int) -> Outcome:
    if library == "requests":
        return _fanout_threads(base, n, concurrency)
    return asyncio.run(_fanout(library, base, n, concurrency))


# -- scenario 3: typed JSON ------------------------------------------------------


def run_typed(library: str, base: str, n: int) -> Outcome:
    user_model = build_user_model()
    urls = [f"{base}/user/{i}" for i in range(1, n + 1)]

    def checked(fetch: Callable[[str], Any]) -> Callable[[str], None]:
        def run(url: str) -> None:
            user = fetch(url)
            if len(user.orders) != 10 or not url.endswith(f"/{user.id}"):
                raise AssertionError(f"unexpected payload for {url}")

        return run

    if library == "nextreq":
        import nextreq

        with nextreq.Client() as client:
            # Typed parsing is built in: bytes -> validated model in one step.
            return measure(
                sync_loop_trial(checked(lambda u: client.get(u, model=user_model)), urls)
            )
    if library == "requests":
        import requests

        with requests.Session() as session:
            return measure(
                sync_loop_trial(
                    checked(lambda u: user_model.model_validate(session.get(u).json())), urls
                )
            )
    if library == "httpx":
        import httpx

        with httpx.Client() as client:
            return measure(
                sync_loop_trial(
                    checked(lambda u: user_model.model_validate(client.get(u).json())), urls
                )
            )
    if library == "aiohttp":
        import aiohttp

        async def main() -> Outcome:
            async with aiohttp.ClientSession() as session:

                async def fetch(url: str) -> None:
                    async with session.get(url) as response:
                        user = user_model.model_validate(await response.json())
                    if len(user.orders) != 10:
                        raise AssertionError(f"unexpected payload for {url}")

                return await ameasure(async_loop_trial(fetch, urls))

        return asyncio.run(main())
    raise ValueError(library)


# -- scenario 4: SSE -------------------------------------------------------------


def sse_trial(consume: Callable[[str], int], url: str, events: int) -> Callable[[], Outcome]:
    def trial() -> Outcome:
        started = time.perf_counter()
        received = consume(url)
        seconds = time.perf_counter() - started
        if received != events:
            raise AssertionError(f"expected {events} events, got {received}")
        return events, seconds, None

    return trial


def run_sse(library: str, base: str, events: int) -> Outcome:
    url = f"{base}/sse?count={events}"
    if library == "nextreq":
        import nextreq

        with nextreq.Client() as client:

            def nextreq_consume(u: str) -> int:
                count = 0
                with client.stream("GET", u) as response:
                    for event in response.iter_sse():
                        _ = event.data
                        count += 1
                return count

            return measure(sse_trial(nextreq_consume, url, events))
    if library == "requests":
        import requests

        with requests.Session() as session:

            def requests_consume(u: str) -> int:
                with session.get(u, stream=True) as response:
                    return consume_lines(response.iter_lines(decode_unicode=True))

            return measure(sse_trial(requests_consume, url, events))
    if library == "httpx":
        import httpx

        with httpx.Client() as client:

            def httpx_consume(u: str) -> int:
                with client.stream("GET", u) as response:
                    return consume_lines(response.iter_lines())

            return measure(sse_trial(httpx_consume, url, events))
    if library == "aiohttp":
        import aiohttp

        async def main() -> Outcome:
            async with aiohttp.ClientSession() as session:

                async def trial() -> Outcome:
                    parser = LineParser()
                    feed = parser.feed
                    received = 0
                    started = time.perf_counter()
                    async with session.get(url) as response:
                        async for raw in response.content:  # one line at a time
                            event = feed(raw.decode("utf-8").rstrip("\r\n"))
                            if event is not None:
                                _ = event.data
                                received += 1
                    seconds = time.perf_counter() - started
                    if received != events:
                        raise AssertionError(f"expected {events} events, got {received}")
                    return events, seconds, None

                return await ameasure(trial)

        return asyncio.run(main())
    raise ValueError(library)


# -- scenario 5: large downloads -------------------------------------------------


def run_large(library: str, base: str, n: int, size_mib: int) -> Outcome:
    size = size_mib * 1024 * 1024
    urls = [f"{base}/large?size={size}"] * n

    def checked(fetch: Callable[[str], bytes]) -> Callable[[str], None]:
        def run(url: str) -> None:
            if len(fetch(url)) != size:
                raise AssertionError("short body")

        return run

    if library == "nextreq":
        import nextreq

        with nextreq.Client() as client:
            return measure(sync_loop_trial(checked(lambda u: client.get(u).content), urls))
    if library == "requests":
        import requests

        with requests.Session() as session:
            return measure(sync_loop_trial(checked(lambda u: session.get(u).content), urls))
    if library == "httpx":
        import httpx

        with httpx.Client() as client:
            return measure(sync_loop_trial(checked(lambda u: client.get(u).content), urls))
    if library == "aiohttp":
        import aiohttp

        async def main() -> Outcome:
            async with aiohttp.ClientSession() as session:

                async def fetch(url: str) -> None:
                    async with session.get(url) as response:
                        if len(await response.read()) != size:
                            raise AssertionError("short body")

                return await ameasure(async_loop_trial(fetch, urls))

        return asyncio.run(main())
    raise ValueError(library)


RUNNERS: dict[str, Callable[..., Outcome]] = {
    "sequential": run_sequential,
    "fanout": run_fanout,
    "typed": run_typed,
    "sse": run_sse,
    "large": run_large,
}


def worker_main(
    scenario: str, library: str, base_url: str, params: dict[str, int], trials: int, warmup: float
) -> None:
    """Entry point of the per-library subprocess; prints one JSON line."""
    global TRIALS, WARMUP_SECONDS
    TRIALS, WARMUP_SECONDS = trials, warmup
    count, seconds, samples = RUNNERS[scenario](library, base_url, **params)
    result: dict[str, Any] = {
        "scenario": scenario,
        "library": library,
        "count": count,
        "seconds": seconds,
        "throughput": count / seconds if seconds > 0 else 0.0,
        "trial_ms": seconds * 1000.0,
        "trials": trials,
        "peak_rss_mb": peak_rss_mb(),
    }
    if samples:
        ordered = sorted(samples)
        result.update(
            p50_ms=percentile(ordered, 0.50),
            p95_ms=percentile(ordered, 0.95),
            p99_ms=percentile(ordered, 0.99),
            mean_ms=statistics.fmean(ordered),
            max_ms=ordered[-1],
        )
    print(json.dumps(result))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def find_cargo() -> str | None:
    found = shutil.which("cargo")
    if found:
        return found
    for candidate in (
        Path.home() / ".cargo" / "bin" / "cargo",
        Path("/opt/homebrew/opt/rustup/bin/cargo"),
        Path("/usr/local/opt/rustup/bin/cargo"),
    ):
        if candidate.exists():
            return str(candidate)
    return None


def rust_server_binary(build: bool) -> Path | None:
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    binary = (
        target_dir / "release" / "examples" / ("bench_server" + (".exe" if os.name == "nt" else ""))
    )
    cargo = find_cargo()
    if build and cargo:
        env = dict(os.environ)
        env["PATH"] = os.pathsep.join([str(Path(cargo).parent), env.get("PATH", "")])
        completed = subprocess.run(
            [cargo, "build", "--release", "--example", "bench_server"],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            print("  cargo build failed; falling back to the Python server", file=sys.stderr)
            print(completed.stderr[-2000:], file=sys.stderr)
            return None
    return binary if binary.exists() else None


@contextlib.contextmanager
def target_server(kind: str) -> Iterator[tuple[str, str]]:
    """Start the benchmark target; yields ``(base_url, description)``."""
    command: list[str] | None = None
    description = ""
    if kind in ("auto", "rust"):
        binary = rust_server_binary(build=True)
        if binary is not None:
            command = [str(binary), "--port", "0"]
            description = "Rust (hyper 1.x, 4 Tokio workers) - `benches/server.rs`"
        elif kind == "rust":
            raise SystemExit("--server rust requested but the Rust server could not be built")
    if command is None:
        command = [sys.executable, str(BENCH_DIR / "server.py"), "--port", "0"]
        description = "Python (asyncio.Protocol) - `benches/server.py`"

    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout is not None
        line = process.stdout.readline().strip()
        if not line.startswith("LISTENING "):
            raise SystemExit(f"benchmark server failed to start (got {line!r})")
        yield f"http://127.0.0.1:{int(line.split()[1])}", description
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


def run_worker(
    scenario: str, library: str, base_url: str, params: dict[str, int], trials: int, warmup: float
) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            "--scenario",
            scenario,
            "--library",
            library,
            "--base-url",
            base_url,
            "--params",
            json.dumps(params),
            "--trials",
            str(trials),
            "--warmup",
            str(warmup),
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"worker {scenario}/{library} failed (exit {completed.returncode}):\n{completed.stderr[-3000:]}"
        )
    result: dict[str, Any] = json.loads(completed.stdout.strip().splitlines()[-1])
    return result


def library_versions() -> dict[str, str]:
    from importlib.metadata import PackageNotFoundError, version

    versions: dict[str, str] = {}
    for package in (
        "nextreq",
        "requests",
        "httpx",
        "httpcore",
        "aiohttp",
        "pydantic",
        "pydantic-core",
    ):
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = "not installed"
    return versions


def cpu_description() -> str:
    try:
        if sys.platform == "darwin":
            return subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        if sys.platform.startswith("linux"):
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return platform.processor() or platform.machine()


def environment(server_description: str, rounds: int, trials: int, quick: bool) -> dict[str, Any]:
    return {
        "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "platform": platform.platform(),
        "cpu": cpu_description(),
        "cpu_count": os.cpu_count(),
        "python": f"{platform.python_implementation()} {platform.python_version()}",
        "server": server_description,
        "rounds": rounds,
        "trials": trials,
        "mode": "quick" if quick else "full",
        "versions": library_versions(),
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def fmt_number(value: float | None, digits: int = 0) -> str:
    if value is None or value != value:  # None or NaN
        return "—"
    return f"{value:,.{digits}f}"


def fmt_ms(value: float | None) -> str:
    if value is None or value != value:
        return "—"
    if value >= 100:
        return f"{value:,.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.3f}" if value < 1 else f"{value:.2f}"


def baseline_for(scenario: Scenario, rows: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    return rows.get("nextreq")


def render_table(scenario: Scenario, rows: dict[str, dict[str, Any]]) -> list[str]:
    baseline = baseline_for(scenario, rows)
    throughput_header = "Events/sec" if scenario.unit == "events/s" else "Requests/sec"
    if scenario.has_latency:
        header = f"| Library | {throughput_header} | nextreq speedup | p50 (ms) | p95 (ms) | p99 (ms) | Peak RSS (MB) |"
        divider = "|---|---:|---:|---:|---:|---:|---:|"
    else:
        header = f"| Library | {throughput_header} | nextreq speedup | Time per stream (ms) | Peak RSS (MB) |"
        divider = "|---|---:|---:|---:|---:|"
    lines = [header, divider]
    ordered = sorted(rows.values(), key=lambda row: row["throughput"], reverse=True)
    for row in ordered:
        name = label_for(scenario.key, row["library"])
        if row["library"].startswith("nextreq"):
            name = f"**{name}**"
        if baseline is None or row["library"].startswith("nextreq"):
            speedup = "baseline" if row["library"] == "nextreq" else "—"
        else:
            ratio = (
                baseline["throughput"] / row["throughput"] if row["throughput"] else float("inf")
            )
            speedup = f"{ratio:.2f}×"
        cells = [name, fmt_number(row["throughput"]), speedup]
        if scenario.has_latency:
            cells += [
                fmt_ms(row.get("p50_ms")),
                fmt_ms(row.get("p95_ms")),
                fmt_ms(row.get("p99_ms")),
            ]
        else:
            cells.append(fmt_ms(row.get("trial_ms")))
        cells.append(fmt_number(row.get("peak_rss_mb"), 1))
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def takeaways(results: dict[str, dict[str, dict[str, Any]]]) -> list[str]:
    points: list[str] = []

    def ratio(scenario: str, library: str, baseline: str = "nextreq") -> float | None:
        rows = results.get(scenario, {})
        if baseline not in rows or library not in rows or not rows[library]["throughput"]:
            return None
        return float(rows[baseline]["throughput"] / rows[library]["throughput"])

    def describe(scenario: str, libraries: tuple[str, ...], baseline: str = "nextreq") -> str:
        parts = []
        for library in libraries:
            value = ratio(scenario, library, baseline)
            if value is not None:
                parts.append(f"**{value:.1f}×** {library}")
        return ", ".join(parts)

    if "sequential" in results:
        rows = results["sequential"]
        text = describe("sequential", ("requests", "httpx", "aiohttp"))
        if text and "nextreq" in rows:
            points.append(
                f"**Sequential sync GETs:** nextreq handles {fmt_number(rows['nextreq']['throughput'])} req/s "
                f"at a p50 of {fmt_ms(rows['nextreq'].get('p50_ms'))} ms - {text}."
            )
    if "fanout" in results:
        rows = results["fanout"]
        text = describe("fanout", ("aiohttp", "httpx", "requests"))
        if text and "nextreq" in rows:
            extra = ""
            if "nextreq-gather" in rows:
                gather_ratio = rows["nextreq-gather"]["throughput"] / rows["nextreq"]["throughput"]
                against_aiohttp = ratio("fanout", "aiohttp", "nextreq-gather")
                extra = (
                    f" The built-in `client.gather()` reaches "
                    f"{fmt_number(rows['nextreq-gather']['throughput'])} req/s "
                    f"({gather_ratio:.2f}× the asyncio.gather harness"
                )
                if against_aiohttp is not None:
                    extra += f", {against_aiohttp:.1f}× aiohttp"
                extra += ")."
            points.append(
                f"**Async fan-out:** with the same `asyncio.gather` + `Semaphore` harness nextreq reaches "
                f"{fmt_number(rows['nextreq']['throughput'])} req/s - {text}.{extra}"
            )
    if "typed" in results:
        text = describe("typed", ("requests", "httpx", "aiohttp"))
        if text:
            points.append(
                f"**Typed JSON:** `client.get(url, model=User)` (bytes validated straight into the model "
                f"by pydantic-core) is {text} doing `User.model_validate(response.json())`."
            )
    if "sse" in results:
        rows = results["sse"]
        text = describe("sse", ("requests", "httpx", "aiohttp"))
        if text and "nextreq" in rows:
            points.append(
                f"**SSE:** the Rust parser delivers {fmt_number(rows['nextreq']['throughput'])} events/s "
                f"as ready-made `SSEEvent` objects - {text} with a hand-written Python parser."
            )
    if "large" in results:
        text = describe("large", ("requests", "httpx", "aiohttp"))
        if text:
            points.append(
                f"**Large downloads:** for 1 MiB bodies, where memory copies rather than per-request "
                f"overhead dominate, nextreq is {text}."
            )
    rss = [
        (scenario, rows["nextreq"].get("peak_rss_mb"))
        for scenario, rows in results.items()
        if "nextreq" in rows and rows["nextreq"].get("peak_rss_mb")
    ]
    if rss:
        low = min(value for _, value in rss if value)
        high = max(value for _, value in rss if value)
        points.append(
            f"**Memory:** nextreq's peak RSS stays between {low:.0f} and {high:.0f} MB across scenarios "
            f"(interpreter included); see each table for the per-library comparison."
        )
    return points


def render_markdown(
    env: dict[str, Any],
    params: dict[str, dict[str, int]],
    results: dict[str, dict[str, dict[str, Any]]],
    raw: dict[str, Any],
) -> str:
    versions = env["versions"]
    out: list[str] = [
        "# nextreq benchmark results",
        "",
        "> Generated by `python benches/run_benchmarks.py`"
        + (" `--quick`" if env["mode"] == "quick" else "")
        + f" on {env['date']}. Do not edit by hand; re-run the script instead.",
        "",
        "## Environment",
        "",
        "| | |",
        "|---|---|",
        f"| Machine | {env['cpu']} ({env['cpu_count']} logical cores) |",
        f"| OS | {env['platform']} |",
        f"| Python | {env['python']} |",
        "| nextreq build | `maturin develop --release` (LTO, `opt-level=3`) |",
        f"| Target server | {env['server']} |",
        f"| Sampling | {env['rounds']} fresh subprocesses per (scenario, library), each timing "
        f"{env['trials']} trials after a time-based warm-up; the median trial of the median process "
        "is reported |",
        "| Versions | " + ", ".join(f"{name} {value}" for name, value in versions.items()) + " |",
        "",
    ]
    if env["mode"] == "quick":
        out += [
            "> **Quick mode:** reduced workloads and a single round. Numbers are indicative only.",
            "",
        ]

    out += ["## Key takeaways", ""]
    out += [f"- {point}" for point in takeaways(results)] or ["- (no results)"]
    out += ["", "\"N× library\" means nextreq's throughput is N times that library's.", ""]

    out += ["## Results", ""]
    for index, (key, rows) in enumerate(results.items(), start=1):
        scenario = SCENARIOS[key]
        out += [
            f"### {index}. {scenario.title}",
            "",
            scenario.description.format(**params[key]),
            "",
            *render_table(scenario, rows),
            "",
        ]

    out += [
        "## How to read these numbers",
        "",
        "- **Everything runs on one machine over loopback.** That removes network jitter and makes",
        "  client overhead visible; over a real network, latency is dominated by the network and the",
        "  relative gaps shrink. Treat these as *client overhead* comparisons.",
        "- **Latency columns** are per request, measured in the client process around the complete",
        "  exchange. For `client.gather` they come from `response.elapsed`, measured in the Rust core",
        "  (pool wait + exchange); they exclude time a finished response waits for the event loop.",
        "- **Sequential rows for aiohttp** await one request at a time on an event loop, since aiohttp",
        "  has no synchronous API.",
        "- **Fan-out:** every asyncio library uses the identical `asyncio.gather` + `Semaphore` harness.",
        "  `requests` uses a thread pool of the same width. httpx runs with its default pool limits,",
        "  its fastest configuration in this workload; its async connection pool is known to degrade",
        "  under concurrency.",
        "- **Typed JSON:** competitors run the usual `Model.model_validate(response.json())`. nextreq's",
        "  `model=` argument hands the raw bytes to pydantic-core (`model_validate_json`), skipping the",
        "  intermediate Python objects. Calling `model_validate_json(response.content)` by hand on the",
        "  other clients would narrow this gap to roughly the scenario-1 transport difference.",
        "- **SSE:** nextreq parses in Rust and yields `SSEEvent` objects. The other clients are given",
        "  the same small pure-Python parser on top of their line iterators, producing objects with the",
        "  same four fields. The stream is a firehose (events are never delayed by the server).",
        "- **Peak RSS** is the whole worker process (CPython, the library and its imports, the",
        "  workload), so it mostly reflects each library's import footprint.",
        "",
        "## Reproducing",
        "",
        "```bash",
        "uv venv && uv pip install -e '.[dev]'",
        "maturin develop --release",
        "python benches/run_benchmarks.py",
        "```",
        "",
        "## Raw results",
        "",
        "Every round of every worker, as recorded (also written to `benches/results.json`).",
        "",
        "<details>",
        "<summary>Raw JSON</summary>",
        "",
        "```json",
        json.dumps(raw, indent=2, sort_keys=True),
        "```",
        "",
        "</details>",
        "",
    ]
    return "\n".join(out)


def orchestrate(args: argparse.Namespace) -> int:
    selected = [s.strip() for s in args.only.split(",")] if args.only else list(SCENARIOS)
    unknown = [name for name in selected if name not in SCENARIOS]
    if unknown:
        raise SystemExit(
            f"unknown scenario(s): {', '.join(unknown)}; choose from {', '.join(SCENARIOS)}"
        )
    rounds = args.rounds if args.rounds else (1 if args.quick else 3)
    trials = args.trials if args.trials else (2 if args.quick else 5)
    warmup = 0.2 if args.quick else 0.5

    print("Preparing benchmark target server ...", flush=True)
    with target_server(args.server) as (base_url, server_description):
        print(f"  {server_description} at {base_url}", flush=True)
        env = environment(server_description, rounds, trials, args.quick)
        results: dict[str, dict[str, dict[str, Any]]] = {}
        raw_runs: dict[str, dict[str, list[dict[str, Any]]]] = {}
        params_used: dict[str, dict[str, int]] = {}

        for key in selected:
            scenario = SCENARIOS[key]
            params = scenario.quick_params if args.quick else scenario.params
            params_used[key] = params
            print(f"\n[{scenario.title}] {scenario.description.format(**params)}", flush=True)
            results[key] = {}
            raw_runs[key] = {}
            for library in scenario.libraries:
                runs = [
                    run_worker(key, library, base_url, params, trials, warmup)
                    for _ in range(rounds)
                ]
                runs.sort(key=lambda run: run["throughput"])
                median = runs[len(runs) // 2]
                results[key][library] = median
                raw_runs[key][library] = runs
                spread = f"{runs[0]['throughput']:,.0f}..{runs[-1]['throughput']:,.0f}"
                print(
                    f"  {label_for(key, library):40s} {median['throughput']:>12,.0f} {scenario.unit:9s}"
                    f"  p50 {fmt_ms(median.get('p50_ms')):>8s} ms"
                    f"  rss {fmt_number(median.get('peak_rss_mb'), 1):>6s} MB   (rounds: {spread})",
                    flush=True,
                )

    raw = {"environment": env, "parameters": params_used, "median": results, "runs": raw_runs}
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown(env, params_used, results, raw), encoding="utf-8")
    raw_path = output.with_name("results.json")
    raw_path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nWrote {output}\nWrote {raw_path}", flush=True)

    print()
    for key, rows in results.items():
        print(f"## {SCENARIOS[key].title}")
        print("\n".join(render_table(SCENARIOS[key], rows)))
        print()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark nextreq against requests, httpx and aiohttp.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--quick", action="store_true", help="small workloads, one round (CI smoke run)"
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=0,
        help="fresh processes per library (0 = 3, or 1 with --quick)",
    )
    parser.add_argument(
        "--trials", type=int, default=0, help="timed trials per process (0 = 5, or 2 with --quick)"
    )
    parser.add_argument("--warmup", type=float, default=0.5, help=argparse.SUPPRESS)
    parser.add_argument("--only", default="", help="comma-separated scenarios to run")
    parser.add_argument("--server", choices=("auto", "rust", "python"), default="auto")
    parser.add_argument(
        "--output", default=str(BENCH_DIR / "RESULTS.md"), help="Markdown report path"
    )
    # Internal: per-library worker mode.
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--scenario", help=argparse.SUPPRESS)
    parser.add_argument("--library", help=argparse.SUPPRESS)
    parser.add_argument("--base-url", help=argparse.SUPPRESS)
    parser.add_argument("--params", default="{}", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        worker_main(
            args.scenario,
            args.library,
            args.base_url,
            json.loads(args.params),
            args.trials or 1,
            args.warmup,
        )
        return 0
    return orchestrate(args)


if __name__ == "__main__":
    sys.exit(main())
