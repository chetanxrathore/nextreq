# Contributing to nextreq

Thanks for helping! This document covers the development setup, the quality
gates every change must pass, and how the code is organised.

## Development setup

You need a Rust toolchain (stable, with `rustfmt` and `clippy`) and Python 3.10+.
[`uv`](https://docs.astral.sh/uv/) is recommended but plain `venv` + `pip` works.

```bash
git clone https://github.com/nextreq/nextreq
cd nextreq

uv venv                          # or: python3 -m venv .venv
source .venv/bin/activate
uv pip install -e ".[dev]"       # or: pip install -e ".[dev]"

maturin develop                  # debug build of the Rust extension, fast to compile
maturin develop --release        # optimised build - always use this for benchmarks
```

Re-run `maturin develop` after changing anything under `src/`. Python-only changes
are picked up immediately (the package is installed in editable mode).

> If you use conda, `maturin` refuses to run while both `VIRTUAL_ENV` and
> `CONDA_PREFIX` are set. Run `conda deactivate` (or `unset CONDA_PREFIX`) first.

## Quality gates

CI runs exactly these commands; all of them must exit 0.

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
maturin develop --release
ruff check python tests benches
mypy --strict python/nextreq tests/test_typing.py
pytest -v
python benches/run_benchmarks.py     # optional locally; add --quick for a smoke run
```

Handy while iterating:

```bash
ruff format python tests benches     # auto-format Python
cargo fmt                            # auto-format Rust
pytest tests/test_sse_ws.py -k parser -x
```

## Project layout

```
src/                  Rust core, compiled to the `nextreq._nextreq` extension
  lib.rs              module definition
  client.rs           engine: pooling, Tokio runtime, sync/async dispatch, timeouts
  config.rs           Timeout, RetryPolicy, proxy and TLS configuration
  sse.rs              WHATWG Server-Sent Events parser and the SSEEvent class
  ws.rs               WebSocket client
  errors.rs           Rust error -> Python exception mapping
python/nextreq/       the public Python API
  client.py           Client, AsyncClient, module-level helpers, gather()
  models.py           Request, Response, Headers, URL, Cookies
  parsing.py          typed JSON parsing (Pydantic, dataclasses, msgspec)
  testing.py          MockTransport, ASGI and WSGI transports
  otel.py             OpenTelemetry hooks
  exceptions.py       the NextReqError hierarchy
  _nextreq.pyi        type stubs for the Rust extension
tests/                pytest suite (local servers only, no network access)
benches/              benchmark server and runner
```

### Where does a change belong?

- **Bytes on the wire** (protocols, TLS, pooling, timeouts, decompression, parsing
  hot paths) belong in Rust.
- **HTTP semantics** (redirects, cookies, auth, retries, hooks, typed parsing)
  belong in Python, *above* the transport, so they behave identically for the real
  network and for `MockTransport` / ASGI / WSGI.

## Rules of the road

1. **Never hold the GIL while waiting on I/O.** Blocking calls go through
   `block_on_interruptible` (which detaches from the interpreter); async calls
   complete through `LoopBridge` or `pyo3-async-runtimes`. Code that runs on a
   Tokio worker must not touch Python objects - return plain Rust data and convert
   it at the boundary.
2. **Keep the stubs in sync.** Every symbol exported from Rust needs a matching
   entry in `python/nextreq/_nextreq.pyi`; `mypy --strict` and
   `tests/test_typing.py` guard this.
3. **Public API is fully typed.** New request methods need `@overload`s so that
   `model=` keeps returning the model type.
4. **Tests are hermetic.** Use the fixtures in `tests/conftest.py` (HTTP/SSE/WS
   server, TLS + HTTP/2 server, stalled listener, forward proxy). No test may
   reach the internet. Avoid sleeping; when timing matters, assert generous bounds.
5. **No new mandatory runtime dependencies.** Integrations (Pydantic, msgspec,
   OpenTelemetry) are detected at runtime and stay optional.
6. **Supported Python is 3.10+.** `mypy` is configured for 3.10; do not use newer
   syntax or stdlib features in `python/nextreq`.
7. **Performance changes need numbers.** Run `benches/run_benchmarks.py` before
   and after on a quiet machine and include both results in the pull request. Do
   not hand-edit `benches/RESULTS.md`.

## Commit messages and pull requests

- Reference the story or issue in the subject, e.g.
  `NXRQ-401: honour Retry-After HTTP-dates`.
- Explain *why* in the body, not just what changed.
- Add or update tests in the same commit as the behaviour they cover.
- Update `README.md` when user-facing behaviour changes.
- Keep pull requests focused; unrelated refactors make review slower.

## Optional features

HTTP/3 is feature-gated because `reqwest` still marks it unstable:

```bash
RUSTFLAGS="--cfg reqwest_unstable" maturin develop --release --features http3
```

## Reporting bugs and security issues

Use GitHub issues for bugs and feature requests - a failing test or a minimal
script is the most helpful thing you can attach. For anything security-sensitive,
follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## License

By contributing you agree that your contributions are dual-licensed under the
[MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE) licenses, as the rest of the
project is.
