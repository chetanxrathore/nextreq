# Security Policy

## Supported versions

nextreq is pre-1.0. Security fixes are released for the latest minor version only.

| Version | Supported |
|---------|-----------|
| 0.2.x   | yes       |
| < 0.2   | no        |

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository ("Security" tab → "Report a vulnerability").

Please include:

- the nextreq version, Python version and platform;
- a description of the issue and its impact;
- a minimal reproduction (a script or failing test is ideal);
- whether the issue is already public.

What to expect:

- acknowledgement within **3 business days**;
- an initial assessment within **7 days**;
- a coordinated disclosure date agreed with you - normally when a fixed release is
  available, and no later than 90 days after the report.

We credit reporters in the release notes unless you prefer otherwise.

## Scope

In scope: anything in this repository - the Rust core (`src/`), the Python package
(`python/nextreq/`) and the published wheels.

Typical examples: TLS verification bypasses, credential or cookie leakage across
origins (for example on redirects), request smuggling or header injection, memory
safety issues in the extension module, and parsers that can be driven to crash or
exhaust resources with crafted responses.

Out of scope: vulnerabilities in upstream dependencies that are not reachable
through nextreq (report those upstream), and issues that require `verify=False` or
other explicitly insecure configuration.

## Security-relevant behaviour

These are deliberate design decisions; if you find a way around any of them, that
is a vulnerability we want to hear about.

- **TLS is verified by default** using rustls with Mozilla's root set
  (`webpki-roots`) merged with the operating system trust store
  (`rustls-native-certs`). `verify=False` disables verification for that client
  only and should never be used in production.
- **Timeouts are on by default** (connect 5 s, read 30 s, write 30 s, pool 10 s),
  so a stalled peer cannot hang a caller forever.
- **Cross-origin redirects drop credentials.** The `Authorization` header and
  request-scoped cookies are removed when a redirect changes scheme, host or port
  (an `http` → `https` upgrade on the same host and default ports is allowed).
- **The cookie jar enforces scoping**: host-only cookies stay on their host, a
  server cannot set cookies for unrelated or dotless domains, `Secure` cookies are
  never sent over plain HTTP, and `Path` is matched on segment boundaries. It does
  not consult the Public Suffix List.
- **Only idempotent methods are retried by default**, so a retry policy cannot
  silently duplicate a `POST`.
- **Redirects to non-HTTP(S) schemes are refused**, and at most 10 redirects are
  followed.
- **Credentials in URLs** (`https://user:pass@host/`) are converted to a Basic
  `Authorization` header and stripped from the URL that is stored on the response
  and reported to OpenTelemetry.
- `Headers.__repr__` masks `Authorization`, `Proxy-Authorization`, `Cookie` and
  `Set-Cookie` values so they do not end up in logs by accident.
- Proxy configuration is read from `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` unless
  `trust_env=False` is passed.

## Dependencies

The Rust dependency tree is pinned in `Cargo.lock`. We recommend running
[`cargo audit`](https://github.com/rustsec/rustsec) and
[`cargo deny`](https://github.com/EmbarkStudios/cargo-deny) in downstream CI.
