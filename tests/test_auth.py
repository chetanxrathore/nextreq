"""Authentication: response-aware auth flows and HTTP Digest."""

from __future__ import annotations

from collections.abc import Generator, Iterator
from typing import Any

import pytest

import nextreq
from nextreq import DigestAuth, Request, Response
from nextreq.client import _parse_digest_challenge

from .conftest import LiveServer

# -- Digest: the algorithm itself ----------------------------------------------


@pytest.mark.parametrize(
    ("algorithm", "expected"),
    [
        ("MD5", "8ca523f5e9506fed4657c9700eebdbec"),
        ("SHA-256", "753927fa0e85d155564e2e272a28d1802ca10daf4496794697cf8db5856cb6c1"),
    ],
)
def test_digest_matches_the_rfc_7616_examples(
    algorithm: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RFC 7616, section 3.9.1."""
    auth = DigestAuth("Mufasa", "Circle of Life")
    monkeypatch.setattr(auth, "_cnonce", lambda: "f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ")
    challenge = {
        "realm": "http-auth@example.org",
        "qop": "auth, auth-int",
        "algorithm": algorithm,
        "nonce": "7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v",
        "opaque": "FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS",
    }
    header = auth._authorization(Request("GET", "http://www.example.org/dir/index.html"), challenge)
    assert header.startswith("Digest ")
    assert f'response="{expected}"' in header
    assert 'username="Mufasa"' in header
    assert 'uri="/dir/index.html"' in header
    assert "qop=auth, nc=00000001" in header
    assert f"algorithm={algorithm}" in header
    assert 'opaque="FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"' in header


def test_digest_challenge_parsing() -> None:
    parsed = _parse_digest_challenge(
        'Basic realm="other, with comma", '
        'Digest realm="a \\"quoted\\" realm", nonce="abc", qop="auth,auth-int", algorithm=SHA-256, '
        "stale=TRUE, Negotiate"
    )
    assert parsed == {
        "realm": 'a "quoted" realm',
        "nonce": "abc",
        "qop": "auth,auth-int",
        "algorithm": "SHA-256",
        "stale": "TRUE",
    }
    assert _parse_digest_challenge('Basic realm="only basic"') is None
    assert _parse_digest_challenge('Digest realm="no nonce"') is None


# -- Digest: against a server ---------------------------------------------------


@pytest.mark.parametrize(
    "algorithm", ["MD5", "MD5-sess", "SHA", "SHA-256", "SHA-256-sess", "SHA-512"]
)
def test_digest_auth_algorithms(client: nextreq.Client, server: LiveServer, algorithm: str) -> None:
    auth = DigestAuth("ada", "s3cr3t pässword")
    response = client.get(
        "/digest-auth/ada/s3cr3t pässword", params={"algorithm": algorithm}, auth=auth
    )
    assert response.status_code == 200
    assert response.json() == {
        "authenticated": True,
        "nc": "00000001",
        "qop": "auth",
        "body": "",
        "user": "ada",
    }
    assert server.digest_requests == 2  # the challenge, then the answer


def test_digest_auth_remembers_the_challenge(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, auth=DigestAuth("ada", "pw")) as client:
        assert client.get("/digest-auth/ada/pw").json()["nc"] == "00000001"
        assert server.digest_requests == 2
        # Later requests answer the remembered challenge straight away, with a
        # fresh nonce count each time.
        assert client.get("/digest-auth/ada/pw").json()["nc"] == "00000002"
        assert client.get("/digest-auth/ada/pw", params={"x": "1"}).json()["nc"] == "00000003"
        assert server.digest_requests == 4


def test_digest_auth_resends_the_body(client: nextreq.Client) -> None:
    response = client.post("/digest-auth/ada/pw", auth=DigestAuth("ada", "pw"), json={"n": 1})
    assert response.json()["body"] == '{"n":1}'

    def once() -> Iterator[bytes]:
        yield b"cannot be sent twice"

    with pytest.raises(nextreq.StreamConsumed, match="one-shot stream"):
        client.post("/digest-auth/ada/pw", auth=DigestAuth("ada", "pw"), content=once())


def test_digest_auth_with_wrong_credentials_returns_the_401(
    client: nextreq.Client, server: LiveServer
) -> None:
    response = client.get("/digest-auth/ada/pw", auth=DigestAuth("ada", "wrong"))
    assert response.status_code == 401
    assert server.digest_requests == 2  # one attempt, no loop

    # No Digest challenge at all: the 401 is simply returned.
    response = client.get("/basic-auth/ada/pw", auth=DigestAuth("ada", "pw"))
    assert response.status_code == 401


def test_digest_auth_variants(client: nextreq.Client, server: LiveServer) -> None:
    # RFC 2069 servers send no qop.
    assert client.get(
        "/digest-auth/ada/pw", params={"qop": ""}, auth=DigestAuth("ada", "pw")
    ).json()["authenticated"]
    # Digest offered next to another scheme in one header.
    assert client.get(
        "/digest-auth/ada/pw", params={"extra": 1}, auth=DigestAuth("ada", "pw")
    ).json()["authenticated"]
    # Unsupported protection level / algorithm are reported, not silently skipped.
    with pytest.raises(nextreq.LocalProtocolError, match="quality of protection"):
        client.get("/digest-auth/ada/pw", params={"qop": "auth-conf"}, auth=DigestAuth("ada", "pw"))
    with pytest.raises(nextreq.LocalProtocolError, match="algorithm"):
        client.get(
            "/digest-auth/ada/pw", params={"algorithm": "WHIRLPOOL"}, auth=DigestAuth("a", "p")
        )


def test_digest_auth_recovers_from_a_stale_nonce(server: LiveServer) -> None:
    """With ?expire=1 the server accepts each nonce once, then reports it stale."""
    with nextreq.Client(base_url=server.url, auth=DigestAuth("ada", "pw")) as client:
        for _ in range(3):
            response = client.get("/digest-auth/ada/pw", params={"expire": 1})
            assert response.status_code == 200
            assert response.json()["nc"] == "00000001"  # always a new nonce


async def test_digest_auth_with_the_async_client(
    aclient: nextreq.AsyncClient, server: LiveServer
) -> None:
    auth = DigestAuth("ada", "pw")
    response = await aclient.get("/digest-auth/ada/pw", params={"algorithm": "SHA-256"}, auth=auth)
    assert response.json()["authenticated"] is True
    async with aclient.stream(
        "GET", "/digest-auth/ada/pw", params={"algorithm": "SHA-256"}, auth=auth
    ) as streamed:
        assert streamed.status_code == 200
        await streamed.aread()
        assert streamed.json()["nc"] == "00000002"


def test_digest_auth_survives_same_origin_redirects_only(
    client: nextreq.Client, server: LiveServer
) -> None:
    auth = DigestAuth("ada", "pw")
    response = client.get("/redirect-to", params={"url": "/digest-auth/ada/pw"}, auth=auth)
    assert response.status_code == 200 and response.json()["authenticated"] is True

    # Another origin (same server, different host name) must not get the credentials.
    elsewhere = f"http://localhost:{server.port}/digest-auth/ada/pw"
    response = client.get("/redirect-to", params={"url": elsewhere}, auth=auth)
    assert response.status_code == 401


# -- custom flows ----------------------------------------------------------------


class RefreshingToken(nextreq.Auth):
    """Fetches a new token from another endpoint when the current one is refused."""

    requires_response_body = True

    def __init__(self) -> None:
        self.token = "stale"
        self.seen: list[Any] = []

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        request.headers["authorization"] = f"Bearer {self.token}"
        response = yield request
        if response.status_code != 401:
            return
        self.seen.append(response.json())  # allowed: requires_response_body
        refreshed = yield Request("POST", "https://auth.test/token", json={"grant": "refresh"})
        self.token = refreshed.json()["token"]
        request.headers["authorization"] = f"Bearer {self.token}"
        yield request


def _token_api() -> nextreq.MockTransport:
    mock = nextreq.MockTransport()

    def items(request: Request) -> Response:
        if request.headers.get("authorization") == "Bearer fresh":
            return Response(200, json={"items": [1, 2]})
        return Response(401, json={"error": "expired"})

    mock.get("https://api.test/items").reply(items)
    mock.post("https://auth.test/token").respond(json={"token": "fresh"})
    return mock


def test_custom_auth_flow_can_refresh_a_token() -> None:
    mock = _token_api()
    auth = RefreshingToken()
    with nextreq.Client(transport=mock, auth=auth, headers={"X-App": "demo"}) as client:
        response = client.get("https://api.test/items")
        assert response.json() == {"items": [1, 2]}
        assert auth.seen == [{"error": "expired"}]
        sent = [(call.request.method, str(call.request.url)) for call in mock.calls]
        assert sent == [
            ("GET", "https://api.test/items"),
            ("POST", "https://auth.test/token"),
            ("GET", "https://api.test/items"),
        ]
        # A request yielded by the flow gets the client's defaults, but is not
        # itself authenticated again.
        token_request = mock.calls[1].request
        assert token_request.headers["x-app"] == "demo"
        assert "authorization" not in token_request.headers

        # The refreshed token is used from now on: one exchange.
        mock.reset()
        assert client.get("https://api.test/items").status_code == 200
        assert len(mock.calls) == 1


async def test_custom_auth_flow_with_the_async_client_and_streaming() -> None:
    mock = _token_api()
    auth = RefreshingToken()
    async with nextreq.AsyncClient(transport=mock) as client:
        async with client.stream("GET", "https://api.test/items", auth=auth) as response:
            assert response.status_code == 200
        assert auth.seen == [{"error": "expired"}]
        assert auth.token == "fresh"


def test_plain_callables_and_simple_auth_classes_still_work(client: nextreq.Client) -> None:
    def stamp(request: Request) -> None:
        request.headers["x-signature"] = "signed"

    assert client.get("/echo", auth=stamp).json()["headers"]["x-signature"] == "signed"
    assert client.get("/basic-auth/u/p", auth=nextreq.BasicAuth("u", "p")).status_code == 200
    assert client.get("/basic-auth/u/p", auth=("u", "p")).status_code == 200
    # An explicit Authorization header wins over a client-level flow.
    with nextreq.Client(transport=_token_api(), auth=RefreshingToken()) as mocked:
        request = Request(
            "GET", "https://api.test/items", headers={"Authorization": "Bearer fresh"}
        )
        assert mocked.send(request).status_code == 200


def test_digest_auth_int_signs_the_body(client: nextreq.Client, tmp_path: Any) -> None:
    only_int = {"qop": "auth-int"}
    auth = DigestAuth("ada", "pw")
    seen = client.post("/digest-auth/ada/pw", params=only_int, content=b"signed body", auth=auth)
    assert seen.json() == {
        "authenticated": True,
        "nc": "00000001",
        "qop": "auth-int",
        "body": "signed body",
        "user": "ada",
    }
    # No body at all, and SHA-256.
    assert (
        client.get(
            "/digest-auth/ada/pw", params={**only_int, "algorithm": "SHA-256"}, auth=auth
        ).json()["qop"]
        == "auth-int"
    )
    # When the server offers both, the cheaper "auth" is used.
    assert (
        client.post(
            "/digest-auth/ada/pw", params={"qop": "auth,auth-int"}, content=b"x", auth=auth
        ).json()["qop"]
        == "auth"
    )

    # Streamed bodies are hashed first and then replayed: a file ...
    path = tmp_path / "payload.bin"
    path.write_bytes(b"file body " * 20_000)
    with path.open("rb") as handle:
        seen = client.put("/digest-auth/ada/pw", params=only_int, content=handle, auth=auth)
    assert seen.json()["qop"] == "auth-int" and len(seen.json()["body"]) == 200_000

    class Reiterable:
        def __iter__(self) -> Any:
            yield b"part one, "
            yield b"part two"

    seen = client.put("/digest-auth/ada/pw", params=only_int, content=Reiterable(), auth=auth)
    assert seen.json()["body"] == "part one, part two"

    # ... but a body that can only be produced once cannot be signed in advance.
    with pytest.raises(nextreq.LocalProtocolError, match="auth-int signs the request body"):
        client.put("/digest-auth/ada/pw", params=only_int, content=iter([b"once"]), auth=auth)


async def test_digest_auth_int_with_the_async_client(aclient: nextreq.AsyncClient) -> None:
    auth = DigestAuth("ada", "pw")
    seen = await aclient.post(
        "/digest-auth/ada/pw", params={"qop": "auth-int"}, json={"n": 1}, auth=auth
    )
    assert seen.json()["qop"] == "auth-int" and seen.json()["body"] == '{"n":1}'

    async def chunks() -> Any:
        yield b"async"

    with pytest.raises(nextreq.LocalProtocolError, match="auth-int signs the request body"):
        await aclient.post(
            "/digest-auth/ada/pw", params={"qop": "auth-int"}, content=chunks(), auth=auth
        )
