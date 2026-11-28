"""Helpers for working with real APIs: ``Link`` headers and ``paginate()``,
JSON Lines bodies, and client-side rate limiting."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import threading
import time
from typing import Any

import pytest

import nextreq
from nextreq import MockTransport, RateLimit, Request, Response
from nextreq.models import parse_link_header

from .conftest import LiveServer

API = "https://api.example.com"

# -- Link headers -------------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ('<https://a.test/2>; rel="next"', [{"url": "https://a.test/2", "rel": "next"}]),
        (
            '<https://a.test/2>; rel="next", <https://a.test/9>; rel="last"',
            [
                {"url": "https://a.test/2", "rel": "next"},
                {"url": "https://a.test/9", "rel": "last"},
            ],
        ),
        # Unquoted values, odd spacing, and upper-case parameter names.
        (
            "  </items?page=2> ;REL=next ; type=text/html",
            [{"url": "/items?page=2", "rel": "next", "type": "text/html"}],
        ),
        # A comma and a semicolon inside a quoted string do not end the link.
        (
            '<https://a.test/x>; title="one, two; three"; rel="next"',
            [{"url": "https://a.test/x", "title": "one, two; three", "rel": "next"}],
        ),
        # Quoted pairs are unescaped.
        (
            '<https://a.test/x>; title="say \\"hi\\""',
            [{"url": "https://a.test/x", "title": 'say "hi"'}],
        ),
        # The first occurrence of a parameter wins.
        (
            '<https://a.test/x>; rel="next"; rel="prev"',
            [{"url": "https://a.test/x", "rel": "next"}],
        ),
        # A parameter without a value, and a link without parameters.
        (
            "<https://a.test/x>; crossorigin, <https://a.test/y>",
            [
                {"url": "https://a.test/x", "crossorigin": ""},
                {"url": "https://a.test/y"},
            ],
        ),
        # Garbage between links is skipped.
        ('nonsense, <https://a.test/2>; rel="next"', [{"url": "https://a.test/2", "rel": "next"}]),
        ("", []),
        ("not a link at all", []),
    ],
)
def test_parse_link_header(header: str, expected: list[dict[str, str]]) -> None:
    assert parse_link_header(header) == expected


def test_response_links_are_keyed_by_relation_and_resolved() -> None:
    response = Response(
        headers=[
            ("link", '</items?page=2>; rel="next", </items?page=1>; rel="first prev"'),
            ("link", "<https://cdn.example.com/style.css>; as=style"),
        ],
        request=Request("GET", f"{API}/items?page=1"),
    )
    links = response.links
    assert links["next"] == {"url": f"{API}/items?page=2", "rel": "next"}
    # One link with two relations is found under both.
    assert links["first"] is links["prev"]
    assert links["first"]["url"] == f"{API}/items?page=1"
    # No rel: keyed by its URL, as requests does.
    assert links["https://cdn.example.com/style.css"]["as"] == "style"


def test_response_without_link_header_has_no_links() -> None:
    assert Response().links == {}
    # Without a request there is nothing to resolve against.
    assert Response(headers={"link": '</next>; rel="next"'}).links["next"]["url"] == "/next"


# -- paginate() ---------------------------------------------------------------


def test_paginate_follows_link_headers(server: LiveServer, client: nextreq.Client) -> None:
    pages = list(client.paginate("/pages", params={"total": 4}))
    assert [page.json()["page"] for page in pages] == [1, 2, 3, 4]
    assert all(page.status_code == 200 for page in pages)
    assert server.request_count == 4


def test_paginate_is_lazy(server: LiveServer, client: nextreq.Client) -> None:
    pages = client.paginate("/pages", params={"total": 50})
    assert server.request_count == 0
    assert next(pages).json()["page"] == 1
    assert next(pages).json()["page"] == 2
    assert server.request_count == 2


def test_paginate_sends_headers_and_auth_with_every_page(server: LiveServer) -> None:
    with nextreq.Client(
        base_url=server.url, params={"api_key": "k"}, auth=("user", "pass"), trust_env=False
    ) as client:
        pages = [
            page.json()
            for page in client.paginate("/pages", params={"total": 3}, headers={"X-Test": "1"})
        ]
    assert [page["x_test"] for page in pages] == ["1", "1", "1"]
    assert all(page["authorization"].startswith("Basic ") for page in pages)
    # The client's default parameters are in the link already, and are not
    # added to it a second time.
    assert [page["query_string"] for page in pages] == [
        "api_key=k&total=3",
        "api_key=k&total=3&page=2",
        "api_key=k&total=3&page=3",
    ]


def test_paginate_resolves_relative_links(client: nextreq.Client) -> None:
    pages = list(client.paginate("/pages", params={"relative": 1}))
    assert [page.json()["page"] for page in pages] == [1, 2, 3]


def test_paginate_max_pages(server: LiveServer, client: nextreq.Client) -> None:
    pages = list(client.paginate("/pages", params={"total": 100}, max_pages=2))
    assert [page.json()["page"] for page in pages] == [1, 2]
    assert server.request_count == 2
    with pytest.raises(ValueError, match="max_pages"):
        list(client.paginate("/pages", max_pages=0))


def test_paginate_stops_when_a_page_links_to_itself(
    server: LiveServer, client: nextreq.Client
) -> None:
    pages = list(client.paginate("/pages", params={"page": 1, "loop": 1}))
    assert len(pages) == 1
    assert server.request_count == 1


def test_paginate_with_a_cursor_in_the_query(client: nextreq.Client) -> None:
    def next_page(page: Response) -> dict[str, str] | None:
        cursor = page.json()["next_cursor"]
        return {"cursor": cursor} if cursor else None

    pages = [
        page.json() for page in client.paginate("/cursor", params={"limit": 2}, next_page=next_page)
    ]
    assert [page["cursor"] for page in pages] == ["", "a", "b"]
    # The cursor replaces the previous one instead of piling up.
    assert [page["query_string"] for page in pages] == [
        "limit=2",
        "limit=2&cursor=a",
        "limit=2&cursor=b",
    ]


def test_paginate_with_a_url_or_a_request_from_next_page(
    server: LiveServer, client: nextreq.Client
) -> None:
    def by_url(page: Response) -> str | None:
        number = page.json()["page"]
        return f"/pages?page={number + 1}&total=3" if number < 3 else None

    assert [p.json()["page"] for p in client.paginate("/pages?total=3", next_page=by_url)] == [
        1,
        2,
        3,
    ]

    def by_request(page: Response) -> Request | None:
        body = page.json()
        if body["json"]["page"] >= 3:
            return None
        return client.build_request("POST", "/echo", json={"page": body["json"]["page"] + 1})

    posted = client.paginate("/echo", method="POST", json={"page": 1}, next_page=by_request)
    assert [page.json()["json"]["page"] for page in posted] == [1, 2, 3]


def test_paginate_applies_send_options_to_every_page() -> None:
    mock = MockTransport()
    mock.get(f"{API}/items").respond(json=[1], headers={"link": f'<{API}/more>; rel="next"'})
    mock.get(f"{API}/more").respond(500)
    with nextreq.Client(transport=mock) as client:
        pages = client.paginate(f"{API}/items", raise_for_status=True)
        assert next(pages).json() == [1]
        with pytest.raises(nextreq.HTTPStatusError):
            next(pages)


async def test_async_paginate(server: LiveServer, aclient: nextreq.AsyncClient) -> None:
    pages = [page.json()["page"] async for page in aclient.paginate("/pages", params={"total": 4})]
    assert pages == [1, 2, 3, 4]

    async def next_page(page: Response) -> dict[str, str] | None:
        await asyncio.sleep(0)
        cursor = page.json()["next_cursor"]
        return {"cursor": cursor} if cursor else None

    cursors = [p.json()["cursor"] async for p in aclient.paginate("/cursor", next_page=next_page)]
    assert cursors == ["", "a", "b"]

    limited = [p async for p in aclient.paginate("/pages", params={"total": 9}, max_pages=3)]
    assert len(limited) == 3


# -- JSON Lines ---------------------------------------------------------------

NDJSON = [
    {"id": 1, "name": "Ada"},
    {"id": 2, "name": "Gréte"},
    {"id": 3, "name": "a\u2028b"},
    {"id": 4, "name": "last"},
]


@dataclasses.dataclass
class Person:
    id: int
    name: str


def test_iter_json_lines_streamed(client: nextreq.Client) -> None:
    with client.stream("GET", "/ndjson") as response:
        assert list(response.iter_json_lines()) == NDJSON


def test_iter_json_lines_with_a_model(client: nextreq.Client) -> None:
    with client.stream("GET", "/ndjson") as response:
        people = list(response.iter_json_lines(model=Person))
    assert people == [Person(**item) for item in NDJSON]


def test_iter_json_lines_on_a_buffered_response() -> None:
    response = Response(content=b'{"a": 1}\n\n{"a": 2}\r\n[3]\n"four"')
    assert list(response.iter_json_lines()) == [{"a": 1}, {"a": 2}, [3], "four"]
    assert list(Response(content=b"").iter_json_lines()) == []


def test_iter_json_lines_reports_a_bad_line() -> None:
    response = Response(content=b'{"id": 1, "name": "ok"}\n{"id": "x"}\n')
    lines = response.iter_json_lines(model=Person)
    assert next(lines) == Person(1, "ok")
    with pytest.raises(nextreq.DecodeError):
        next(lines)
    with pytest.raises(json.JSONDecodeError):
        list(Response(content=b"{not json}\n").iter_json_lines())


async def test_aiter_json_lines(aclient: nextreq.AsyncClient) -> None:
    async with aclient.stream("GET", "/ndjson") as response:
        assert [item async for item in response.aiter_json_lines()] == NDJSON
    async with aclient.stream("GET", "/ndjson") as response:
        people = [person async for person in response.aiter_json_lines(model=Person)]
    assert people[1] == Person(2, "Gréte")


# -- rate limiting ------------------------------------------------------------


def _mock() -> MockTransport:
    mock = MockTransport()
    mock.get(f"{API}/ok").respond(json={"ok": True})
    return mock


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        ((0,), {}),
        ((-1,), {}),
        ((float("inf"),), {}),
        ((float("nan"),), {}),
        ((True,), {}),
        ((10,), {"per": 0}),
        ((10,), {"per": float("inf")}),
        ((10,), {"burst": 0}),
        ((10,), {"burst": 1.5}),
        ((10,), {"burst": True}),
    ],
)
def test_rate_limit_rejects_bad_values(args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        RateLimit(*args, **kwargs)


def test_rate_limit_option_types() -> None:
    assert nextreq.Client(transport=_mock()).rate_limit is None
    limit = nextreq.Client(transport=_mock(), rate_limit=20).rate_limit
    assert isinstance(limit, RateLimit)
    assert (limit.rate, limit.per, limit.burst) == (20, 1.0, 1)
    shared = RateLimit(600, per=60, burst=5)
    assert nextreq.AsyncClient(transport=_mock(), rate_limit=shared).rate_limit is shared
    assert repr(shared) == "RateLimit(600, per=60, burst=5)"
    assert repr(RateLimit(2.5)) == "RateLimit(2.5, per=1)"
    for bad in ("10", True, [10]):
        with pytest.raises(TypeError, match="rate_limit"):
            nextreq.Client(transport=_mock(), rate_limit=bad)  # type: ignore[arg-type]


def test_rate_limit_reserve_spaces_slots_evenly() -> None:
    limit = RateLimit(10)  # one slot every 100 ms
    waits = [limit.reserve() for _ in range(4)]
    assert waits[0] == 0.0
    for index, wait in enumerate(waits[1:], start=1):
        assert wait == pytest.approx(index * 0.1, abs=0.02)


def test_rate_limit_burst_lets_requests_through_after_an_idle_period() -> None:
    limit = RateLimit(10, burst=3)
    waits = [limit.reserve() for _ in range(5)]
    assert waits[:3] == [0.0, 0.0, 0.0]
    assert waits[3] == pytest.approx(0.1, abs=0.02)
    assert waits[4] == pytest.approx(0.2, abs=0.02)


def test_rate_limit_refills_while_idle() -> None:
    limit = RateLimit(20, burst=2)  # 50 ms apart
    assert [limit.reserve(), limit.reserve()] == [0.0, 0.0]
    time.sleep(0.12)  # long enough for both slots to come back
    assert [limit.reserve(), limit.reserve()] == [0.0, 0.0]
    assert limit.reserve() > 0


def test_sync_client_observes_the_rate_limit() -> None:
    with nextreq.Client(transport=_mock(), rate_limit=50) as client:  # 20 ms apart
        started = time.perf_counter()
        for _ in range(6):
            client.get(f"{API}/ok")
        elapsed = time.perf_counter() - started
    assert 0.095 <= elapsed < 1.0


async def test_async_client_observes_the_rate_limit_under_gather() -> None:
    mock = _mock()
    async with nextreq.AsyncClient(transport=mock, rate_limit=50) as client:
        started = time.perf_counter()
        responses = await client.gather([f"{API}/ok"] * 6, max_concurrency=6)
        elapsed = time.perf_counter() - started
    assert len(responses) == 6
    assert 0.095 <= elapsed < 1.0


def test_one_rate_limit_is_shared_between_clients_and_threads() -> None:
    limit = RateLimit(50)
    stamps: list[float] = []
    lock = threading.Lock()

    def worker() -> None:
        with nextreq.Client(transport=_mock(), rate_limit=limit) as client:
            for _ in range(3):
                client.get(f"{API}/ok")
                with lock:
                    stamps.append(time.perf_counter())

    threads = [threading.Thread(target=worker) for _ in range(3)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # Nine requests, 20 ms apart, no matter which client or thread sent them.
    assert len(stamps) == 9
    assert max(stamps) - started >= 8 * 0.02 - 0.005


def test_retries_and_redirects_count_against_the_rate_limit(server: LiveServer) -> None:
    with nextreq.Client(base_url=server.url, rate_limit=25, trust_env=False) as client:
        started = time.perf_counter()
        response = client.get("/redirect/3")  # four requests reach the server
        elapsed = time.perf_counter() - started
    assert response.status_code == 200
    assert len(response.history) == 3
    assert elapsed >= 3 * 0.04 - 0.005


def test_no_rate_limit_adds_no_delay() -> None:
    with nextreq.Client(transport=_mock()) as client:
        started = time.perf_counter()
        for _ in range(200):
            client.get(f"{API}/ok")
        assert time.perf_counter() - started < 1.0
