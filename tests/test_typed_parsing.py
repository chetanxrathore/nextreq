"""NXRQ-402: typed JSON parsing into Pydantic, dataclasses and msgspec."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import uuid
from typing import Any, Literal, Optional, TypedDict, Union

import pydantic
import pytest

import nextreq
from nextreq import DecodeError, Response
from nextreq.parsing import parse_json

msgspec = pytest.importorskip("msgspec")

# -- models -------------------------------------------------------------------


class Address(pydantic.BaseModel):
    city: str
    zip: str


class User(pydantic.BaseModel):
    id: int
    name: str
    email: str
    active: bool
    tags: list[str] = []
    address: Address
    created_at: datetime.datetime


@dataclasses.dataclass
class AddressDC:
    city: str
    zip: str


@dataclasses.dataclass
class UserDC:
    id: int
    name: str
    email: str
    active: bool
    address: AddressDC
    created_at: datetime.datetime
    tags: list[str] = dataclasses.field(default_factory=list)
    nickname: Optional[str] = None  # noqa: UP045 - exercise typing.Optional


class AddressStruct(msgspec.Struct):
    city: str
    zip: str


class UserStruct(msgspec.Struct):
    id: int
    name: str
    email: str
    active: bool
    address: AddressStruct
    created_at: datetime.datetime
    tags: list[str] = []  # noqa: RUF012 - msgspec copies mutable defaults


# -- Pydantic -----------------------------------------------------------------


def test_response_json_into_pydantic_model(client: nextreq.Client) -> None:
    user = client.get("/users/7").json(model=User)
    assert isinstance(user, User)
    assert user.id == 7 and user.name == "User 7" and user.active is False
    assert user.address == Address(city="Paris", zip="75001")
    assert user.created_at == datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


def test_client_get_with_model(client: nextreq.Client) -> None:
    user = client.get("/users/2", model=User)
    assert isinstance(user, User) and user.id == 2 and user.active is True
    users = client.get("/users", params={"count": 4}, model=list[User])
    assert [u.id for u in users] == [1, 2, 3, 4] and all(isinstance(u, User) for u in users)
    assert nextreq.get(f"{client.base_url}/users/3", model=User).id == 3
    assert client.request("GET", "/users/5", model=User).id == 5


async def test_async_client_get_with_model(aclient: nextreq.AsyncClient) -> None:
    user = await aclient.get("/users/9", model=User)
    assert isinstance(user, User) and user.id == 9
    users = await aclient.get("/users", model=list[User])
    assert [u.id for u in users] == [1, 2, 3]
    posted = await aclient.post("/echo", json={"k": 1}, model=dict[str, Any])
    assert posted["json"] == {"k": 1}


def test_pydantic_validation_failure_is_a_structured_decode_error(client: nextreq.Client) -> None:
    with pytest.raises(DecodeError) as info:
        client.get("/users/invalid", model=User)
    error = info.value
    assert error.status_code == 200
    assert error.url is not None and error.url.endswith("/users/invalid")
    assert error.model is User
    assert error.response is not None and error.response.json()["name"] == 5
    assert error.request is not None and error.request.method == "GET"
    locations = {tuple(e["loc"]) for e in error.errors}
    assert ("id",) in locations and ("name",) in locations and ("email",) in locations
    assert all({"loc", "msg", "type"} <= set(e) for e in error.errors)
    message = str(error)
    assert "User" in message and "status 200" in message and "/users/invalid" in message
    assert isinstance(error.__cause__, pydantic.ValidationError)
    assert isinstance(error, nextreq.NextReqError) and isinstance(error, ValueError)


def test_invalid_json_is_a_decode_error_with_context(client: nextreq.Client) -> None:
    for model in (User, UserDC, UserStruct, list[User]):
        with pytest.raises(DecodeError) as info:
            client.get("/users/notjson", raise_for_status=False, model=model)
        assert info.value.status_code == 200
        assert info.value.url is not None and "/users/notjson" in info.value.url
        assert info.value.errors, "every decode error carries at least one structured entry"


def test_http_errors_take_precedence_over_parsing(client: nextreq.Client) -> None:
    """An error body should not be force-fed into the model."""
    with pytest.raises(nextreq.HTTPStatusError) as info:
        client.get("/users/missing", model=User)
    assert info.value.status_code == 404
    # ...unless the caller explicitly opts out, in which case it is a DecodeError.
    with pytest.raises(DecodeError) as decode:
        client.get("/users/missing", model=User, raise_for_status=False)
    assert decode.value.status_code == 404


# -- dataclasses --------------------------------------------------------------


def test_dataclass_parsing(client: nextreq.Client) -> None:
    user = client.get("/users/4", model=UserDC)
    assert isinstance(user, UserDC)
    assert user.id == 4 and user.tags == ["a", "b"] and user.nickname is None
    assert isinstance(user.address, AddressDC) and user.address.city == "Paris"
    assert user.created_at.year == 2026 and user.created_at.tzinfo is not None
    users = client.get("/users", model=list[UserDC])
    assert [u.name for u in users] == ["User 1", "User 2", "User 3"]


def test_dataclass_errors_are_collected_with_paths(client: nextreq.Client) -> None:
    with pytest.raises(DecodeError) as info:
        client.get("/users/invalid", model=UserDC)
    errors = {tuple(e["loc"]): e for e in info.value.errors}
    assert errors[("id",)]["msg"] == "expected an integer, got a string"
    assert errors[("name",)]["msg"] == "expected a string, got a number"
    assert errors[("email",)]["type"] == "missing"
    assert errors[("address",)]["type"] == "missing"
    assert "id: expected an integer" in str(info.value)
    assert info.value.model is UserDC


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Meta(TypedDict):
    version: int


def test_builtin_converter_types() -> None:
    @dataclasses.dataclass
    class Everything:
        flag: bool
        ratio: float
        color: Color
        mode: Literal["fast", "slow"]
        ident: uuid.UUID
        price: decimal.Decimal
        day: datetime.date
        maybe: Optional[int]  # noqa: UP045
        either: Union[int, str]
        modern: int | None
        pair: tuple[int, str]
        many: tuple[int, ...]
        unique: set[str]
        scores: dict[str, float]
        by_id: dict[int, AddressDC]
        nested: list[list[int]]
        meta: Meta
        anything: Any
        defaulted: int = 7

    payload = (
        b'{"flag": true, "ratio": 3, "color": "blue", "mode": "fast",'
        b' "ident": "12345678-1234-5678-1234-567812345678", "price": "9.99",'
        b' "day": "2026-11-20", "maybe": null, "either": "text", "modern": 5,'
        b' "pair": [1, "x"], "many": [1, 2, 3], "unique": ["a", "a", "b"],'
        b' "scores": {"x": 1, "y": 2.5}, "by_id": {"7": {"city": "Rome", "zip": "00100"}},'
        b' "nested": [[1], [2, 3]], "meta": {"version": 2}, "anything": {"k": [1]},'
        b' "ignored_extra_key": true}'
    )
    obj = parse_json(payload, Everything)
    assert obj.flag is True and obj.ratio == 3.0 and isinstance(obj.ratio, float)
    assert obj.color is Color.BLUE and obj.mode == "fast"
    assert obj.ident == uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert obj.price == decimal.Decimal("9.99") and obj.day == datetime.date(2026, 11, 20)
    assert obj.maybe is None and obj.either == "text" and obj.modern == 5
    assert obj.pair == (1, "x") and obj.many == (1, 2, 3) and obj.unique == {"a", "b"}
    assert obj.scores == {"x": 1.0, "y": 2.5}
    assert obj.by_id == {7: AddressDC("Rome", "00100")}
    assert obj.nested == [[1], [2, 3]] and obj.meta == {"version": 2}
    assert obj.anything == {"k": [1]} and obj.defaulted == 7


@pytest.mark.parametrize(
    ("model", "payload", "fragment"),
    [
        (int, b'"5"', "expected an integer"),
        (int, b"true", "expected an integer"),
        (str, b"5", "expected a string"),
        (bool, b"1", "expected a boolean"),
        (float, b'"x"', "expected a number"),
        (list[int], b'{"a": 1}', "expected an array"),
        (list[int], b'[1, "two", 3]', "1: expected an integer"),
        (dict[str, int], b"[1]", "expected an object"),
        (dict[int, str], b'{"x": "y"}', "invalid int key"),
        (Literal["a", "b"], b'"c"', "expected one of"),
        (Optional[int], b'"x"', "expected an integer"),  # noqa: UP045
        (Union[int, bool], b'"x"', "does not match any of"),
        (tuple[int, int], b"[1]", "expected an array of 2 items"),
        (uuid.UUID, b'"nope"', "invalid UUID"),
        (datetime.datetime, b'"yesterday"', "invalid datetime"),
        (AddressDC, b"[]", "expected a JSON object for AddressDC"),
        (AddressDC, b'{"city": "x"}', "zip: field required"),
        (list[AddressDC], b'[{"city": "x", "zip": 1}]', "0.zip: expected a string"),
    ],
)
def test_builtin_converter_rejects_mismatches(model: Any, payload: bytes, fragment: str) -> None:
    with pytest.raises(DecodeError, match=fragment):
        parse_json(payload, model)


def test_builtin_converter_scalars_and_passthrough() -> None:
    assert parse_json(b"5", int) == 5 and parse_json(b"5.0", int) == 5
    assert parse_json(b'"x"', str) == "x" and parse_json(b"null", type(None)) is None
    assert parse_json(b'{"a": [1, {"b": null}]}', Any) == {"a": [1, {"b": None}]}  # type: ignore[arg-type]
    assert parse_json(b"[1, 2]", list) == [1, 2] and parse_json(b'{"a": 1}', dict) == {"a": 1}
    assert parse_json(b'"2026-01-02T03:04:05Z"', datetime.datetime).tzinfo is not None
    assert parse_json(b"1700000000", datetime.datetime).year == 2023


def test_locally_defined_types_degrade_gracefully() -> None:
    """With postponed annotations, a function-local class cannot be resolved by
    name. Fields that *can* be resolved must still be validated."""

    class Local(enum.Enum):
        A = "a"

    @dataclasses.dataclass
    class Holder:
        count: int
        local: Local

    holder = parse_json(b'{"count": 2, "local": "a"}', Holder)
    assert holder.count == 2
    assert holder.local == "a", "unresolvable annotation: value is passed through"
    with pytest.raises(DecodeError, match="count: expected an integer"):
        parse_json(b'{"count": "two", "local": "a"}', Holder)


def test_dataclass_post_init_errors_are_wrapped() -> None:
    @dataclasses.dataclass
    class Positive:
        value: int

        def __post_init__(self) -> None:
            if self.value <= 0:
                raise ValueError("value must be positive")

    assert parse_json(b'{"value": 3}', Positive).value == 3
    with pytest.raises(DecodeError, match="value must be positive") as info:
        parse_json(b'{"value": -1}', Positive)
    assert info.value.status_code is None and info.value.url is None


# -- msgspec ------------------------------------------------------------------


def test_msgspec_struct_parsing(client: nextreq.Client) -> None:
    user = client.get("/users/6", model=UserStruct)
    assert isinstance(user, UserStruct)
    assert user.id == 6 and user.address == AddressStruct(city="Paris", zip="75001")
    assert user.created_at.year == 2026
    users = client.get("/users", model=list[UserStruct])
    assert [u.id for u in users] == [1, 2, 3]


def test_msgspec_validation_failure(client: nextreq.Client) -> None:
    with pytest.raises(DecodeError) as info:
        client.get("/users/invalid", model=UserStruct)
    assert info.value.status_code == 200 and info.value.model is UserStruct
    assert info.value.errors and "id" in info.value.errors[0]["msg"]
    assert isinstance(info.value.__cause__, msgspec.ValidationError)


# -- mixing libraries and request bodies ---------------------------------------


def test_generic_containers_of_library_models() -> None:
    payload = b'{"p": {"city": "Oslo", "zip": "0150"}}'
    assert parse_json(payload, dict[str, Address]) == {"p": Address(city="Oslo", zip="0150")}
    assert parse_json(payload, dict[str, AddressStruct]) == {
        "p": AddressStruct(city="Oslo", zip="0150")
    }
    assert parse_json(payload, dict[str, AddressDC]) == {"p": AddressDC("Oslo", "0150")}
    with pytest.raises(DecodeError) as info:
        parse_json(b'{"p": {"city": "Oslo"}}', dict[str, Address])
    assert tuple(info.value.errors[0]["loc"]) == ("p", "zip")


def test_models_can_be_sent_as_json_bodies(client: nextreq.Client) -> None:
    address = {"city": "Paris", "zip": "75001"}
    assert client.post("/echo", json=Address(**address)).json()["json"] == address
    assert client.post("/echo", json=AddressDC(**address)).json()["json"] == address
    assert client.post("/echo", json=AddressStruct(**address)).json()["json"] == address
    mixed = {
        "when": datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc),
        "id": uuid.UUID(int=1),
        "tags": {"only"},
        "nested": [AddressDC(**address)],
    }
    echoed = client.post("/echo", json=mixed).json()["json"]
    assert echoed == {
        "when": "2026-01-02T00:00:00+00:00",
        "id": "00000000-0000-0000-0000-000000000001",
        "tags": ["only"],
        "nested": [address],
    }
    with pytest.raises(TypeError, match="not JSON serializable"):
        client.post("/echo", json={"bad": object()})


def test_round_trip_post_and_parse(client: nextreq.Client) -> None:
    class Echo(pydantic.BaseModel):
        method: str
        json_: Address = pydantic.Field(alias="json")

    echo = client.post("/echo", json=Address(city="Lima", zip="15001"), model=Echo)
    assert echo.method == "POST" and echo.json_.city == "Lima"


def test_plain_json_still_works(client: nextreq.Client) -> None:
    response: Response = client.get("/users/1")
    assert response.json()["id"] == 1
    assert response.json(parse_float=decimal.Decimal)["id"] == 1
    with pytest.raises(ValueError):
        client.get("/users/notjson").json()
