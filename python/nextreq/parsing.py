"""Typed JSON parsing for ``response.json(model=...)`` and ``client.get(url, model=...)``.

Supported targets, with no mandatory dependencies:

* **Pydantic v2** models - validated straight from bytes by ``pydantic-core``
  (``model_validate_json``), which is faster than ``json.loads`` + ``model_validate``.
* **msgspec** ``Struct`` types - decoded with a cached ``msgspec.json.Decoder``.
* **dataclasses** - built-in strict converter (nested dataclasses, containers,
  ``Optional`` / ``Union``, ``Literal``, enums, datetimes, UUIDs, ``Decimal``).
* **generics** such as ``list[User]`` or ``dict[str, User]`` wrapping any of the above.

Pydantic and msgspec are detected by duck typing, so neither is ever imported
unless the caller hands us one of their types.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import importlib
import json
import sys
import types
import typing
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar, Union

from .exceptions import DecodeError

if TYPE_CHECKING:
    from .models import Response

__all__ = ["parse_json", "parse_response"]

T = TypeVar("T")

_Decoder = Callable[[bytes], Any]
_DECODERS: dict[Any, _Decoder] = {}
_NONE_TYPE = type(None)


class _ConversionError(Exception):
    """Raised by the built-in converter; carries pydantic-style error dicts."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        super().__init__(errors[0]["msg"] if errors else "conversion failed")
        self.errors = errors


def _error(path: tuple[Any, ...], message: str, kind: str = "type_error") -> _ConversionError:
    return _ConversionError([{"loc": path, "msg": message, "type": kind}])


def _type_name(tp: Any) -> str:
    if typing.get_origin(tp) is not None:
        # Generic aliases: show the parameters too, e.g. ``list[User]``.
        args = ", ".join(_type_name(arg) for arg in typing.get_args(tp))
        origin = getattr(typing.get_origin(tp), "__name__", None) or str(typing.get_origin(tp))
        return f"{origin}[{args}]" if args else origin
    if tp is Ellipsis:
        return "..."
    return getattr(tp, "__name__", None) or str(tp).replace("typing.", "")


def _is_pydantic_model(tp: Any) -> bool:
    return isinstance(tp, type) and callable(getattr(tp, "model_validate_json", None))


def _is_msgspec_struct(tp: Any) -> bool:
    return isinstance(tp, type) and hasattr(tp, "__struct_fields__")


# -- built-in converter -------------------------------------------------------

_FIELD_CACHE: dict[type, list[tuple[str, Any, bool]]] = {}


def _resolve_hints_individually(cls: type) -> dict[str, Any]:
    """Fallback for when ``get_type_hints`` fails as a whole.

    That happens when *one* annotation cannot be resolved - typically a class
    defined inside a function while ``from __future__ import annotations`` is
    active. Resolve what we can field by field; only the unresolvable fields
    fall back to ``Any`` (i.e. are passed through unvalidated).
    """
    module = sys.modules.get(cls.__module__)
    namespace: dict[str, Any] = dict(vars(typing))
    if module is not None:
        namespace.update(vars(module))
    namespace[cls.__name__] = cls
    hints: dict[str, Any] = {}
    for field in dataclasses.fields(cls):
        annotation: Any = field.type
        if isinstance(annotation, str):
            try:
                # The string is source code from the model's own module (the
                # same thing typing.get_type_hints evaluates), never user data.
                annotation = eval(annotation, namespace)
            except Exception:
                annotation = Any
        hints[field.name] = annotation
    return hints


def _dataclass_fields(cls: type) -> list[tuple[str, Any, bool]]:
    """``(name, resolved_type, required)`` for each init field of a dataclass."""
    cached = _FIELD_CACHE.get(cls)
    if cached is None:
        try:
            hints = typing.get_type_hints(cls)
        except Exception:
            hints = _resolve_hints_individually(cls)
        cached = []
        for field in dataclasses.fields(cls):
            if not field.init:
                continue
            required = (
                field.default is dataclasses.MISSING
                and field.default_factory is dataclasses.MISSING
            )
            cached.append((field.name, hints.get(field.name, Any), required))
        _FIELD_CACHE[cls] = cached
    return cached


def _convert_dataclass(value: Any, cls: type, path: tuple[Any, ...]) -> Any:
    if not isinstance(value, dict):
        raise _error(path, f"expected a JSON object for {cls.__name__}, got {_json_kind(value)}")
    kwargs: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    for name, tp, required in _dataclass_fields(cls):
        if name in value:
            try:
                kwargs[name] = _convert(value[name], tp, (*path, name))
            except _ConversionError as exc:
                errors.extend(exc.errors)
        elif required:
            errors.append({"loc": (*path, name), "msg": "field required", "type": "missing"})
    if errors:
        raise _ConversionError(errors)
    try:
        return cls(**kwargs)
    except Exception as exc:  # __post_init__ validation and the like
        raise _error(path, f"could not construct {cls.__name__}: {exc}", "value_error") from exc


def _json_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, str):
        return "a string"
    if isinstance(value, list):
        return "an array"
    if isinstance(value, dict):
        return "an object"
    return type(value).__name__


def _convert(value: Any, tp: Any, path: tuple[Any, ...]) -> Any:
    """Convert a decoded JSON ``value`` to type ``tp`` or raise ``_ConversionError``."""
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        return value

    origin = typing.get_origin(tp)

    if origin is None:
        if tp is str:
            if isinstance(value, str):
                return value
            raise _error(path, f"expected a string, got {_json_kind(value)}")
        if tp is int:
            if isinstance(value, int) and not isinstance(value, bool):
                return value
            if isinstance(value, float) and value.is_integer():
                return int(value)
            raise _error(path, f"expected an integer, got {_json_kind(value)}")
        if tp is float:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            raise _error(path, f"expected a number, got {_json_kind(value)}")
        if tp is bool:
            if isinstance(value, bool):
                return value
            raise _error(path, f"expected a boolean, got {_json_kind(value)}")
        if tp is None or tp is _NONE_TYPE:
            if value is None:
                return None
            raise _error(path, f"expected null, got {_json_kind(value)}")
        if isinstance(tp, type):
            if dataclasses.is_dataclass(tp):
                return _convert_dataclass(value, tp, path)
            if _is_pydantic_model(tp):
                try:
                    return tp.model_validate(value)  # type: ignore[attr-defined]
                except Exception as exc:
                    raise _ConversionError(_library_errors(exc, path)) from exc
            if _is_msgspec_struct(tp):
                try:
                    return importlib.import_module("msgspec").convert(value, tp)
                except Exception as exc:
                    raise _ConversionError(_library_errors(exc, path)) from exc
            if issubclass(tp, enum.Enum):
                try:
                    return tp(value)
                except ValueError:
                    allowed = ", ".join(repr(m.value) for m in tp)
                    raise _error(
                        path, f"{value!r} is not a valid {tp.__name__} (allowed: {allowed})", "enum"
                    ) from None
            if tp is datetime.datetime or tp is datetime.date or tp is datetime.time:
                if isinstance(value, str):
                    try:
                        text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
                        return tp.fromisoformat(text)
                    except ValueError as exc:
                        raise _error(path, f"invalid {tp.__name__}: {exc}", "value_error") from None
                if (
                    tp is datetime.datetime
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                ):
                    return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
                raise _error(path, f"expected an ISO 8601 string, got {_json_kind(value)}")
            if tp is uuid.UUID:
                try:
                    return uuid.UUID(value)
                except (ValueError, AttributeError, TypeError):
                    raise _error(path, f"invalid UUID: {value!r}", "value_error") from None
            if tp is decimal.Decimal:
                if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                    try:
                        return decimal.Decimal(str(value))
                    except decimal.InvalidOperation:
                        pass
                raise _error(path, f"invalid decimal: {value!r}", "value_error")
            if tp is bytes:
                if isinstance(value, str):
                    return value.encode("utf-8")
                raise _error(path, f"expected a string, got {_json_kind(value)}")
            if tp in (list, tuple, set, frozenset):
                if isinstance(value, list):
                    return tp(value)
                raise _error(path, f"expected an array, got {_json_kind(value)}")
            if tp is dict:
                if isinstance(value, dict):
                    return value
                raise _error(path, f"expected an object, got {_json_kind(value)}")
            if typing.is_typeddict(tp):
                if not isinstance(value, dict):
                    raise _error(path, f"expected an object, got {_json_kind(value)}")
                hints = typing.get_type_hints(tp)
                required: frozenset[str] = getattr(tp, "__required_keys__", frozenset())
                out: dict[str, Any] = dict(value)
                errors: list[dict[str, Any]] = []
                for key, hint in hints.items():
                    if key in value:
                        try:
                            out[key] = _convert(value[key], hint, (*path, key))
                        except _ConversionError as exc:
                            errors.extend(exc.errors)
                    elif key in required:
                        errors.append(
                            {"loc": (*path, key), "msg": "field required", "type": "missing"}
                        )
                if errors:
                    raise _ConversionError(errors)
                return out
            if isinstance(value, tp):
                return value
            raise _error(path, f"expected {tp.__name__}, got {_json_kind(value)}")
        supertype = getattr(tp, "__supertype__", None)  # typing.NewType
        if supertype is not None:
            return _convert(value, supertype, path)
        return value

    args = typing.get_args(tp)

    if origin is typing.Annotated:
        return _convert(value, args[0], path)
    if origin is Union or origin is types.UnionType:
        if value is None and _NONE_TYPE in args:
            return None
        collected: list[dict[str, Any]] = []
        for arg in args:
            if arg is _NONE_TYPE:
                continue
            try:
                return _convert(value, arg, path)
            except _ConversionError as exc:
                collected.extend(exc.errors)
        expected = " | ".join(_type_name(a) for a in args)
        if len([a for a in args if a is not _NONE_TYPE]) == 1 and collected:
            raise _ConversionError(collected)
        raise _error(path, f"value does not match any of: {expected}", "union_error")
    if origin is typing.Literal:
        for option in args:
            if value == option and type(value) is type(option):
                return value
        allowed = ", ".join(repr(a) for a in args)
        raise _error(path, f"expected one of: {allowed}; got {value!r}", "literal_error")
    if (
        origin in (list, set, frozenset)
        or origin is Sequence
        or (isinstance(origin, type) and issubclass(origin, Sequence) and origin is not tuple)
    ):
        if not isinstance(value, list):
            raise _error(path, f"expected an array, got {_json_kind(value)}")
        item_type = args[0] if args else Any
        items: list[Any] = []
        errors = []
        for index, item in enumerate(value):
            try:
                items.append(_convert(item, item_type, (*path, index)))
            except _ConversionError as exc:
                errors.extend(exc.errors)
        if errors:
            raise _ConversionError(errors)
        if origin in (set, frozenset):
            return origin(items)
        return items
    if origin is tuple:
        if not isinstance(value, list):
            raise _error(path, f"expected an array, got {_json_kind(value)}")
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_convert(item, args[0], (*path, i)) for i, item in enumerate(value))
        if args and len(args) != len(value):
            raise _error(path, f"expected an array of {len(args)} items, got {len(value)}")
        return tuple(
            _convert(item, arg, (*path, i))
            for i, (item, arg) in enumerate(zip(value, args, strict=False))
        )
    if (
        origin is dict
        or origin is Mapping
        or (isinstance(origin, type) and issubclass(origin, Mapping))
    ):
        if not isinstance(value, dict):
            raise _error(path, f"expected an object, got {_json_kind(value)}")
        key_type, value_type = args if len(args) == 2 else (Any, Any)
        result: dict[Any, Any] = {}
        errors = []
        for key, item in value.items():
            try:
                converted_key = _convert_key(key, key_type, (*path, key))
                result[converted_key] = _convert(item, value_type, (*path, key))
            except _ConversionError as exc:
                errors.extend(exc.errors)
        if errors:
            raise _ConversionError(errors)
        return result
    # Unknown generic alias (e.g. a user generic): fall back to its origin.
    return _convert(value, origin, path)


def _convert_key(key: str, tp: Any, path: tuple[Any, ...]) -> Any:
    """JSON object keys are always strings; coerce them for ``dict[int, ...]`` etc."""
    if tp is Any or tp is str:
        return key
    try:
        if tp is int:
            return int(key)
        if tp is float:
            return float(key)
        if isinstance(tp, type) and issubclass(tp, enum.Enum):
            return tp(key)
        if tp is uuid.UUID:
            return uuid.UUID(key)
    except ValueError:
        raise _error(path, f"invalid {_type_name(tp)} key: {key!r}", "value_error") from None
    return _convert(key, tp, path)


# -- error normalisation ------------------------------------------------------


def _library_errors(exc: BaseException, prefix: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Extract structured errors from Pydantic / msgspec exceptions."""
    errors_method = getattr(exc, "errors", None)
    if callable(errors_method):  # pydantic.ValidationError
        try:
            raw = errors_method(include_url=False)
        except TypeError:
            raw = errors_method()
        out: list[dict[str, Any]] = [
            {
                "loc": (*prefix, *tuple(item.get("loc", ()))),
                "msg": str(item.get("msg", "")),
                "type": str(item.get("type", "value_error")),
            }
            for item in raw
        ]
        if out:
            return out
    return [{"loc": prefix, "msg": str(exc), "type": type(exc).__name__}]


def _summarise(errors: Sequence[Mapping[str, Any]]) -> str:
    def one(error: Mapping[str, Any]) -> str:
        loc = ".".join(str(part) for part in error.get("loc", ()))
        return f"{loc}: {error['msg']}" if loc else str(error["msg"])

    shown = "; ".join(one(e) for e in errors[:3])
    if len(errors) > 3:
        shown += f"; ... ({len(errors) - 3} more)"
    return shown


# -- decoder selection --------------------------------------------------------


def _builtin_decoder(model: Any) -> _Decoder:
    def decode(content: bytes) -> Any:
        return _convert(json.loads(content), model, ())

    return decode


def _decoder_for(model: Any) -> _Decoder:
    try:
        cached = _DECODERS.get(model)
    except TypeError:  # unhashable type expression
        return _builtin_decoder(model)
    if cached is not None:
        return cached
    decoder: _Decoder
    if _is_pydantic_model(model):
        decoder = model.model_validate_json
    elif _is_msgspec_struct(model):
        decoder = importlib.import_module("msgspec").json.Decoder(model).decode
    else:
        decoder = _builtin_decoder(model)
    _DECODERS[model] = decoder
    return decoder


def parse_json(content: bytes, model: type[T], *, response: Response | None = None) -> T:
    """Decode ``content`` into ``model``; raise :class:`DecodeError` on failure."""
    try:
        result: T = _decoder_for(model)(content)
        return result
    except DecodeError:
        raise
    except _ConversionError as exc:
        errors = exc.errors
        cause: BaseException = exc
    except json.JSONDecodeError as exc:
        errors = [{"loc": (), "msg": f"invalid JSON: {exc}", "type": "json_invalid"}]
        cause = exc
    except Exception as exc:
        errors = _library_errors(exc)
        cause = exc
    context = ""
    if response is not None:
        context = f" (status {response.status_code} from {response.url})"
    message = (
        f"Could not decode response body as {_type_name(model)}{context}: {_summarise(errors)}"
    )
    raise DecodeError(message, response=response, model=model, errors=errors) from cause


def parse_response(response: Response, model: type[T]) -> T:
    """Decode a response body into ``model`` with status / URL error context."""
    return parse_json(response.content, model, response=response)
