"""Serialization utilities for task arguments and results.

Default serializer uses JSON (safe, human-readable, cross-language compatible).
A pickle-based serializer is available for complex Python objects but must be
explicitly opted in due to security implications.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """Base interface for serializers."""

    @abstractmethod
    def dumps(self, obj: Any) -> bytes:
        """Serialize *obj* to bytes."""

    @abstractmethod
    def loads(self, data: bytes) -> Any:
        """Deserialize bytes back to a Python object."""


class JSONSerializer(Serializer):
    """JSON-based serializer (default).

    Safe, human-readable, and interoperable with other languages.
    Only supports JSON-serializable types (str, int, float, list, dict, None, bool).
    """

    def __init__(self, *, ensure_ascii: bool = False, sort_keys: bool = False) -> None:
        self._ensure_ascii = ensure_ascii
        self._sort_keys = sort_keys

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(
            obj,
            ensure_ascii=self._ensure_ascii,
            sort_keys=self._sort_keys,
            separators=(",", ":"),  # compact
        ).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class PickleSerializer(Serializer):
    """Pickle-based serializer.

    .. warning::
        Only use this if you fully trust the source of the serialized data.
        Pickle can execute arbitrary code during deserialization.
    """

    def __init__(self, protocol: int | None = None) -> None:
        import pickle

        self._protocol = protocol or pickle.HIGHEST_PROTOCOL

    def dumps(self, obj: Any) -> bytes:
        import pickle

        return pickle.dumps(obj, protocol=self._protocol)

    def loads(self, data: bytes) -> Any:
        import pickle

        return pickle.loads(data)


# Default serializer instance
default_serializer = JSONSerializer()
