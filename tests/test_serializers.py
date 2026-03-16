"""Tests for serializers."""

from __future__ import annotations

import pytest

from flashq.serializers import JSONSerializer, PickleSerializer


class TestJSONSerializer:
    def test_roundtrip_dict(self):
        s = JSONSerializer()
        data = {"key": "value", "num": 42, "list": [1, 2, 3]}
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_string(self):
        s = JSONSerializer()
        assert s.loads(s.dumps("hello")) == "hello"

    def test_roundtrip_none(self):
        s = JSONSerializer()
        assert s.loads(s.dumps(None)) is None

    def test_unicode(self):
        s = JSONSerializer()
        data = {"emoji": "🚀", "turkish": "çğıöşü"}
        result = s.loads(s.dumps(data))
        assert result == data

    def test_compact_output(self):
        s = JSONSerializer()
        raw = s.dumps({"a": 1, "b": 2})
        text = raw.decode()
        assert " " not in text  # compact separators

    def test_unserializable_raises(self):
        s = JSONSerializer()
        with pytest.raises(TypeError):
            s.dumps(object())


class TestPickleSerializer:
    def test_roundtrip_dict(self):
        s = PickleSerializer()
        data = {"key": "value", "set": {1, 2, 3}}
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_complex_types(self):
        s = PickleSerializer()
        data = {"tuple": (1, 2), "bytes": b"hello", "frozenset": frozenset([1, 2])}
        result = s.loads(s.dumps(data))
        assert result == data

    def test_roundtrip_class_instance(self):
        import datetime

        s = PickleSerializer()
        # Use a stdlib type instead of local class (unpicklable)
        obj = datetime.timedelta(days=5, hours=3)
        result = s.loads(s.dumps(obj))
        assert result == obj
