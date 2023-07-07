from trane.typing.logical_types import (
    Boolean,
    Categorical,
)


def test_eq():
    assert Boolean == Boolean


def test_neq():
    assert Boolean != Categorical


def test_str():
    assert str(Boolean()) == "Boolean"
