from trane.typing.ml_types import (
    Boolean,
    Categorical,
    Double,
    Integer,
)


def test_eq():
    assert Boolean == Boolean


def test_neq():
    assert Boolean != Categorical


def test_str():
    assert str(Boolean()) == "Boolean"


def test_repr():
    assert repr(Boolean()) == "Boolean"


def test_get_tags():
    assert Boolean().get_tags() == set()
    assert Categorical().get_tags() == {"category"}
    category_primary = Categorical()
    category_primary.add_tags({"primary_key"})
    assert category_primary.get_tags() == {"category", "primary_key"}
    assert Integer().get_tags() == {"numeric"}
    assert Double().get_tags() == {"numeric"}
