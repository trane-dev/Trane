from trane.typing.ml_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    MLType,
)


def test_eq():
    assert Boolean == Boolean
    assert Datetime == Datetime
    assert Datetime() == Datetime()


def test_neq():
    assert Boolean != Categorical
    assert Boolean != Boolean(tags={"category"})
    assert Datetime != Datetime()
    assert Boolean() != Datetime()


def test_str():
    assert str(Boolean()) == "Boolean"


def test_repr():
    assert repr(Boolean()) == "Boolean"


def test_parse_column_tags():
    ml_type = MLType(tags={"set"})
    assert ml_type.get_tags() == {"set"}

    ml_type = MLType()
    ml_type.add_tags({"list"})
    assert ml_type.get_tags() == {"list"}

    ml_type = MLType()
    ml_type.add_tags("str")
    assert ml_type.get_tags() == {"str"}


def test_add_remove_tags():
    boolean_ = Boolean()
    assert boolean_.get_tags() == set()
    assert Categorical().get_tags() == {"category"}
    category_primary = Categorical()
    category_primary.add_tags({"primary_key"})
    assert category_primary.get_tags() == {"category", "primary_key"}
    category_primary.remove_tag("primary_key")
    assert category_primary.get_tags() == {"category"}
    assert Integer().get_tags() == {"numeric"}
    assert Double().get_tags() == {"numeric"}


def test_is_numeric():
    assert Boolean().is_numeric is False
    assert Categorical().is_numeric is False

    assert Integer(tags={"numeric"}).is_numeric is True
    assert Integer().is_numeric is True
    assert Double(tags={"numeric"}).is_numeric is True
    assert Double().is_numeric is True

    assert Boolean(tags={"numeric"}).is_numeric is False
    assert Categorical(tags={"numeric"}).is_numeric is False
