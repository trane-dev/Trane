from trane.typing.column_schema import ColumnSchema
from trane.typing.ml_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
)


def test_eq():
    assert ColumnSchema(logical_type=Boolean) == ColumnSchema(logical_type=Boolean)


def test_neq():
    assert ColumnSchema(logical_type=Boolean) != ColumnSchema(logical_type=Categorical)
    assert ColumnSchema(logical_type=Boolean) != ColumnSchema(
        logical_type=Boolean,
        semantic_tags={"category"},
    )


def test_parse_column_tags():
    assert ColumnSchema(semantic_tags="category").semantic_tags == {"category"}
    assert ColumnSchema(semantic_tags=["category"]).semantic_tags == {"category"}
    assert ColumnSchema(semantic_tags={"category"}).semantic_tags == {"category"}
    assert ColumnSchema(semantic_tags=None).semantic_tags == set()


def test_is_numeric():
    assert ColumnSchema(logical_type=Boolean).is_numeric is False
    assert ColumnSchema(logical_type=Categorical).is_numeric is False

    assert (
        ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}).is_numeric is True
    )
    assert (
        ColumnSchema(logical_type=Double, semantic_tags={"numeric"}).is_numeric is True
    )
    # TODO: Should this be fixed?
    assert (
        ColumnSchema(logical_type=Boolean, semantic_tags={"numeric"}).is_numeric is True
    )
    assert (
        ColumnSchema(logical_type=Categorical, semantic_tags={"numeric"}).is_numeric
        is True
    )


def test_is_categorical():
    assert ColumnSchema(logical_type=Boolean).is_categorical is False
    assert (
        ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}).is_categorical
        is False
    )
    assert (
        ColumnSchema(
            logical_type=Categorical,
            semantic_tags={"category"},
        ).is_categorical
        is True
    )
    assert (
        ColumnSchema(logical_type=Boolean, semantic_tags={"category"}).is_categorical
        is True
    )


def test_is_datetime():
    assert ColumnSchema(logical_type=Boolean).is_datetime is False
    assert (
        ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}).is_datetime
        is False
    )
    assert (
        ColumnSchema(logical_type=Categorical, semantic_tags={"category"}).is_datetime
        is False
    )
    assert ColumnSchema(logical_type=Datetime).is_datetime is True


def test_is_boolean():
    assert ColumnSchema(logical_type=Boolean).is_boolean is True
    assert (
        ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}).is_boolean
        is False
    )
    assert (
        ColumnSchema(logical_type=Categorical, semantic_tags={"category"}).is_boolean
        is False
    )
    assert ColumnSchema(logical_type=Datetime).is_boolean is False
