from trane.core.prediction_problem import _parse_table_meta
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    Categorical,
    Datetime,
    Double,
)


def test_parse_table_meta():
    meta = {
        "id": ("Categorical", {"index", "category"}),
        "date": "Datetime",
        "cost": ("Double", {"numeric"}),
        "amount": (None, {"numeric"}),
    }
    parsed_meta = _parse_table_meta(meta)
    assert parsed_meta["id"] == ColumnSchema(
        logical_type=Categorical,
        semantic_tags={"index", "category"},
    )
    assert parsed_meta["date"] == ColumnSchema(logical_type=Datetime, semantic_tags={})
    assert parsed_meta["cost"] == ColumnSchema(
        logical_type=Double,
        semantic_tags={"numeric"},
    )
    assert parsed_meta["amount"] == ColumnSchema(
        logical_type=None,
        semantic_tags={"numeric"},
    )
    assert len(parsed_meta) == 4
