from trane.core.utils import _check_operations_valid, _parse_table_meta
from trane.ops.aggregation_ops import CountAggregationOp
from trane.ops.filter_ops import EqFilterOp, GreaterFilterOp


def test_parse_table():
    table_meta = {
        "id": ("Categorical", {"index", "category"}),
        "amount": ("Double", {"numeric"}),
    }
    table_meta = _parse_table_meta(table_meta)
    operations = [EqFilterOp("amount"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False
    assert len(modified_meta) == 0

    operations = [GreaterFilterOp("amount"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True
    assert modified_meta["id"] == table_meta["id"]
