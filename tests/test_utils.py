from datetime import datetime

import pandas as pd

from trane.core.utils import _check_operations_valid, _parse_table_meta, clean_date
from trane.ops.aggregation_ops import CountAggregationOp
from trane.ops.filter_ops import AllFilterOp, EqFilterOp, GreaterFilterOp


def test_parse_table_simple():
    table_meta = {
        "id": ("Categorical", {"index", "category"}),
    }
    table_meta = _parse_table_meta(table_meta)
    # For each <id> predict the number of records
    operations = [AllFilterOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True
    assert modified_meta["id"] == table_meta["id"]


def test_parse_table_numeric():
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


def test_parse_table_cat():
    table_meta = {
        "id": ("Categorical", {"index", "category"}),
        "state": ("Categorical", {"category"}),
    }
    table_meta = _parse_table_meta(table_meta)
    operations = [EqFilterOp("state"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True


def test_clean_date():
    assert clean_date("2019-01-01") == pd.Timestamp(
        datetime.strptime("2019-01-01", "%Y-%m-%d"),
    )
    timestamp = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d"))
    assert clean_date(timestamp) == timestamp
