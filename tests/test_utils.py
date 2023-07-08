from datetime import datetime

import pandas as pd

from trane.core.utils import (
    _check_operations_valid,
    _parse_table_meta,
    clean_date,
)
from trane.ops.aggregation_ops import (
    AvgAggregationOp,
    CountAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)
from trane.ops.filter_ops import (
    AllFilterOp,
    EqFilterOp,
    GreaterFilterOp,
    LessFilterOp,
    NeqFilterOp,
)
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import Double


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
        "amount": ("Integer", {"numeric"}),
    }
    table_meta = _parse_table_meta(table_meta)

    # For each <id> predict the number of records with <amount> equal to
    # Technically could be a valid operation, but we don't support it yet
    # For categorical columns it makes sense (see below)
    operations = [EqFilterOp("amount"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False
    assert len(modified_meta) == 0

    # For each <id> predict the number of records with <amount> greater than
    operations = [GreaterFilterOp("amount"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the number of records with <amount> less than
    operations = [LessFilterOp("amount"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the total <amount> in all related records
    operations = [AllFilterOp("amount"), SumAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)
    assert modified_meta["id"] == table_meta["id"]

    # For each <id> predict the total <amount> in all related records with <amount> greater than
    operations = [GreaterFilterOp("amount"), SumAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the total <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), SumAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <amount> in all related records
    operations = [AllFilterOp("amount"), AvgAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <amount> in all related records with <amount> greater than
    operations = [GreaterFilterOp("amount"), AvgAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), AvgAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <amount> in all related records
    operations = [AllFilterOp("amount"), MaxAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <amount> in all related records with <amount> greater than
    operations = [GreaterFilterOp("amount"), MaxAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), MaxAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <amount> in all related records
    operations = [AllFilterOp("amount"), MinAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <amount> in all related records with <amount> greater than
    operations = [GreaterFilterOp("amount"), MinAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), MinAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)


def verify_numeric_op(modified_meta, result):
    assert result is True
    assert modified_meta["amount"] == ColumnSchema(
        logical_type=Double,
        semantic_tags={"numeric"},
    )


def test_parse_table_cat():
    table_meta = {
        "id": ("Categorical", {"index", "category"}),
        "state": ("Categorical", {"category"}),
    }
    table_meta = _parse_table_meta(table_meta)

    # For each <id> predict the number of records with <state> equal to
    operations = [EqFilterOp("state"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # For each <id> predict the number of records with <state> not equal to
    operations = [NeqFilterOp("state"), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # For each <id> predict the majority <state> in all related records with <state> equal to NY in next 2d days
    operations = [EqFilterOp("state"), MajorityAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    operations = [AllFilterOp(None), SumAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False


def test_clean_date():
    assert clean_date("2019-01-01") == pd.Timestamp(
        datetime.strptime("2019-01-01", "%Y-%m-%d"),
    )
    timestamp = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d"))
    assert clean_date(timestamp) == timestamp
