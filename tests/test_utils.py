from datetime import datetime

import pandas as pd
import pytest

from trane.core.utils import (
    _check_operations_valid,
    _generate_possible_operations,
    _parse_table_meta,
    clean_date,
)
from trane.ops.aggregation_ops import (
    AggregationOpBase,
    AvgAggregationOp,
    CountAggregationOp,
    ExistsAggregationOp,
    FirstAggregationOp,
    LastAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)
from trane.ops.filter_ops import (
    AllFilterOp,
    EqFilterOp,
    FilterOpBase,
    GreaterFilterOp,
    LessFilterOp,
    NeqFilterOp,
)
from trane.ops.transformation_ops import IdentityOp, TransformationOpBase
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    Categorical,
    Datetime,
    Double,
    Integer,
)


@pytest.fixture(scope="function")
def table_meta():
    return {
        "id": ("Categorical", {"primary_key", "category"}),
        "amount": ("Integer", {"numeric"}),
        "user_id": ("Integer", {"foreign_key"}),
    }


def test_parse_table_simple(table_meta):
    modified_meta = _parse_table_meta(table_meta)
    assert len(modified_meta) == 3
    assert modified_meta["id"] == ColumnSchema(
        logical_type=Categorical,
        semantic_tags={"category", "primary_key"},
    )
    assert modified_meta["amount"] == ColumnSchema(
        logical_type=Integer,
        semantic_tags={"numeric"},
    )
    assert modified_meta["user_id"] == ColumnSchema(
        logical_type=Integer,
        semantic_tags={"foreign_key"},
    )


def test_simple_check_operations(table_meta):
    # For each <id> predict the number of records
    operations = [AllFilterOp(None), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True
    assert modified_meta["id"] == table_meta["id"]
    assert all(key in table_meta.keys() for key in modified_meta.keys())


def test_parse_table_numeric():
    table_meta = {
        "id": ("Categorical", {"primary_key", "category"}),
        "amount": ("Integer", {"numeric"}),
    }
    table_meta = _parse_table_meta(table_meta)

    # For each <id> predict the number of records with <amount> equal to
    # Technically could be a valid operation, but we don't support it yet
    # For categorical columns it makes sense (see below)
    operations = [EqFilterOp("amount"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False
    assert len(modified_meta) == 0

    # For each <id> predict the number of records with <amount> greater than
    operations = [GreaterFilterOp("amount"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the number of records with <amount> less than
    operations = [LessFilterOp("amount"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the total <amount> in all related records
    operations = [AllFilterOp("amount"), IdentityOp(None), SumAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)
    assert modified_meta["id"] == table_meta["id"]

    # For each <id> predict the total <amount> in all related records with <amount> greater than
    operations = [
        GreaterFilterOp("amount"),
        IdentityOp(None),
        SumAggregationOp("amount"),
    ]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the total <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), IdentityOp(None), SumAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <amount> in all related records
    operations = [AllFilterOp("amount"), IdentityOp(None), AvgAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <amount> in all related records with <amount> greater than
    operations = [
        GreaterFilterOp("amount"),
        IdentityOp(None),
        AvgAggregationOp("amount"),
    ]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), IdentityOp(None), AvgAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <amount> in all related records
    operations = [AllFilterOp("amount"), IdentityOp(None), MaxAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <amount> in all related records with <amount> greater than
    operations = [
        GreaterFilterOp("amount"),
        IdentityOp(None),
        MaxAggregationOp("amount"),
    ]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), IdentityOp(None), MaxAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <amount> in all related records
    operations = [AllFilterOp("amount"), IdentityOp(None), MinAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <amount> in all related records with <amount> greater than
    operations = [
        GreaterFilterOp("amount"),
        IdentityOp(None),
        MinAggregationOp("amount"),
    ]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <amount> in all related records with <amount> less than
    operations = [LessFilterOp("amount"), IdentityOp(None), MinAggregationOp("amount")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    verify_numeric_op(modified_meta, result)


def verify_numeric_op(modified_meta, result):
    assert result is True
    assert modified_meta["amount"] == ColumnSchema(
        logical_type=Double,
        semantic_tags={"numeric"},
    )


def test_check_operations_boolean():
    table_meta = {
        "id": ("Categorical", {"primary_key", "category"}),
        "is_fraud": ("Boolean", {}),
    }
    table_meta = _parse_table_meta(table_meta)
    operations = [
        EqFilterOp("is_fraud"),
        IdentityOp(None),
        MajorityAggregationOp("is_fraud"),
    ]
    result, _ = _check_operations_valid(operations, table_meta)
    assert result is False


def test_check_operations_cat():
    table_meta = {
        "id": ("Categorical", {"primary_key", "category"}),
        "state": ("Categorical", {"category"}),
    }
    table_meta = _parse_table_meta(table_meta)

    # For each <id> predict the number of records with <state> equal to
    operations = [EqFilterOp("state"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # For each <id> predict the number of records with <state> not equal to
    operations = [NeqFilterOp("state"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # For each <id> predict the majority <state> in all related records with <state> equal to NY
    operations = [EqFilterOp("state"), IdentityOp(None), MajorityAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # Not a valid operation
    # cannot do GreaterFilterOp on categorical
    operations = [GreaterFilterOp("state"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False

    # Not a valid operation
    # cannot do SumAggregation on categorical
    operations = [AllFilterOp(None), IdentityOp(None), SumAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False

    # For each <id> predict if there exists a record in all related records with <state> equal to NY
    operations = [AllFilterOp(None), IdentityOp(None), ExistsAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # For each <id> predict the first <state> in all related records
    operations = [AllFilterOp(None), IdentityOp(None), FirstAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True

    # For each <id> predict the last <state> in all related records
    operations = [AllFilterOp(None), IdentityOp(None), LastAggregationOp("state")]
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is True


def test_foreign_key():
    table_meta = {
        "id": ("Categorical", {"primary_key", "category"}),
        "amount": ("Integer", {"numeric"}),
        "department": ("Categorical", {"category"}),
        "user_id": ("Integer", {"numeric", "foreign_key"}),
    }
    # For each <orders.user_id> predict the total <user_id> in all related
    # records with <products.departments.department> equal to dairy eggs in next 2w days
    # [EqFilterOp, SumAggregationOp]
    operations = [
        EqFilterOp("department"),
        IdentityOp(None),
        SumAggregationOp("user_id"),
    ]
    table_meta = _parse_table_meta(table_meta)
    result, modified_meta = _check_operations_valid(operations, table_meta)
    assert result is False


def test_generate_possible_operations():
    table_meta = {
        "id": ("Categorical", {"primary_key", "category"}),
        "time": ("Datetime", {"time_index"}),
        "amount": ("Integer", {"numeric"}),
        "department": ("Categorical", {"category"}),
        "user_id": ("Integer", {"numeric", "foreign_key"}),
    }
    table_meta = _parse_table_meta(table_meta)
    possible_operations = _generate_possible_operations(
        table_meta=table_meta,
    )
    for filter_op, transform_op, agg_op in possible_operations:
        assert isinstance(agg_op, AggregationOpBase)
        assert isinstance(transform_op, TransformationOpBase)
        assert isinstance(filter_op, FilterOpBase)
        if isinstance(agg_op, CountAggregationOp):
            assert agg_op.column_name is None
        if isinstance(filter_op, AllFilterOp):
            assert filter_op.column_name is None
        assert agg_op.column_name not in ["id", "time", "user_id"]
        assert filter_op.column_name not in ["id", "time", "user_id"]
        assert {
            filter_op.__class__.__name__,
            transform_op.__class__.__name__,
            agg_op.__class__.__name__,
        }.intersection(filter_op.restricted_ops) == set()
        assert {
            filter_op.__class__.__name__,
            transform_op.__class__.__name__,
            agg_op.__class__.__name__,
        }.intersection(transform_op.restricted_ops) == set()
        assert {
            filter_op.__class__.__name__,
            transform_op.__class__.__name__,
            agg_op.__class__.__name__,
        }.intersection(agg_op.restricted_ops) == set()


def test_clean_date():
    assert clean_date("2019-01-01") == pd.Timestamp(
        datetime.strptime("2019-01-01", "%Y-%m-%d"),
    )
    timestamp = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d"))
    assert clean_date(timestamp) == timestamp


def test_parse_table_meta():
    meta = {
        "id": ("Categorical", {"primary_key", "category"}),
        "date": "Datetime",
        "cost": ("Double", {"numeric"}),
        "amount": (None, {"numeric"}),
    }
    parsed_meta = _parse_table_meta(meta)
    assert parsed_meta["id"] == ColumnSchema(
        logical_type=Categorical,
        semantic_tags={"primary_key", "category"},
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
