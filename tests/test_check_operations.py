from datetime import datetime

import pandas as pd
import pytest

from trane.core.problem import (
    _check_operations_valid,
    check_ml_type_valid,
)
from trane.core.problem_generator import _generate_possible_operations
from trane.core.utils import clean_date
from trane.metadata import SingleTableMetadata
from trane.ops import (
    AggregationOpBase,
    AllFilterOp,
    AvgAggregationOp,
    CountAggregationOp,
    EqFilterOp,
    ExistsAggregationOp,
    FilterOpBase,
    FirstAggregationOp,
    GreaterFilterOp,
    IdentityOp,
    LastAggregationOp,
    LessFilterOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    NeqFilterOp,
    SumAggregationOp,
    TransformationOpBase,
)
from trane.typing.ml_types import (
    Boolean,
    Double,
    Integer,
    MLType,
)
from trane.utils.testing_utils import generate_mock_data


@pytest.fixture(scope="function")
def metadata():
    _, ml_types, _, primary_key, time_index = generate_mock_data(
        tables=["products"],
    )
    # user creates single table metadata
    metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key=primary_key,
        time_index=time_index,
    )
    return metadata


def test_simple_check_operations(metadata):
    # For each <id> predict the number of records
    operations = [AllFilterOp(None), IdentityOp(None), CountAggregationOp(None)]
    result, modified_ml_types = _check_operations_valid(operations, metadata)
    assert result is True
    assert modified_ml_types["id"] == metadata.ml_types["id"]
    assert all(key in metadata.ml_types.keys() for key in modified_ml_types.keys())


def test_check_ml_type_valid():
    assert (
        check_ml_type_valid(op_input_ml_type=Integer(), column_ml_type=Integer())
        is True
    )
    assert check_ml_type_valid(MLType(), Boolean()) is True

    assert check_ml_type_valid(MLType(), Double()) is True
    assert check_ml_type_valid(MLType(), Double) is True

    assert check_ml_type_valid(Integer(), Integer()) is True


def test_parse_table_numeric(metadata):
    # For each <id> predict the number of records with <price> equal to
    # Technically could be a valid operation, but we don't support it yet
    # For categorical columns it makes sense (see below)
    operations = [EqFilterOp("price"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is False
    assert len(modified_meta) == 0

    assert metadata.ml_types["price"].get_tags() == {"numeric"}
    # For each <id> predict the number of records with <price> greater than
    operations = [GreaterFilterOp("price"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the number of records with <price> less than
    operations = [LessFilterOp("price"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the total <price> in all related records
    operations = [AllFilterOp("price"), IdentityOp(None), SumAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)
    assert modified_meta["id"] == metadata.ml_types["id"]

    # For each <id> predict the total <price> in all related records with <price> greater than
    operations = [GreaterFilterOp("price"), IdentityOp(None), SumAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the total <price> in all related records with <price> less than
    operations = [LessFilterOp("price"), IdentityOp(None), SumAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <price> in all related records
    operations = [AllFilterOp("price"), IdentityOp(None), AvgAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <price> in all related records with <price> greater than
    operations = [GreaterFilterOp("price"), IdentityOp(None), AvgAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the average <price> in all related records with <price> less than
    operations = [LessFilterOp("price"), IdentityOp(None), AvgAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <price> in all related records
    operations = [AllFilterOp("price"), IdentityOp(None), MaxAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <price> in all related records with <price> greater than
    operations = [
        GreaterFilterOp("price"),
        IdentityOp(None),
        MaxAggregationOp("price"),
    ]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the maximum <price> in all related records with <price> less than <threshold>
    operations = [LessFilterOp("price"), IdentityOp(None), MaxAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <price> in all related records
    operations = [AllFilterOp("price"), IdentityOp(None), MinAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <price> in all related records with <price> greater than
    operations = [GreaterFilterOp("price"), IdentityOp(None), MinAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)

    # For each <id> predict the minimum <price> in all related records with <price> less than
    operations = [LessFilterOp("price"), IdentityOp(None), MinAggregationOp("price")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    verify_numeric_op(modified_meta, result)


def verify_numeric_op(modified_ml_types, result, column="price"):
    assert result is True
    assert isinstance(modified_ml_types[column], Double)


def test_check_operations_boolean(metadata):
    operations = [
        EqFilterOp("first_purchase"),
        IdentityOp(None),
        MajorityAggregationOp("first_purchase"),
    ]
    result, _ = _check_operations_valid(operations, metadata)
    assert result is False


def test_check_operations_cat(metadata):
    # For each <id> predict the number of records with <card_type> equal to
    operations = [EqFilterOp("card_type"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is True

    # For each <id> predict the number of records with <card_type> not equal to
    operations = [NeqFilterOp("card_type"), IdentityOp(None), CountAggregationOp(None)]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is True

    # For each <id> predict the majority <card_type> in all related records with <card_type> equal to NY
    operations = [
        EqFilterOp("card_type"),
        IdentityOp(None),
        MajorityAggregationOp("card_type"),
    ]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is True

    # Not a valid operation
    # cannot do GreaterFilterOp on categorical
    operations = [
        GreaterFilterOp("card_type"),
        IdentityOp(None),
        CountAggregationOp(None),
    ]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is False

    # Not a valid operation
    # cannot do SumAggregation on categorical
    operations = [AllFilterOp(None), IdentityOp(None), SumAggregationOp("card_type")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is False

    # For each <id> predict if there exists a record in all related records with <card_type> equal to NY
    operations = [AllFilterOp(None), IdentityOp(None), ExistsAggregationOp("card_type")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is True

    # For each <id> predict the first <card_type> in all related records
    operations = [AllFilterOp(None), IdentityOp(None), FirstAggregationOp("card_type")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is True

    # For each <id> predict the last <card_type> in all related records
    operations = [AllFilterOp(None), IdentityOp(None), LastAggregationOp("card_type")]
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is True


def test_foreign_key(metadata):
    operations = [
        EqFilterOp("price"),
        IdentityOp(None),
        SumAggregationOp("card_type"),
    ]
    metadata.ml_types["card_type"].add_tags({"foreign_key"})
    result, modified_meta = _check_operations_valid(operations, metadata)
    assert result is False


def test_generate_possible_operations(metadata):
    # ml_types: Dict[str, MLType],
    # primary_key: str = None,
    # time_index: str = None,
    # aggregation_operations: List[AggregationOpBase] = None,
    # filter_operations: List[FilterOpBase] = None,
    # transformation_operations: List[TransformationOpBase] = None,
    possible_operations = _generate_possible_operations(
        ml_types=metadata.ml_types,
        primary_key=metadata.primary_key,
        time_index=metadata.time_index,
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
        # assert {
        #     filter_op.__class__.__name__,
        #     transform_op.__class__.__name__,
        #     agg_op.__class__.__name__,
        # }.intersection(agg_op.restricted_ops) == set()


def test_clean_date():
    assert clean_date("2019-01-01") == pd.Timestamp(
        datetime.strptime("2019-01-01", "%Y-%m-%d"),
    )
    timestamp = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d"))
    assert clean_date(timestamp) == timestamp
