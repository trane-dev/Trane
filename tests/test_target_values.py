import numpy as np
import pandas as pd
import pytest

from trane import SingleTableMetadata
from trane.core.problem import Problem
from trane.ops.aggregation_ops import ExistsAggregationOp
from trane.ops.filter_ops import AllFilterOp, GreaterFilterOp
from trane.ops.transformation_ops import IdentityOp


@pytest.fixture
def data():
    num_rows = 100
    data = {
        "building_id": np.random.randint(0, 100, num_rows),
        "timestamp": pd.date_range(start="2016-01-01", periods=num_rows, freq="H"),
        "meter_reading": np.random.uniform(0, 100, num_rows),
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def metadata():
    ml_types = {
        "building_id": "Integer",
        "timestamp": "Datetime",
        "meter_reading": "Double",
    }
    metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key="building_id",
        time_index="timestamp",
    )
    return metadata


def test_greater_than(data, metadata):
    operations = [
        GreaterFilterOp("meter_reading"),
        IdentityOp(None),
        ExistsAggregationOp(None),
    ]
    problem = Problem(
        metadata=metadata,
        operations=operations,
        entity_column="building_id",
        window_size="2d",
    )
    labels = problem.create_target_values(data)
    print(labels)


def test_exists(data, metadata):
    operations = [AllFilterOp(None), IdentityOp(None), ExistsAggregationOp(None)]
    problem = Problem(
        metadata=metadata,
        operations=operations,
        entity_column="building_id",
        window_size="2d",
    )
    labels = problem.create_target_values(data)
    print(labels)
