import numpy as np
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from trane.ops.filter_ops import (
    AllFilterOp,
    GreaterFilterOp,
    EqFilterOp,
    NeqFilterOp,
    LessFilterOp
)
from trane.utils.table_meta import TableMeta

@pytest.fixture
def df():
    df = pd.DataFrame({'col': [1, 2, 3, 4, 5]})
    return df

@pytest.fixture
def meta():
    meta = TableMeta({
        "tables": [
            {"fields": [{
                'name': 'col', 
                'type': TableMeta.SUPERTYPE[TableMeta.TYPE_FLOAT], 
                'subtype': TableMeta.TYPE_FLOAT}]
            }
        ]})
    return meta


@pytest.mark.parametrize("filter_operation,expected_values", [
    (AllFilterOp, [1, 2, 3, 4, 5]), 
    (GreaterFilterOp, [3, 4, 5]), 
    (EqFilterOp, [2]), 
    (NeqFilterOp, [1, 3, 4, 5]),  
    (LessFilterOp, [1]),
])
def test_agg_ops(df, meta, filter_operation, expected_values):
    op = filter_operation('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df)
    output = output.reset_index(drop=True)
    expected_df = pd.DataFrame({'col': expected_values})
    assert output.equals(expected_df)
