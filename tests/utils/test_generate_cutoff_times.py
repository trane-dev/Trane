from trane.utils.generate_cutoff_times import *
from pandas import DataFrame


def test_fixed_cutoff_time():
    cutoff_strategy = ConstantIntegerCutoffTimes(0)
    data = {
        'entity1': DataFrame({'col1': [1, 2, 3]}),
        'entity2': DataFrame({'col1': [4, 5, 6]})
    }
    cutoffs = cutoff_strategy.generate_cutoffs(data)
    assert type(cutoffs) == dict
    assert cutoffs['entity1'][1] == 0
    assert cutoffs['entity2'][1] == 0
