from trane.utils.generate_cutoff_times import *
from pandas import DataFrame


def test_fixed_cutoff_time():
    cutoff_strategy = ConstantCutoffTime(0, 0)
    data = {
        'entity1': DataFrame({'col1': [1, 2, 3]}),
        'entity2': DataFrame({'col1': [4, 5, 6]})
    }
    cutoffs = cutoff_strategy.generate_cutoffs(data, 'col1')
    assert type(cutoffs) == dict
    assert cutoffs['entity1'][1] == 0
    assert cutoffs['entity2'][1] == 0

def test_dynamic_cutoff_time():
    cutoff_strategy = DynamicCutoffTime()

    data = {
        'entity1': DataFrame({'col1': [1, 1, 1, 2, 3]}),
        'entity2': DataFrame({'col1': [4, 4, 4, 6, 5]})
    }
    cutoffs = cutoff_strategy.generate_cutoffs(data, 'col1')
    assert type(cutoffs) == dict
    assert cutoffs['entity1'][1] == 2
    assert cutoffs['entity1'][2] == 3
    
    assert cutoffs['entity2'][1] == 5
    assert cutoffs['entity2'][2] == 6
