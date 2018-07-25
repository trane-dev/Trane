from pandas import DataFrame
import numpy as np

from trane.utils.generate_cutoff_times import CutoffStrategy  # noqa


def test_fixed_cutoff_time():
    def generate_fn(entity_id, group):
        training_cutoff = np.datetime64('1980-02-25')
        test_cutoff = np.datetime64('2000-02-25')
        return((training_cutoff, test_cutoff))

    cutoff_strategy = CutoffStrategy(
        generate_fn=generate_fn,
        description='foo description')

    test_data = DataFrame(
        data=np.arange(90).reshape(-1, 3),
        columns=['entity_id', 'other_data', 'other_data_2'])

    cutoff_df = cutoff_strategy.generate_cutoffs(
        df=test_data, entity_id_col='entity_id')

    # no variance in training_cutoff or test_cutoff columns
    assert len(cutoff_df.training_cutoff.unique()) == 1
    assert len(cutoff_df.test_cutoff.unique()) == 1

    assert cutoff_df.training_cutoff.unique()[0] == np.datetime64('1980-02-25')
    assert cutoff_df.test_cutoff.unique()[0] == np.datetime64('2000-02-25')

    assert cutoff_strategy.description == 'foo description'


def test_dynamic_cutoff_time():
    def generate_fn(entity_id, group):

        # test_cutoff is 5 days after first row's date
        # import pdb; pdb.set_trace()
        date_of_first_row = group.iloc[0].dates
        test_cutoff = date_of_first_row - np.timedelta64(5, 'D')

        # training_cutoff is constant
        training_cutoff = np.datetime64('1980-02-25')

        return((training_cutoff, test_cutoff))

    cutoff_strategy = CutoffStrategy(
        generate_fn=generate_fn,
        description='foo description')

    data = {'entity_id': ['a', 'a', 'b'],
            'dates': [
                np.datetime64('1980-02-25'),
                np.datetime64('1980-02-24'),
                np.datetime64('1980-02-23')],
            'other_col': ['d', 'e', 'f']}

    test_data = DataFrame(data)

    cutoff_df = cutoff_strategy.generate_cutoffs(
        df=test_data, entity_id_col='entity_id')

    # no variance in training_cutoff columns
    assert len(cutoff_df.training_cutoff.unique()) == 1
    assert cutoff_df.training_cutoff.unique()[0] == np.datetime64('1980-02-25')

    # test_cutoff should be a different date
    assert cutoff_df.loc['a'].test_cutoff == np.datetime64('1980-02-20')
    assert cutoff_df.loc['b'].test_cutoff == np.datetime64('1980-02-18')

    # description should be the same
    assert cutoff_strategy.description == 'foo description'
