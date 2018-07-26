# IMPORTANT INFO ABOUT FILE STRUCTURE:
# Each cutoff strategy will be it's own class.
# The only function that should change across classes is
# apply_cutoff_strategy()
import numpy as np
import pandas as pd


__all__ = ["CutoffStrategy"]


class CutoffStrategy:
    """
    Class that holds a CutoffStrategy. This is a measure to prevent leakage

    Parameters
    ----------
    generate_fn: a function that generates training and test cutoff times.
        input: entity rows
        output: a tuple with the following timestamps:
            (training_cutoff, test_cutoff)

    Returns
    -------
    CutoffStrategy Instance
    """

    def __init__(self, generate_fn, description='undescribed cutoff strategy'):
        self.generate_fn = generate_fn
        self.description = description

    def generate_cutoffs(self, df, entity_id_col):
        """
        generate a cutoff dataframe. Takes about 10 sec to load, but then runs
        inNlogN time.

        Parameters
        ----------
        df: Pandas.DataFrame that problems are generated from
        entity_id_col: str name of the entity_id_column

        Returns
        -------
        DF with three columns:
        entity_id (indexed) | training_cutoff | test_cutoff

        training_cutoff and test_cutoff are of type np.datetime64
        """

        # group by the entity id column
        grouped = df.groupby(entity_id_col)

        val_arr = []

        # for each group, compute the training and test cutoff
        for entity_id, df_group in grouped:

            # we re-index by the entity_id column because
            # a user's generate_fn may expect it
            df_group.set_index(entity_id_col, inplace=True)
            training_cutoff, test_cutoff = self.generate_fn(
                entity_id, df_group)

            # add this data to a long array
            val_arr.extend((entity_id, training_cutoff, test_cutoff))

        # reshape the array so that it has 3 columns.
        # -1 indicates that the number of rows is inferred
        data = np.array(val_arr).reshape(-1, 3)
        cutoff_df = pd.DataFrame(
            data, columns=[entity_id_col, 'training_cutoff', 'test_cutoff'])
        cutoff_df.set_index(entity_id_col, inplace=True)

        return cutoff_df
