import numpy as np
import pandas as pd


class CutoffStrategy:
    """
    Class that holds a CutoffStrategy. This is a measure to prevent leakage

    Parameters
    ----------
    generate_fn: a function that generates a cutoff time for a given entity.
        input: entity rows
        output: a training cutoff in np.datetime64 format

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
        inNlogN time. (probably. Haven't checked numpy and pandas code. That's
        just what anecdotally seen in testing)

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

            cutoff = None

            if self.generate_fn:
                cutoff = self.generate_fn(
                    df_group, entity_id)

            # add this data to a long array
            val_arr.extend((entity_id, cutoff))

        # reshape the array so that it has 3 columns.
        # -1 indicates that the number of rows is inferred
        data = np.array(val_arr).reshape(-1, 2)
        cutoff_df = pd.DataFrame(data)
        cutoff_df.rename(columns={0: entity_id_col, 1: 'cutoff'}, inplace=True)
        cutoff_df.set_index(entity_id_col, inplace=True)

        return cutoff_df
