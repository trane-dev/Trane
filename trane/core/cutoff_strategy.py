from datetime import timedelta

import pandas as pd

__all__ = ["FixWindowCutoffStrategy"]


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


class FixWindowCutoffStrategy(CutoffStrategy):
    def __init__(self, entity_col, cutoff_base, cutoff_end, cutoff_window):
        self.description = "in next {} days".format(cutoff_window)
        self.cutoff_base = cutoff_base
        self.cutoff_end = cutoff_end
        self.cutoff_window = cutoff_window
        self.entity_col = entity_col

    def generate_cutoffs(self, df):
        cutoff_st_ed_pairs = []

        current = self.cutoff_base
        while True:
            current_end = current + timedelta(days=self.cutoff_window)
            if current_end > self.cutoff_end:
                break
            cutoff_st_ed_pairs.append((current, current_end))
            current = current_end

        entity_cutoffs = []
        for entity_name in set(df[self.entity_col]):
            for cutoff_st, cutoff_ed in cutoff_st_ed_pairs:
                entity_cutoffs.append((entity_name, cutoff_st, cutoff_ed))

        return pd.DataFrame(entity_cutoffs, columns=[self.entity_col, "cutoff_st", "cutoff_ed"])
