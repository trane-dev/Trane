from trane.core.utils import clean_date


class CutoffStrategy:
    def __init__(
        self,
        entity_col,
        window_size,
        minimum_data=None,
        maximum_data=None,
    ):
        self.entity_col = entity_col
        self.target_dataframe_index = entity_col
        self.window_size = window_size
        self.minimum_data = clean_date(minimum_data)
        self.maximum_data = clean_date(maximum_data)
        self.description = "in next {} days".format(window_size)

    # I don't think this code is needed but not 100% sure
    # def generate_cutoffs(self, df):
    #     cutoff_st_ed_pairs = []
    #     current = self.cutoff_base
    #     while True:
    #         current_end = current + timedelta(days=self.cutoff_window)
    #         if current_end > self.cutoff_end:
    #             break
    #         cutoff_st_ed_pairs.append((current, current_end))
    #         current = current_end
    #     entity_cutoffs = []
    #     for entity_name in set(df[self.entity_col]):
    #         for cutoff_st, cutoff_ed in cutoff_st_ed_pairs:
    #             entity_cutoffs.append((entity_name, cutoff_st, cutoff_ed))
    #     return pd.DataFrame(
    #         entity_cutoffs,
    #         columns=[self.entity_col, "cutoff_st", "cutoff_ed"],
    #     )
