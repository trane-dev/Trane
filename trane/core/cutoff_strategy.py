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
