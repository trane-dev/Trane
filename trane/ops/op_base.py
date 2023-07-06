import pandas as pd

from trane.ops.threshold_functions import sample_unique_values

__all__ = ["OpBase"]


class OpBase(object):
    """
    Super class of all operations.
    """

    description = None
    threshold = None

    def __init__(self, column_name, input_type=None, output_type=None):
        """
        Initalization of all operations.
        Subclasses shouldn't have their own init.

        Parameters
        ----------
        column_name: the column this operation applies to
        input_type: the ColumnSchema of the input column
        output_type: the ColumnSchema of the output column

        Returns
        -------
        None
        """
        self.column_name = column_name

    def __call__(self, dataslice):
        return self.label_function(dataslice)

    def label_function(self, dataslice):
        raise NotImplementedError

    def generate_description(self):
        if self.description:
            return self.description.format(self.column_name)
        return self.description

    def set_parameters(self, threshold: float):
        raise NotImplementedError

    def find_threshold_by_fraction_of_data_to_keep(
        self,
        fraction_of_data_target: float,
        df: pd.DataFrame,
        label_col: str,
        max_num_unique_values: int = 10,
        max_number_of_rows: int = 2000,
        random_state: int = None,
    ):
        original_threshold = self.threshold
        unique_vals = sample_unique_values(
            df[label_col],
            max_num_unique_values,
            random_state,
        )
        if len(df) > max_number_of_rows:
            df = df.sample(max_number_of_rows, random_state=random_state)

        best_score, best_threshold = 1, 0
        original_num_rows = len(df)
        for unique_val in unique_vals:
            self.set_parameters(threshold=unique_val)
            filtered_df = self.label_function(df)
            count = len(filtered_df)
            fraction_of_data_left = count / original_num_rows
            score = abs(fraction_of_data_left - fraction_of_data_target)
            # minimize the score (reduce the difference)
            if score < best_score:
                best_score = score
                best_threshold = unique_val
        self.set_parameters(threshold=original_threshold)
        return best_threshold

    def __hash__(self):
        return hash((type(self).__name__, self.column_name))

    # def __repr__(self):
    #     hyper_param_str = ",".join(
    #         [str(x) for x in list(self.hyper_parameter_settings.values())],
    #     )
    #     if len(hyper_param_str) > 0:
    #         hyper_param_str = "@" + hyper_param_str
    #     return "%s(%s%s)" % (type(self).__name__, self.column_name, hyper_param_str)

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False
