import random
from collections import Counter

from scipy import stats

__all__ = ['OpBase']


class OpBase(object):
    """
    Super class of all operations.
    All operations should have REQUIRED_PARAMETERS and IOTYPES.

    IOTYPES is a list of possible input and output type pairs.
        For example `greater` can operate on int and str and output bool.
        [(int, bool), (str, bool), ...]

    REQUIRED_PARAMETERS is a list of parameter and type dicts.

    hyper_parameter_settings is a dict of parameter name and value.

    """

    REQUIRED_PARAMETERS = None
    IOTYPES = None

    def __init__(self, column_name):
        """
        Initalization of all operations. Subclasses shouldn't have their own
        init.

        Parameters
        ----------
        column_name: the column this operation applies to

        Returns
        -------
        None
        """
        self.column_name = column_name
        self.input_type = None
        self.output_type = None
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        """
        Data type check for the operation.
        Operations may change the data type of a column, eg. int -> bool.
        One operation can only be applied on a few data types, eg. `greater`
        can be applied on int but can't be applied on bool.
        This function checks whether the current operation can be applied on
        the data.
        It returns the updated TableMeta for next operation or None if it's not
        valid.

        Parameters
        ----------
        table_meta: table meta before this operation.

        Returns
        -------
        table_meta: table meta after this operation. None if not compatable.

        """
        self.input_type = table_meta.get_type(self.column_name)
        for idx, (input_type, output_type) in enumerate(self.IOTYPES):
            if self.input_type == input_type:
                self.output_type = output_type
                table_meta.set_type(self.column_name, output_type)
                return table_meta
        return None

    def set_hyper_parameter(self, hyper_parameter):
        """
        Set the hyper parameter of the operation.

        Parameters
        ----------
        hyper_parameter: value for the hyper parameter

        Returns
        -------
        None

        """
        for parameter_requirement in self.REQUIRED_PARAMETERS:
            for parameter_name, parameter_type in parameter_requirement.items():  # noqa
                self.hyper_parameter_settings[parameter_name] = hyper_parameter

    def __call__(self, dataframe):
        return self.execute(dataframe)

    def execute(self, dataframe):
        raise NotImplementedError

    def auto_set_hyperparams(
            self, df, label_col, entity_col, filter_col=None,
            num_random_samples=10, num_rows_to_execute_on=2000):
        """
        Sets hyperparameters for the operation. In most cases (as implemented
        here), this is done by finding threshhold values that maximize column
        diversity. This can be overrideen in subclasses, as it is for
        filter ops.

        Parameters
        ----------
        df: Dataframe to be tuned to
        label_col: str, name of the column of interest in the data
        entity_col: str, name of the column containing entities ids in the data
        num_random_samples: if there's more than this many unique values to
            test, randomly sample this many values from the dataset
        num_rows_to_execute_on: if the dataframe contains more than this number
            of rows, randomly select this many rows to use as the dataframe

        Returns
        -------
        hyperparameter: But this has already been set to the operation

        """

        # If the operator has no required parameters, return None
        if len(self.REQUIRED_PARAMETERS) == 0:
            return None

        hyperaparam = 0
        # hyperaparam = self.find_threshhold_by_diversity(
        #     df=df, label_col=label_col,
        #     entity_col=entity_col,
        #     num_random_samples=num_random_samples,
        #     num_rows_to_execute_on=num_rows_to_execute_on)

        self.set_hyper_parameter(hyperaparam)
        return hyperaparam

    def find_threshhold_by_remaining(
            self, fraction_of_data_target, df, col, num_random_samples=10,
            num_rows_to_execute_on=2000):
        """
        This function finds and returns a parameter setting for the
        op. The parameter setting that comes closest to the
        fraction_of_data_target data is chosen.

        Parameters
        ----------
        fraction_of_data_target: The fraction of the filter op
            aims to keep in the dataset.
        df: the relevant data
        col: the column name of the column intended to be operated on
        num_random_samples: if there's more than this many unique values to
            test, randomly sample this many values from the dataset.
            More is better, but slower.
        num_rows_to_execute_on: if the df contains more than this number
            of rows, randomly select this many rows to use as the df.
            More is better, but slower.

        Returns
        ----------
        best_filter_value: parameter setting for the filter op.
        """

        # record the original settings to prevent side effects
        original_hyperparam_settings = self.hyper_parameter_settings

        df, unique_vals = self._sample_df_and_uniqe_values(
            df=df, col=col, max_num_unique_values=num_random_samples,
            max_num_rows=num_rows_to_execute_on)

        # best score is the fraction of data that remains after data is
        # truncated at a given value
        best_score, best_val = 1, 0

        # cycle through unique values for the parameter
        unique_vals = set(df[col])
        for unique_val in unique_vals:

            total = len(df)

            # apply the operation to the sampled df and see what happens
            # this overwrites existing hyperparams. They will need to be
            # reset later
            self.set_hyper_parameter(unique_val)
            filtered_df = self.execute(df)

            # see how many items remain. Score based on how close we are to
            # the correct fraction of data that will remain at the give number
            count = len(filtered_df)
            fraction_of_data_left = count / total
            score = abs(fraction_of_data_left - fraction_of_data_target)

            #  record the closest score
            if score < best_score:
                best_score = score
                best_val = unique_val

        # reset operation to original hyperparamets
        self.hyper_parameter_settings = original_hyperparam_settings
        return best_val

    def find_threshhold_by_diversity(
            self, df, label_col, entity_col,
            num_random_samples=10, num_rows_to_execute_on=2000):
        """
        This function selects a parameter setting for the
        operations, excluding the filter operation.
        The parameter setting that maximizes the
        entropy of the output data is chosen.
        Parameters
        ----------
        df: the relevant data
        label_col: column name of the column of interest in the data
        entity_col: column name of
            the column containing entities in the data
        num_random_samples: if there's more than this many unique values to
            test, randomly sample this many values from the dataset
        num_rows_to_execute_on: if the dataframe contains more than this number
            of rows, randomly select this many rows to use as the dataframe

        Returns
        -------
        best_parameter_value: parameter setting for the operation.
        best_df: the dataframe (possibly filtered depending on
            num_rows_to_execute_on)
            after having the operation applied with the chosen parameter value.
        """

        # record the original settings to prevent side effects
        original_hyperparam_settings = self.hyper_parameter_settings

        df, unique_vals = self._sample_df_and_uniqe_values(
            df=df, col=label_col, max_num_unique_values=num_random_samples,
            max_num_rows=num_rows_to_execute_on)

        best_entropy = 0
        best_parameter_value = 0

        # try each unique value
        # return the one that results in the most entropy
        unique_vals = set(df[label_col])
        for unique_val in unique_vals:

            self.set_hyper_parameter(unique_val)

            output_df = df.groupby(entity_col).apply(self.execute)

            current_entropy = self._entropy_of_a_list(
                list(output_df[label_col]))

            if current_entropy > best_entropy:
                best_entropy = current_entropy
                best_parameter_value = unique_val

        self.hyper_parameter_settings = original_hyperparam_settings
        return best_parameter_value

    def _sample_df_and_uniqe_values(
            self, df, col, max_num_unique_values, max_num_rows):
        """
        Helper methods

        Randomly sample unique values in the passed col in a dataframe so that
        the number of unique_values is the <= max_num_unique_values

        Randomly sample a dataframe so that the number of
        rows is <= max_num_rows

        Parameters
        ----------
        df: Pandas DataFrame
        col: column to base sampling on
        max_num_unique_values: the maximum allowed number of unique values to
            return
        max_num_rows: the maximum allowed number of rows to return

        Returns
        -------
        df: sampled dataframe which is a subset of the original dataframe
        unique_vals: a list of unique values
        """
        unique_vals = set(df[col])

        if len(unique_vals) > max_num_unique_values:
            unique_vals = list(
                random.sample(unique_vals, max_num_unique_values))

        if len(df) > max_num_rows:
            df = df.sample(max_num_rows)

        return (df, unique_vals)

    def _entropy_of_a_list(self, values):
        """
        Calculate the entropy (information) of the list.
        Parameters
        ----------
        values: list of values
        Returns
        ----------
        entropy: the entropy or information in the list.
        """
        counts = Counter(values).values()
        total = float(sum(counts))
        probabilities = [val / total for val in counts]
        entropy = stats.entropy(probabilities)
        return entropy

    def __hash__(self):
        return hash((type(self).__name__, self.column_name))

    def __repr__(self):
        hyper_param_str = ','.join(
            [str(x) for x in list(self.hyper_parameter_settings.values())])

        if len(hyper_param_str) > 0:
            hyper_param_str = '@' + hyper_param_str

        return "%s(%s%s)" % (
            type(self).__name__, self.column_name, hyper_param_str)

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False
