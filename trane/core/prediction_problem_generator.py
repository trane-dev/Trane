import itertools
import random

from collections import Counter
from scipy import stats

from ..ops.filter_ops import FilterOpBase

from ..ops import aggregation_ops as agg_ops
from ..ops import filter_ops
from ..ops import row_ops
from ..ops import transformation_ops as trans_ops
from ..utils.table_meta import TableMeta
from .prediction_problem import PredictionProblem

__all__ = ['PredictionProblemGenerator']


class PredictionProblemGenerator:
    """
    Object for generating prediction problems on data.
    """

    def __init__(self, table_meta, entity_id_col,
                 label_generating_col, time_col, filter_col):
        """
        Parameters
        ----------
        table_meta: TableMeta object. Contains
            meta information about the data
        entity_id_col: column name of
            the column containing entities in the data
        label_generating_col: column name of the
            column of interest in the data
        time_col: column name of the column
            containing time information in the data
        filter_col: column name of the column
            to be filtered over

        Returns
        -------
        None
        """
        self.table_meta = table_meta
        self.entity_id_col = entity_id_col
        self.label_generating_col = label_generating_col
        self.time_col = time_col
        self.filter_col = filter_col
        self.ensure_valid_inputs()

    def generate(self, df):
        """
        Generate the prediction problems. The prediction problems operations
        hyper parameters are also set.

        Parameters
        ----------
        df: the data to be parsed

        Returns
        -------
        problems: a list of Prediction Problem objects.
        """

        op_types = [agg_ops.AGGREGATION_OPS, trans_ops.TRANSFORMATION_OPS,
                    row_ops.ROW_OPS, filter_ops.FILTER_OPS]

        # a list of problems that will eventually be returned
        problems = []

        def iter_over_ops():
            for ag, trans, row, filter in itertools.product(*op_types):
                yield ag, trans, row, filter

        for ops_combo in iter_over_ops():
            # for filter_col in self.table_meta.get_columns():
            agg_op_name, trans_op_name, row_op_name, filter_op_name = ops_combo

            agg_op_obj = getattr(agg_ops, agg_op_name)(self.label_generating_col)  # noqa
            trans_op_obj = getattr(trans_ops, trans_op_name)(self.label_generating_col)  # noqa
            row_op_obj = getattr(row_ops, row_op_name)(self.label_generating_col)  # noqa
            filter_op_obj = getattr(filter_ops, filter_op_name)(self.filter_col)  # noqa

            operations = [filter_op_obj, row_op_obj, trans_op_obj, agg_op_obj]

            # filter ops are treated differently than other ops
            for op in operations:
                if issubclass(type(op), FilterOpBase):
                    self._tune_and_set_filter_op_hyperparam(
                        filter_op=filter_op_obj, df=df, col=self.filter_col)
                else:
                    self._select_by_diversity(
                        op=op, df=df,
                        label_generating_col=self.label_generating_col,
                        entity_id_col=self.entity_id_col)

            problem = PredictionProblem(
                operations=operations, entity_id_col=self.entity_id_col,
                time_col=self.time_col, table_meta=self.table_meta,
                cutoff_strategy=None)

            if problem.is_valid():
                problems.append(problem)

        return problems

    def ensure_valid_inputs(self):
        """
        TypeChecking for the problem generator entity_id_col
        and label_generating_col. Errors if types don't match up.
        """
        assert(self.table_meta.get_type(self.entity_id_col)
               in [TableMeta.TYPE_IDENTIFIER, TableMeta.TYPE_TEXT,
                   TableMeta.TYPE_CATEGORY])
        assert(self.table_meta.get_type(self.label_generating_col)
               in [TableMeta.TYPE_FLOAT, TableMeta.TYPE_INTEGER,
                   TableMeta.TYPE_TEXT])
        assert(self.table_meta.get_type(self.time_col)
               in [TableMeta.TYPE_TIME, TableMeta.TYPE_INTEGER])

    def _tune_and_set_filter_op_hyperparam(
            self, filter_op, df, col, fraction_of_data_target=0.8):
        filter_hyperparam = self.select_by_remaining(
            fraction_of_data_target=fraction_of_data_target,
            df=df, op=filter_op, col=col)

        filter_op.set_hyper_parameter(filter_hyperparam)

    def select_by_remaining(
            self, fraction_of_data_target, df, op, col,
            num_random_samples=10, num_rows_to_execute_on=2000):
        """
        This function selects a parameter setting for the
        passed op. The parameter setting that comes closest to the
        fraction_of_data_target data is chosen.

        Parameters
        ----------
        fraction_of_data_target: The fraction of the filter op
            aims to keep in the dataset.
        df: the relevant data
        op: the filter op
        col: the column name of the column intended to be operated on
        num_random_samples: if there's more than this many unique values to
            test, randomly sample this many values from the dataset
        num_rows_to_execute_on: if the df contains more than this number
            of rows, randomly select this many rows to use as the df

        Returns
        ----------
        best_filter_value: parameter setting for the filter op.
        """

        # if no params are required, return None
        if len(op.REQUIRED_PARAMETERS) == 0:
            return None

        # record the original settings to prevent side effects
        original_hyperparam_settings = op.hyper_parameter_settings

        unique_vals = set(df[col])

        # randomly select samples from the unique values
        if len(unique_vals) > num_random_samples:
            unique_vals = list(
                random.sample(unique_vals, num_random_samples))

        # pare down the dataframe to just the length desired
        if len(df) > num_rows_to_execute_on:
            df = df.sample(num_rows_to_execute_on)

        best = 1
        best_val = 0
        # cycle through unique values for the parameter
        for unique_val in unique_vals:

            total = len(df)

            # apply the operation to the sampled df and see what happens
            # this overwrites existing hyperparams. They will need to be
            # reset later
            op.set_hyper_parameter(unique_val)
            filtered_df = op.execute(df)

            # see how many items remain. Score based on how close we are to
            # the correct fraction of data that will remain at the give number
            count = len(filtered_df)
            fraction_of_data_left = count / total
            score = abs(fraction_of_data_left - fraction_of_data_target)

            #  record the closest score
            if score < best:
                best = score
                best_val = unique_val

        # reset operation to original hyperparamets
        op.hyper_parameter_settings = original_hyperparam_settings
        return best_val

    def _select_by_diversity(
            self, op, df, label_generating_col, entity_id_col,
            num_random_samples=10, num_rows_to_execute_on=2000):
        """
        This function selects a parameter setting for the
        operations, excluding the filter operation.
        The parameter setting that maximizes the
        entropy of the output data is chosen.
        Parameters
        ----------
        df: the relevant data
        op: the filter operation
        label_generating_col: column name of the
            column of interest in the data
        entity_id_col: column name of
            the column containing entities in the data
        num_random_samples: if there's more than this many unique values to
            test, randomly sample this many values from the dataset
        num_rows_to_execute_on: if the dataframe contains more than this number
            of
            rows, randomly select this many rows to use as the dataframe

        Returns
        -------
        best_parameter_value: parameter setting for the operation.
        best_df: the dataframe (possibly filtered depending on
            num_rows_to_execute_on)
            after having the operation applied with the chosen parameter value.
        """

        # If the operator has no required parameters, return None
        if len(op.REQUIRED_PARAMETERS) == 0:
            return None, df

        df = df.copy()

        unique_vals = set(df[label_generating_col])

        # randomly sample from the unique parameters
        if len(unique_vals) > num_random_samples:
            unique_vals = list(random.sample(
                unique_vals, num_random_samples))

        # randomly sample the dataframe to be the right size
        if len(df) > num_rows_to_execute_on:
            df = df.sample(num_rows_to_execute_on)

        best_entropy = 0
        best_parameter_value = 0

        # try each unique value
        # return the one that results in the most entropy
        for unique_val in unique_vals:

            op.set_hyper_parameter(unique_val)

            output_df = df.groupby(entity_id_col).apply(op.execute)

            current_entropy = self.entropy_of_a_list(
                list(output_df[label_generating_col]))

            if current_entropy > best_entropy:
                best_entropy = current_entropy
                best_parameter_value = unique_val

        return best_parameter_value

    def entropy_of_a_list(self, values):
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
