import itertools

from ..ops import aggregation_ops as agg_ops
from ..ops import filter_ops
from ..utils.table_meta import TableMeta
from .prediction_problem import PredictionProblem

__all__ = ['PredictionProblemGenerator']


class PredictionProblemGenerator:
    """
    Object for generating prediction problems on data.
    """

    def __init__(self, table_meta, entity_col, time_col):
        """
        Parameters
        ----------
        table_meta: TableMeta object. Contains
            meta information about the data
        entity_col: column name of
            the column containing entities in the data
        label_col: column name of the
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
        self.entity_col = entity_col
        self.time_col = time_col
        self.ensure_valid_inputs()

    def generate(self):
        """
        Generate the prediction problems. The prediction problems operations
        hyper parameters are also set.

        Parameters
        ----------

        Returns
        -------
        problems: a list of Prediction Problem objects.
        """

        # a list of problems that will eventually be returned
        problems = []

        def iter_over_ops():
            for ag, filter in itertools.product(
                    agg_ops.AGGREGATION_OPS, filter_ops.FILTER_OPS):

                filter_cols = [None] if filter == "AllFilterOp" else self.table_meta.get_columns()
                ag_cols = [None] if ag == "CountAggregationOp" else self.table_meta.get_columns()

                for filter_col, ag_col in itertools.product(filter_cols, ag_cols):
                    if filter_col != self.entity_col and ag_col != self.entity_col:
                        yield ag_col, filter_col, ag, filter

        all_attempts = 0
        success_attempts = 0
        for op_col_combo in iter_over_ops():
            print("\rSuccess/Attempt = {}/{}".format(success_attempts, all_attempts), end="")
            all_attempts += 1
            ag_col, filter_col, agg_op_name, filter_op_name = op_col_combo

            agg_op_obj = getattr(agg_ops, agg_op_name)(ag_col)  # noqa
            filter_op_obj = getattr(filter_ops, filter_op_name)(filter_col)  # noqa

            operations = [filter_op_obj, agg_op_obj]

            problem = PredictionProblem(
                operations=operations, entity_col=self.entity_col, time_col=self.time_col,
                table_meta=self.table_meta, cutoff_strategy=None)

            if problem.is_valid():
                problems.append(problem)
                success_attempts += 1
        print("\rSuccess/Attempt = {}/{}".format(success_attempts, all_attempts))
        return problems

    def ensure_valid_inputs(self):
        """
        TypeChecking for the problem generator entity_col
        and label_col. Errors if types don't match up.
        """
        assert(self.table_meta.get_type(self.entity_col)
               in [TableMeta.TYPE_IDENTIFIER, TableMeta.TYPE_TEXT,
                   TableMeta.TYPE_CATEGORY])
