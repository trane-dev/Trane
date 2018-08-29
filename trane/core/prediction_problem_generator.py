import itertools

from ..ops import aggregation_ops as agg_ops
from ..ops import filter_ops, row_ops
from ..ops import transformation_ops as trans_ops
from ..utils.table_meta import TableMeta
from .prediction_problem import PredictionProblem

__all__ = ['PredictionProblemGenerator']


class PredictionProblemGenerator:
    """
    Object for generating prediction problems on data.
    """

    def __init__(self, table_meta, entity_col,
                 label_col, filter_col):
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
        self.label_col = label_col
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

            agg_op_obj = getattr(agg_ops, agg_op_name)(self.label_col)  # noqa
            trans_op_obj = getattr(trans_ops, trans_op_name)(self.label_col)  # noqa
            row_op_obj = getattr(row_ops, row_op_name)(self.label_col)  # noqa
            filter_op_obj = getattr(filter_ops, filter_op_name)(self.filter_col)  # noqa

            operations = [filter_op_obj, row_op_obj, trans_op_obj, agg_op_obj]

            # filter ops are treated differently than other ops
            for op in operations:
                op.auto_set_hyperparams(
                    df=df, label_col=self.label_col,
                    entity_col=self.entity_col, filter_col=self.filter_col)

            problem = PredictionProblem(
                operations=operations, entity_id_col=self.entity_col,
                label_col=self.label_col,
                table_meta=self.table_meta, cutoff_strategy=None)

            if problem.is_valid():
                problems.append(problem)

        return problems

    def ensure_valid_inputs(self):
        """
        TypeChecking for the problem generator entity_col
        and label_col. Errors if types don't match up.
        """
        assert(self.table_meta.get_type(self.entity_col)
               in [TableMeta.TYPE_IDENTIFIER, TableMeta.TYPE_TEXT,
                   TableMeta.TYPE_CATEGORY])
        assert(self.table_meta.get_type(self.label_col)
               in [TableMeta.TYPE_FLOAT, TableMeta.TYPE_INTEGER,
                   TableMeta.TYPE_TEXT])
