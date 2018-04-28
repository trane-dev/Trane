import logging

from ..ops import aggregation_ops, filter_ops, row_ops, transformation_ops
from ..utils.table_meta import TableMeta
from .prediction_problem import PredictionProblem

__all__ = ['PredictionProblemGenerator']


class PredictionProblemGenerator:
    """
    Object for generating prediction problems on data.
    """

    def __init__(self, table_meta, entity_id_column,
                 label_generating_column, time_column, filter_column):
        """
        Parameters
        ----------
        table_meta: TableMeta object. Contains
            meta information about the data
        entity_id_column: column name of
            the column containing entities in the data
        label_generating_column: column name of the
            column of interest in the data
        time_column: column name of the column
            containing time information in the data
        filter_column: column name of the column
            to be filtered over

        Returns
        ----------
        None
        """
        self.table_meta = table_meta
        self.entity_id_column = entity_id_column
        self.label_generating_column = label_generating_column
        self.time_column = time_column
        self.filter_column = filter_column
        self.ensure_valid_inputs()

    def generate(self, dataframe):
        """
        Generate the prediction problems. The prediction problems operations
        hyper parameters are also set.

        Parameters
        ----------
        dataframe: the data to be parsed

        Returns
        ----------
        problems: a list of Prediction Problem objects.
        """
        def iter_over_ops():
            for aggregation_op_name in aggregation_ops.AGGREGATION_OPS:
                for transformation_op_name in transformation_ops.TRANSFORMATION_OPS:
                    for row_op_name in row_ops.ROW_OPS:
                        for filter_op_name in filter_ops.FILTER_OPS:
                            yield aggregation_op_name, transformation_op_name, \
                                row_op_name, filter_op_name

        problems = []
        for ops in iter_over_ops():
            # for filter_column in self.table_meta.get_columns():
            aggregation_op_name, transformation_op_name, \
                row_op_name, filter_op_name = ops

            aggregation_op_obj = getattr(aggregation_ops, aggregation_op_name)(
                self.label_generating_column)
            transformation_op_obj = getattr(transformation_ops, transformation_op_name)(
                self.label_generating_column)
            row_op_obj = getattr(row_ops, row_op_name)(
                self.label_generating_column)
            filter_op_obj = getattr(
                filter_ops, filter_op_name)(self.filter_column)

            operations = [
                filter_op_obj,
                row_op_obj,
                transformation_op_obj,
                aggregation_op_obj]

            prediction_problem = PredictionProblem(operations)

            logging.debug("prediction problem generated, now checking validity...")

            (is_valid_prediction_problem, filter_column_order_of_types,
                label_generating_column_order_of_types) = \
                prediction_problem.is_valid_prediction_problem(
                self.table_meta, self.filter_column,
                self.label_generating_column)
            if not is_valid_prediction_problem:
                logging.debug("invalid prediction problem")
                continue

            logging.debug("valid prediction problem, now generating and setting hyper parameters...")

            prediction_problem.generate_and_set_hyper_parameters(dataframe,
                                                                 self.label_generating_column,
                                                                 self.filter_column,
                                                                 self.entity_id_column)

            logging.debug(
                "valid problem with parameters generated: {} \n".format(prediction_problem))

            problems.append(prediction_problem)
        return problems

    def ensure_valid_inputs(self):
        assert(self.table_meta.get_type(self.entity_id_column)
               in [TableMeta.TYPE_IDENTIFIER, TableMeta.TYPE_TEXT,
                   TableMeta.TYPE_CATEGORY])
        assert(self.table_meta.get_type(self.label_generating_column)
               in [TableMeta.TYPE_FLOAT, TableMeta.TYPE_INTEGER])
        assert(self.table_meta.get_type(self.time_column)
               in [TableMeta.TYPE_TIME, TableMeta.TYPE_INTEGER])
