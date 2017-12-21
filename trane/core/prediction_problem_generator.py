import json
from .prediction_problem import PredictionProblem
from ..ops import aggregation_ops, row_ops, transformation_ops, filter_ops
from ..utils.table_meta import TableMeta

import logging

__all__ = ['PredictionProblemGenerator']

class PredictionProblemGenerator:
    """
    Automatically generate prediction problems with a sequence of 
    fileter, row, transformation and aggregation operations.
    """
    def __init__(self, table_meta, entity_id_column=None, label_generating_column=None, time_column=None):
        """
        Args:
            label_generating_column: column to operate over. If None, use all TYPE_VALUE columns.
            entity_id_column: the column with entity id's. If None, use all TYPE_IDENTIFIER columns.
            time_column: the name of the column containing time information. If None, use TYPE_TIME columns.
        Returns:
            None
        """
        if isinstance(table_meta, list):
            table_meta = TableMeta(table_meta)
        assert isinstance(table_meta, TableMeta)
        self.table_meta = table_meta
        
        def select_column_with_type(column_name, data_type):
            if column_name:
                assert(self.table_meta.get_type(column_name) == data_type)
                return [column_name]
            else:
                ret = []
                for column in self.table_meta.get_columns():
                    if self.table_meta.get_type(column) == data_type:
                        ret.append(column)
                return ret
        
        self.entity_id_columns = select_column_with_type(entity_id_column, TableMeta.TYPE_IDENTIFIER)
        self.label_generating_columns = select_column_with_type(label_generating_column, TableMeta.TYPE_VALUE)
        self.time_columns = select_column_with_type(time_column, TableMeta.TYPE_TIME)
        
        logging.info("Generate labels on [%s]" % ', '.join(self.label_generating_columns))
        logging.info("Entites [%s]" % ', '.join(self.entity_id_columns))
        logging.info("Time [%s]" % ', '.join(self.time_columns))

    def generate(self):
        """
        Generate prediction problems.
        yeilds:
            PredictionProblem
        """
        #NOTE tricks for less indents
        def iter_over_ops():
            for aggregation_op_name in aggregation_ops.AGGREGATION_OPS:
                for transformation_op_name in transformation_ops.TRANSFORMATION_OPS:
                    for row_op_name in row_ops.ROW_OPS:
                        for filter_op_name in filter_ops.FILTER_OPS:
                            yield aggregation_op_name, transformation_op_name, \
                                row_op_name, filter_op_name                            
        def iter_over_column():
            for entity_id_column in self.entity_id_columns:
                for time_column in self.time_columns:
                    for operate_column in self.label_generating_columns:
                        for filter_column in self.table_meta.get_columns():
                            yield entity_id_column, time_column, \
                                operate_column, filter_column


        for ops in iter_over_ops():
            for columns in iter_over_column():
                aggregation_op_name, transformation_op_name, \
                    row_op_name, filter_op_name = ops
                entity_id_column, time_column, \
                    operate_column, filter_column = columns
                    
                aggregation_op_obj = getattr(aggregation_ops, aggregation_op_name)(operate_column)    
                transformation_op_obj = getattr(transformation_ops, transformation_op_name)(operate_column)
                row_op_obj = getattr(row_ops, row_op_name)(operate_column)
                filter_op_obj = getattr(filter_ops, filter_op_name)(filter_column)

                prediction_problem = PredictionProblem(self.table_meta,
                    [filter_op_obj, row_op_obj, transformation_op_obj, aggregation_op_obj],
                    operate_column, entity_id_column, time_column)
                if not prediction_problem.valid:
                    continue
                yield prediction_problem
