import json
from .prediction_problem import PredictionProblem
from ..ops import aggregation_ops, row_ops, transformation_ops, filter_ops
from ..utils.table_meta import TableMeta

import logging

__all__ = ['PredictionProblemGenerator']

class PredictionProblemGenerator:

	"""
	Args:
		(String) label_generating_column: the column of interest. This column
			will be solely used for performing operations against.
		(String) entity_id_column: the column with entity id's.
		(String) time_column: the name of the column containing time information.
	Returns:
		None
	"""
	def __init__(self, table_meta, entity_id_column=None, label_generating_column=None, time_column=None):
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

	"""
	Inspired by Ben Shreck's MIT MEng Thesis pg. 57, named LittleTrane.
		LittleTrane imposes constraints on PredictionProblem definitions.
		Beginning with a simple implementation.
		FilterOp - RowOp - TransOp - AggOp
	We may expand the possibilities later, but for now we start simple.
	Args:
		None
	Returns:
		(List): A list of prediction problems.
	"""
	#possible_operations for each class is a dictionary mapping a string name to an Operation of that class.
	def generate(self):
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

if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
						datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)

	table_meta = open('../../test_datasets/taxi_meta.json').read()
	table_meta = json.loads(table_meta)
	gen = PredictionProblemGenerator(table_meta, entity_id_column='taxi_id')
	cnt = 0
	for problem in gen.generate():
		print(str(problem))
		print(problem.generate_nl_description())
		cnt += 1
	logging.info("Generate %d problems." % cnt)
	
	# pred_problems = gen.generate()
	# print([str(pred_problem) for pred_problem in pred_problems][0])
	# # print(pred_problems[25])
	# # print(pred_problems[25].execute(df))
	# # print(len([str(pred_problem) for pred_problem in pred_problems]))
