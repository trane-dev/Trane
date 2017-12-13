import json
from PredictionProblem import PredictionProblem
from AggregationOperation import AggregationOperation
from RowOperation import RowOperation
from TransformationOperation import TransformationOperation
from FilterOperation import FilterOperation
from TableMeta import TableMeta

import AggregationOperationModule as ag
import RowOperationModule as ro
import TransformationOperationModule as tr
import FilterOperationModule as fi

import logging

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
		possible_row_operation_names = ro.possible_operations.keys()
		possible_filter_operation_names = fi.possible_operations.keys()

		possible_aggregation_operation_names = ag.possible_operations.keys()
		possible_transformation_operation_names = tr.possible_operations.keys()

		prediction_problems = []
		
		for entity_id_column in self.entity_id_columns:
			for time_column in self.time_columns:
				for aggregation_operation_name in possible_aggregation_operation_names:
					for transformation_operation_name in possible_transformation_operation_names:
						for row_operation_name in possible_row_operation_names:
							for filter_operation_name in possible_filter_operation_names:
								for column_to_operate_over  in self.label_generating_columns:
									for column_to_filter_over in self.table_meta.get_columns():

										aggregation_operation = AggregationOperation(
											column_to_operate_over, aggregation_operation_name)
										transformation_operation = TransformationOperation(
											column_to_operate_over, transformation_operation_name)
										row_operation = RowOperation(column_to_operate_over, row_operation_name)
										filter_operation = FilterOperation(column_to_operate_over, filter_operation_name)

										prediction_problem = PredictionProblem(self.table_meta.copy(),
											[filter_operation, row_operation, 
											transformation_operation, aggregation_operation],
											column_to_operate_over, entity_id_column, time_column)
										if not prediction_problem.valid:
											continue
										yield prediction_problem

if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
						datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)

	table_meta = open('../../test_datasets/taxi_meta.json').read()
	table_meta = json.loads(table_meta)
	gen = PredictionProblemGenerator(table_meta, entity_id_column='taxi_id')
	for problem in gen.generate():
		print(str(problem))
	
	# pred_problems = gen.generate()
	# print([str(pred_problem) for pred_problem in pred_problems][0])
	# # print(pred_problems[25])
	# # print(pred_problems[25].execute(df))
	# # print(len([str(pred_problem) for pred_problem in pred_problems]))
