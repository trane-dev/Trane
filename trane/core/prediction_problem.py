import pandas as pd
import json
from ..utils.table_meta import TableMeta
from ..ops.op_saver import *
from dateutil import parser
from collections import Counter
import numpy as np
import sys
import logging
from scipy import stats

__all__ = ['PredictionProblem']


class PredictionProblem:


	"""Prediction Problem is made up of a list of Operations. The list of operations delineate
		the order the operations will be applied in.

	"""

	def __init__(self, operations):
		self.operations = operations
		self.filter_column_order_of_types = None
		self.label_generating_column_order_of_types = None

	def is_valid_prediction_problem(self, table_meta, filter_column, label_generating_column):
		temp_meta = table_meta.copy()

		filter_column_order_of_types = [table_meta.get_type(filter_column)]
		label_generating_column_order_of_types = [table_meta.get_type(label_generating_column)]
		
		filter_op = self.operations[0]
		temp_meta = filter_op.op_type_check(temp_meta)
		if not temp_meta:
			return False, None, None
		filter_column_order_of_types.append(temp_meta.get_type(filter_column))

		for op in self.operations[1:]:
			temp_meta = op.op_type_check(temp_meta)
			if not temp_meta:
				return False, None, None
			label_generating_column_order_of_types.append(temp_meta.get_type(label_generating_column))
		
		self.filter_column_order_of_types = filter_column_order_of_types
		self.label_generating_column_order_of_types = label_generating_column_order_of_types

		return (True, filter_column_order_of_types, label_generating_column_order_of_types)

	def set_hyper_parameters(self, hyper_parameters):
		for idx, op in enumerate(self.operations):
			hyper_parameter = hyper_parameters[idx]
			op.set_hyper_parameter(hyper_parameter)

	def generate_and_set_hyper_parameters(self, dataframe, label_generating_column, filter_column,
											hyper_parameter_memo_table):
		hyper_parameters = []
		

		FRACTION_OF_DATA_TARGET = 0.8
		column_data = dataframe[filter_column]
		unique_filter_values = set(column_data)
		operation = self.operations[0]
		operation_hash = hash(operation)
		if operation_hash in hyper_parameter_memo_table:	
			value = hyper_parameter_memo_table[operation_hash]
		else:
			value = select_by_remaining(
					FRACTION_OF_DATA_TARGET, unique_filter_values, 
					dataframe, operation
					)
			hyper_parameter_memo_table[operation_hash] = value
		
		hyper_parameters.append(value)
		
		for operation in self.operations[1:]:
			operation_hash = hash(operation)
			if operation_hash in hyper_parameter_memo_table:
				value = hyper_parameter_memo_table[operation_hash]
			else:
				column_data = dataframe[label_generating_column]
				unique_parameter_values = set(column_data)
				value = select_by_diversity(unique_parameter_values, 
						dataframe, operation, label_generating_column)
				hyper_parameter_memo_table[operation_hash] = value
			
			hyper_parameters.append(value)
					
		self.set_hyper_parameters(hyper_parameters)
		return hyper_parameters

	def execute(self, dataframe, time_column, label_cutoff_time, 
		filter_column_order_of_types, label_generating_column_order_of_types):
		"""This function executes all the operations on the dataframe and returns the output. The output
			should be structured as a single label/value per the Trane documentation.
			See paper: "What would a data scientist ask? Automatically formulating and solving predicton
			problems."

		Args:
			(Pandas DataFrame): the dataframe containing the data we wish to analyze.

		Returns:
			(Boolean/Float): The Label/Value of the prediction problem's formulation when applied to the data.

		"""
		dataframe = dataframe.sort_values(by = time_column)
		dataframe = dataframe.copy()

		pre_label_cutoff_time_execution_result = dataframe[
			dataframe[time_column] < label_cutoff_time]
		all_data_execution_result = dataframe

		continue_executing_on_precutoff_df = True
		continue_executing_on_all_data_df = True

		for idx, operation in enumerate(self.operations):
			
			logging.debug("Beginning execution of operation: {}".format(operation))

			if len(pre_label_cutoff_time_execution_result) == 0:
				continue_executing_on_precutoff_df = False

			if len(all_data_execution_result) == 0:
				continue_executing_on_all_data_df = False


			if continue_executing_on_all_data_df:
				single_piece_of_data = all_data_execution_result.iloc[0][operation.column_name]
				
				if idx == 0:
				  check_type(filter_column_order_of_types[idx], single_piece_of_data)
				elif idx > 0:
				  check_type(label_generating_column_order_of_types[idx - 1], single_piece_of_data)

				all_data_execution_result = operation.execute(all_data_execution_result)

				single_piece_of_data = all_data_execution_result.iloc[0][operation.column_name]

				if idx == 0:
				  check_type(filter_column_order_of_types[idx + 1], single_piece_of_data)
				elif idx > 0:
				  check_type(label_generating_column_order_of_types[idx], single_piece_of_data)

			if continue_executing_on_precutoff_df:
				pre_label_cutoff_time_execution_result = operation.execute(
					pre_label_cutoff_time_execution_result)

		return pre_label_cutoff_time_execution_result, all_data_execution_result

	def __str__(self):
		"""Args:
			None

		Returns:
			A natural language text describing the prediction problem.

		"""
		description = ""
		last_op_idx = len(self.operations) - 1
		for idx, operation in enumerate(self.operations):
			description += str(operation)
			if idx != last_op_idx:
				description += "->"
		return description

	def to_json(self):
		return json.dumps(
			{"operations": [json.loads(op_to_json(op)) for op in self.operations],
			"filter_column_order_of_types": self.filter_column_order_of_types,
			"label_generating_column_order_of_types": self.label_generating_column_order_of_types})

	def from_json(json_data):
		data = json.loads(json_data)
		operations = [op_from_json(json.dumps(item))
					  for item in data['operations']]
		problem = PredictionProblem(operations)
		problem.filter_column_order_of_types = data['filter_column_order_of_types']
		problem.label_generating_column_order_of_types = data['label_generating_column_order_of_types']
		return problem

	def __eq__(self, other):
		"""Overrides the default implementation"""
		if isinstance(self, other.__class__):
			return self.__dict__ == other.__dict__
		return False
def select_by_remaining(fraction_of_data_target, unique_filter_values, dataframe, operation):
	if len(operation.REQUIRED_PARAMETERS) == 0:
		return None
	else:
		best = 1
		best_filter_value = 0
		for unique_filter_value in unique_filter_values:
			total = len(dataframe)
			
			operation.set_hyper_parameter(unique_filter_value)
			
			filtered_df = operation.execute(dataframe)
			count = len(filtered_df)

			fraction_of_data_left = count / total
			
			score = abs(fraction_of_data_left - fraction_of_data_target)
			if score < best:
				best = score
				best_filter_value = unique_filter_value
		return best_filter_value

def select_by_diversity(unique_parameter_values, dataframe, operation, label_generating_column):
	if len(operation.REQUIRED_PARAMETERS) == 0:
		return None
	else:
		best = 0
		best_parameter_value = 0
		for unique_parameter_value in unique_parameter_values:
			
			operation.set_hyper_parameter(unique_parameter_value)

			resulting_df = operation.execute(dataframe)
			entropy = entropy_of_a_list(list(resulting_df[label_generating_column]))
			if entropy > best:
				best = entropy
				best_parameter_value = unique_parameter_value
		return best_parameter_value

def entropy_of_a_list(values):
	counts = Counter(values).values()
	total = float(sum(counts))
	probabilities = [val/total for val in counts]
	entropy = stats.entropy(probabilities)
	return entropy

def check_type(expected_type, actual_data):
	logging.debug("Beginning check type. Expected type is: {}, Actual data is: {}, Actual type is: {}".format(expected_type, actual_data, type(actual_data)))
	
	if expected_type == TableMeta.TYPE_CATEGORY:
		allowed_types = [bool, int, str, float]
		assert(type(actual_data) in allowed_types)

	elif expected_type == TableMeta.TYPE_BOOL:
		allowed_types = [bool, np.bool_]
		assert(type(actual_data) in allowed_types)        
	
	elif expected_type == TableMeta.TYPE_ORDERED:
		allowed_types = [bool, int, str, float]
		assert(type(actual_data) in allowed_types)
	
	elif expected_type == TableMeta.TYPE_TEXT:
		allowed_types = [str]           
		assert(type(actual_data) in allowed_types)
	
	elif expected_type == TableMeta.TYPE_INTEGER:
		allowed_types = [int, np.int64]
		assert(type(actual_data) in allowed_types)

	elif expected_type == TableMeta.TYPE_FLOAT:
		allowed_types = [float, np.float64, np.float32]
		assert(type(actual_data) in allowed_types)

	elif expected_type == TableMeta.TYPE_TIME:
		allowed_types = [bool, int, str, float]
		assert(type(actual_data) in allowed_types)

	elif expected_type == TableMeta.TYPE_IDENTIFIER:
		allowed_types = [int, str, float]
		assert(type(actual_data) in allowed_types)
	
	else:
		logging.critical('Check_type function received an unexpected type.')
