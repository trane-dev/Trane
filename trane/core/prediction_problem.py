import pandas as pd
import json
from ..utils.table_meta import TableMeta
from ..ops.op_saver import *
from dateutil import parser

import sys
import logging

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
			print("temp_meta.get_type(label_generating_column): {}".format(temp_meta.get_type(label_generating_column)))
			label_generating_column_order_of_types.append(temp_meta.get_type(label_generating_column))
		
		self.filter_column_order_of_types = filter_column_order_of_types
		self.label_generating_column_order_of_types = label_generating_column_order_of_types

		return (True, filter_column_order_of_types, label_generating_column_order_of_types)

	def set_thresholds(self, table_meta):
		for op in self.operations:
			op.set_thresholds(table_meta)

	def execute(self, dataframe, time_column, cutoff_time):
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

		precutoff_time_execution_result = dataframe[
			dataframe[time_column] < cutoff_time]
		all_time_execution_result = dataframe

		for operation in self.operations:

			continue_executing_on_precutoff_df = True
			continue_executing_on_all_data_df = True

			if len(precutoff_time_execution_result) == 0:
				continue_executing_on_precutoff_df = False

			if len(all_time_execution_result) == 0:
				continue_executing_on_all_data_df = False

			if continue_executing_on_precutoff_df:
				precutoff_time_execution_result = operation.execute(
					precutoff_time_execution_result)
			if continue_executing_on_all_data_df:
				all_time_execution_result = operation.execute(all_time_execution_result)
				

		return precutoff_time_execution_result, all_time_execution_result

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
