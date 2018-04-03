import pandas as pd
import json
from ..utils.table_meta import TableMeta
from ..ops.op_saver import *
from dateutil import parser
from collections import Counter
import sys
import logging
from scipy import stats

__all__ = ['PredictionProblem']


class PredictionProblem:

    """Prediction Problem is made up of a list of Operations. The list of operations delineate
        the order the operations will be applied in.

    """

    def __init__(self, operations):
        """Args:
            (List) Operations: a list of operations (class Operation) that define the
                order in which operations should take place.
            (String) label_generating_column: the column of interest. This column
                will be solely used for performing operations against.
            (String) entity_id_column: the column with entity id's.
            (String) time_column: the name of the column containing time information.

        Returns:
            None

        """
        self.operations = operations

    def op_type_check(self, table_meta):
        temp_meta = table_meta.copy()
        for op in self.operations:
            temp_meta = op.op_type_check(temp_meta)
            if not temp_meta:
                return False
        return True

    def set_hyper_parameters(self, hyper_parameters):
        for idx, op in enumerate(self.operations):
        	hyper_parameter = hyper_parameters[idx]
        	op.set_hyper_parameter(hyper_parameter)

    def generate_and_set_hyper_parameters(self, dataframe, label_generating_column, filter_column):
        hyper_parameters = []
                

        FRACTION_OF_DATA_TARGET = 0.8
        column_data = dataframe[filter_column]
        unique_filter_values = set(column_data)
        hyper_parameters.append(
            select_by_remaining(
                FRACTION_OF_DATA_TARGET, unique_filter_values, 
                dataframe, self.operations[0]
                )
        )
        
        for operation in self.operations[1:]:
            column_data = dataframe[label_generating_column]
            unique_parameter_values = set(column_data)
            hyper_parameters.append(
                select_by_diversity(
                    unique_parameter_values, dataframe, operation, label_generating_column)
                )
        
        self.set_hyper_parameters(hyper_parameters)
        return hyper_parameters

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
        dataframe_precutoff_time = dataframe[
            dataframe[time_column] < cutoff_time]
        dataframe_all_data = dataframe


        for operation in self.operations:

            continue_executing_on_precutoff_df = True
            continue_executing_on_all_data_df = True

            if len(dataframe_precutoff_time) == 0:
                continue_executing_on_precutoff_df = False

            if len(dataframe_all_data) == 0:
                continue_executing_on_all_data_df = False

            if continue_executing_on_precutoff_df:
                dataframe_precutoff_time = operation.execute(
                    dataframe_precutoff_time)
            if continue_executing_on_all_data_df:
                dataframe_all_data = operation.execute(dataframe_all_data)


        return dataframe_precutoff_time, dataframe_all_data

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
            {"operations": [json.loads(op_to_json(op)) for op in self.operations]})

    def from_json(json_data):
        data = json.loads(json_data)
        operations = [op_from_json(json.dumps(item))
                      for item in data['operations']]
        return PredictionProblem(operations)

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