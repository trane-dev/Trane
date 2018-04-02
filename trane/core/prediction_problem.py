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
        dataframe_precutoff_time = dataframe[
            dataframe[time_column] < cutoff_time]
        dataframe_all_data = dataframe

        # logging.debug("Dataframe before any execution: {}".format(dataframe_all_data))

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
                # logging.debug("Before execution of operation: {}, the data in the column of interest is: \n {} \n".format(operation, dataframe_all_data[operation.column_name]))

                dataframe_all_data = operation.execute(dataframe_all_data)
                
                # logging.debug("After execution of operation: {}, the data in the column of interest is: \n {} \n".format(operation, dataframe_all_data[operation.column_name]))

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
