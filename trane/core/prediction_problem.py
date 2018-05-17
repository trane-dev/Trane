import json
import logging
import random
import time
from collections import Counter

import numpy as np
from scipy import stats

from ..ops.op_saver import op_from_json, op_to_json
from ..utils.table_meta import TableMeta

__all__ = ['PredictionProblem']


class PredictionProblem:

    """
    Prediction Problem is made up of a series of Operations. It also contains
    information about the types expected as the input and output of
    each operation.
    """

    def __init__(self, operations):
        """
        Parameters
        ----------
        operations: list of Operations of type op_base

        Returns
        ----------
        None
        """
        self.operations = operations
        self.filter_column_order_of_types = None
        self.label_generating_column_order_of_types = None

    def is_valid_prediction_problem(
            self, table_meta, filter_column, label_generating_column):
        """
        Checks that a prediction problem is valid. Ensures the input output
        types of operations match.

        Parameters
        ----------
        table_meta: TableMeta object. Contains
            meta information about the data
        filter_column: column name of the column
            to be filtered over
        label_generating_column: column name of the
            column of interest in the data

        Returns
        ----------
        is_valid: True/False if problem is valid
        filter_column_order_of_types: a list containing the types expected in the
            sequence of operations on the filter column
        label_generating_column_order_of_types: a list containing the types expected in the
            sequence of operations on the label generating column
        """
        logging.debug("Performing is_valid_prediction_problem...")

        temp_meta = table_meta.copy()

        filter_column_order_of_types = [table_meta.get_type(filter_column)]
        label_generating_column_order_of_types = [
            table_meta.get_type(label_generating_column)]

        filter_op = self.operations[0]
        temp_meta = filter_op.op_type_check(temp_meta)
        if not temp_meta:
            return False, None, None
        filter_column_order_of_types.append(temp_meta.get_type(filter_column))

        for op in self.operations[1:]:
            temp_meta = op.op_type_check(temp_meta)
            if not temp_meta:
                return False, None, None
            label_generating_column_order_of_types.append(
                temp_meta.get_type(label_generating_column))

        self.filter_column_order_of_types = filter_column_order_of_types
        self.label_generating_column_order_of_types = label_generating_column_order_of_types  # noqa

        return (True, filter_column_order_of_types,
                label_generating_column_order_of_types)

    def set_hyper_parameters(self, hyper_parameters):
        """

        Parameters
        ----------
        hyper_parameters: hyper parameter settings
            to be set on the operations

        Returns
        ----------
        None
        """
        for idx, op in enumerate(self.operations):
            hyper_parameter = hyper_parameters[idx]
            op.set_hyper_parameter(hyper_parameter)

    def generate_and_set_hyper_parameters(self, dataframe, label_generating_column,
                                          filter_column, entity_id_column):
        """
        Generate then set hyper parameters for the operations in this prediction problem.

        Parameters
        ----------
        dataframe: data
        label_generating_column: column name of the
            column of interest in the data
        filter_column: column name of the column
            to be filtered over
        entity_id_column: column name of
            the column containing entities in the data

        Returns
        ----------
        hyper_parameters: list of hyper parameter settings
        """
        logging.debug("Performing generate_and_set_hyper_parameters...")

        hyper_parameters = []

        dataframe = dataframe.copy()
        FRACTION_OF_DATA_TARGET = 0.8
        operation = self.operations[0]

        value = select_by_remaining(
            FRACTION_OF_DATA_TARGET,
            dataframe, operation, filter_column
        )

        hyper_parameters.append(value)

        for operation in self.operations[1:]:
            value, dataframe = select_by_diversity(
                dataframe, operation,
                label_generating_column,
                entity_id_column)
            hyper_parameters.append(value)

        self.set_hyper_parameters(hyper_parameters)
        return hyper_parameters

    def execute(self, dataframe, time_column, label_cutoff_time,
                filter_column_order_of_types,
                label_generating_column_order_of_types):
        """
        This function executes all the operations on the dataframe
        and returns the output. The output is two values. The
        first value is the result of executing on all data before
        the label_cutoff_time. The second value is the result
        of executing on all data, including that after the
        label_cutoff_time.

        See paper:
        "What would a data scientist ask? Automatically
        formulating and solving predicton problems."

        Parameters
        ----------
        dataframe: data to be executed on
        time_column: column name of the column
            containing time information in the data
        label_cutoff_time: the time at which the data
            is segmented. data after the time is used for
            labels during test. data before the time is used
            for labels during training.
        filter_column_order_of_types: a list containing the types expected in the
            sequence of operations on the filter column
        label_generating_column_order_of_types: a list containing the types expected in the
            sequence of operations on the label generating column

        Returns
        ----------
        pre_label_cutoff_time_execution_result: label for training time segment
        all_data_execution_result: label for testing (all time) time segment

        """
        dataframe = dataframe.sort_values(by=time_column)
        dataframe = dataframe.copy()

        pre_label_cutoff_time_execution_result = dataframe[
            dataframe[time_column] < label_cutoff_time]
        all_data_execution_result = dataframe

        continue_executing_on_precutoff_df = True
        continue_executing_on_all_data_df = True

        for idx, operation in enumerate(self.operations):

            logging.debug(
                "Beginning execution of operation: {}".format(operation))

            if len(pre_label_cutoff_time_execution_result) == 0:
                continue_executing_on_precutoff_df = False

            if len(all_data_execution_result) == 0:
                continue_executing_on_all_data_df = False

            if continue_executing_on_all_data_df:
                single_piece_of_data = all_data_execution_result.iloc[
                    0][operation.column_name]

                if idx == 0:
                    check_type(
                        filter_column_order_of_types[idx],
                        single_piece_of_data)
                elif idx > 0:
                    check_type(
                        label_generating_column_order_of_types[idx - 1],
                        single_piece_of_data)

                all_data_execution_result = operation.execute(
                    all_data_execution_result)

                if len(all_data_execution_result) == 0:
                    continue

                single_piece_of_data = all_data_execution_result.iloc[0][operation.column_name]

                if idx == 0:
                    check_type(
                        filter_column_order_of_types[idx + 1],
                        single_piece_of_data)
                elif idx > 0:
                    check_type(
                        label_generating_column_order_of_types[idx],
                        single_piece_of_data)

            if continue_executing_on_precutoff_df:
                pre_label_cutoff_time_execution_result = operation.execute(
                    pre_label_cutoff_time_execution_result)

        return pre_label_cutoff_time_execution_result, all_data_execution_result

    def __str__(self):
        """
        This function converts Prediction Problems to English.

        Parameters
        ----------
        None

        Returns
        ----------
        description: natural language description

        """
        description = ""
        last_op_idx = len(self.operations) - 1
        for idx, operation in enumerate(self.operations):
            description += str(operation)
            if idx != last_op_idx:
                description += "->"
        return description

    def to_json(self):
        """
        This function converts Prediction Problems to JSON

        Parameters
        ----------
        None

        Returns
        ----------
        json: JSON representation of the Prediction Problem.

        """
        return json.dumps(
            {"operations": [json.loads(op_to_json(op)) for op in self.operations],
             "filter_column_order_of_types":
             self.filter_column_order_of_types,
             "label_generating_column_order_of_types":
             self.label_generating_column_order_of_types})

    def from_json(json_data):
        """
        This function converts a JSON snippet
        to a prediction problem

        Parameters
        ----------
        json_data: JSON code containing the prediction problem.

        Returns
        ----------
        problem: Prediction Problem
        """
        data = json.loads(json_data)
        operations = [op_from_json(json.dumps(item))
                      for item in data['operations']]
        problem = PredictionProblem(operations)
        problem.filter_column_order_of_types = data[
            'filter_column_order_of_types']
        problem.label_generating_column_order_of_types = data[
            'label_generating_column_order_of_types']
        return problem

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False


def select_by_remaining(fraction_of_data_target, dataframe, operation,
                        filter_column, num_random_samples=10, num_rows_to_execute_on=2000):
    """
    This function selects a parameter setting for the
    filter operation. The parameter setting that nearest filters
    fraction_of_data_target data is chosen.

    Parameters
    ----------
    fraction_of_data_target: The fraction of the filter operation
        aims to keep in the dataset.
    dataframe: the relevant data
    operation: the filter operation
    filter_column: the column name of the column intended to be filtered over
    num_random_samples: if there's more than this many unique values to test,
        randomly sample this many values from the dataset
    num_rows_to_execute_on: if the dataframe contains more than this number of rows,
        randomly select this many rows to use as the dataframe

    Returns
    ----------
    best_filter_value: parameter setting for the filter operation.
    """
    logging.debug("Performing select_by_remaining...")
    if len(operation.REQUIRED_PARAMETERS) == 0:
        return None
    else:
        unique_filter_values = set(dataframe[filter_column])

        if len(unique_filter_values) > num_random_samples:
            unique_filter_values = list(random.sample(unique_filter_values, num_random_samples))

        if len(dataframe) > num_rows_to_execute_on:
            dataframe = dataframe.sample(num_rows_to_execute_on)

        logging.debug("number of unique filter values: {}".format(len(unique_filter_values)))

        best = 1
        best_filter_value = 0

        for unique_filter_value in unique_filter_values:
            loop_start_time = time.time()

            total = len(dataframe)

            operation.set_hyper_parameter(unique_filter_value)

            filtered_df = operation.execute(dataframe)
            count = len(filtered_df)

            fraction_of_data_left = count / total

            score = abs(fraction_of_data_left - fraction_of_data_target)
            if score < best:
                best = score
                best_filter_value = unique_filter_value

            logging.debug("single loop iteration time: {}".format(time.time() - loop_start_time))

        return best_filter_value


def select_by_diversity(dataframe, operation, label_generating_column,
                        entity_id_column, num_random_samples=10, num_rows_to_execute_on=2000):
    """
    This function selects a parameter setting for the
    operations, excluding the filter operation.
    The parameter setting that maximizes the
    entropy of the output data is chosen.

    Parameters
    ----------
    dataframe: the relevant data
    operation: the filter operation
    label_generating_column: column name of the
        column of interest in the data
    entity_id_column: column name of
        the column containing entities in the data
    num_random_samples: if there's more than this many unique values to test,
        randomly sample this many values from the dataset
    num_rows_to_execute_on: if the dataframe contains more than this number of rows,
        randomly select this many rows to use as the dataframe

    Returns
    ----------
    best_parameter_value: parameter setting for the operation.
    pertinent_df: the dataframe after having the operation applied
        with the chosen parameter value.
    """
    logging.debug("Performing select_by_diversity")
    df = dataframe.copy()

    if len(operation.REQUIRED_PARAMETERS) == 0:
        logging.debug("no parameters required for this operation")
        return None, dataframe
    else:
        unique_parameter_values = set(dataframe[label_generating_column])

        if len(unique_parameter_values) > num_random_samples:
            unique_parameter_values = list(random.sample(unique_parameter_values, num_random_samples))

        if len(dataframe) > num_rows_to_execute_on:
            df = dataframe.sample(num_rows_to_execute_on)

        logging.debug("number of unique parameter values: {}".format(len(unique_parameter_values)))

        best = 0
        best_parameter_value = 0

        for unique_parameter_value in unique_parameter_values:
            loop_start_time = time.time()

            operation.set_hyper_parameter(unique_parameter_value)

            output_df = df.groupby(entity_id_column).apply(operation.execute)

            entropy = entropy_of_a_list(
                list(output_df[label_generating_column]))
            if entropy > best:
                best = entropy
                best_parameter_value = unique_parameter_value

            logging.debug("single loop iteration time: {}".format(time.time() - loop_start_time))

        operation.set_hyper_parameter(best_parameter_value)
        pertinent_df = dataframe.copy().groupby(entity_id_column).apply(operation.execute)

        logging.debug("found optimal parameter setting: {}".format(best_parameter_value))

        return best_parameter_value, pertinent_df


def entropy_of_a_list(values):
    """
    Calculate the entropy (information) of the list.

    Parameters
    ----------
    values: list of values

    Returns
    ----------
    entropy: the entropy or information in the list.
    """
    counts = Counter(values).values()
    total = float(sum(counts))
    probabilities = [val / total for val in counts]
    entropy = stats.entropy(probabilities)
    return entropy


def check_type(expected_type, actual_data):
    """
    Asserts that the expected type matches the actual data's type.

    Parameters
    ----------
    expected_type: the expected type of the data in TableMeta format
    actual_data: a piece of the actual data

    Returns
    ----------
    None
    """
    logging.debug(
        "Beginning check type. Expected type is: {}, \
        Actual data is: {}, Actual type is: {}".format(
            expected_type,
            actual_data,
            type(actual_data)))

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
        logging.critical('check_type function received an unexpected type.')
