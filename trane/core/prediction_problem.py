import json
import logging
import os
import warnings

import dill
import numpy as np
import pandas as pd

from ..ops.op_saver import op_from_json, op_to_json
from ..utils.table_meta import TableMeta

__all__ = ['PredictionProblem']


class PredictionProblem:

    """
    Prediction Problem is made up of a series of Operations. It also contains
    information about the types expected as the input and output of
    each operation.
    """

    def __init__(self, operations, entity_id_col=None, time_col=None,
                 table_meta=None, cutoff_strategy=None):
        """
        Parameters
        ----------
        operations: list of Operations of type op_base
        cutoff_strategy: a CutoffStrategy object

        Returns
        -------
        None
        """
        self.operations = operations
        self.entity_id_col = entity_id_col
        self.time_col = time_col
        self.table_meta = table_meta
        self.cutoff_strategy = cutoff_strategy
        self.filter_column_order_of_types = None
        self.label_generating_column_order_of_types = None

    def is_valid(self, table_meta=None):
        '''
        Typechecking for operations. Insures that their input and output types
        match. Allows a user to use the problem's existing table_meta, or pass
        in a new one

        Parameters
        ----------
        table_meta: TableMeta object. Contains meta information about the data

        Returns
        -------
        Bool
        '''
        # don't contaminate original table_meta
        if table_meta:
            temp_meta = table_meta.copy()
        else:
            temp_meta = self.table_meta.copy()

        # sort each operation in its respective bucket
        for op in self.operations:
            # op.type_check returns a modified temp_meta,
            # which accounts for the operation having taken place
            temp_meta = op.op_type_check(temp_meta)
            if temp_meta is None:
                return False

        return True

    def execute(self, df):
        """
        executes a prediction problem on an entire dataframe. Makes use of
        cutoff times supplied by self.cutoff_strategy

        Parameters
        ----------
        df: dataframe for labels to be generated from
        time_col: str column which will be used to compare data cutoffs

        Returns
        -------
        pre_label_execution_df: a dataframe of labels generated from before the
            specified cutoff time
        post_label_execution_df: a dataframe of labels generated from after the
            specified cutoff time
        """
        if not self.is_valid(self.table_meta):
            print('Your Problem\'s specified operations do not match with the '
                  'problem\'s table meta. Therefore, the problem is not '
                  'valid.')
            raise

        df = df.copy()
        df.index = df[self.entity_id_col]
        pre_res_dict, post_res_dict = {}, {}

        # cycle through each entity
        for index in df.index.unique():

            # original rows of an entity
            entity_df = df.loc[[index]]

            # test cutoff and label cutoff are the same thing
            # Go directly to the cutoff_fn instead of generate_cutoffs, which
            # is used for generating cutoffs from multi-entity df's
            _, test_cut = self.cutoff_strategy.generate_fn(
                entity_df, self.entity_id_col)

            # tested to here. Having trouble with comparison operators
            pre_test_df = entity_df[entity_df[self.time_col] <= test_cut]
            test_df = entity_df[entity_df[self.time_col] > test_cut]
            # execute the operations

            pre_test_df = self._execute_operations_on_df(pre_test_df)
            test_df = self._execute_operations_on_df(test_df)
            # add the operation result to a dictionary
            pre_res_dict = self._insert_first_row_into_dict(
                pre_res_dict, pre_test_df, index)
            post_res_dict = self._insert_first_row_into_dict(
                post_res_dict, test_df, index)

        # convert pre-test cutoff results to a dataframe
        # pre_test_df = pd.DataFrame.from_dict(
        #     data=pre_res_dict, orient='index', columns=df.columns)
        pre_test_df = pd.DataFrame.from_dict(data=pre_res_dict, orient='index')
        pre_test_df = self._rename_columns(pre_test_df, df.columns.values)
        pre_test_df.index = pre_test_df[self.entity_id_col]

        # convert post-test cutoff results to a dataframe
        test_df = pd.DataFrame.from_dict(data=post_res_dict, orient='index')
        test_df = self._rename_columns(test_df, df.columns.values)
        test_df.index = test_df[self.entity_id_col]

        # return the two dataframes
        return pre_test_df, test_df

    def _rename_columns(self, df, column_list):
        '''
        Renames columns in a given dataframe, in order, as the column_list.
        This is required because of support for Python 2.7 and Pandas 0.21
        A more modern way is to pass columns=df.columns
            into pd.DataFrame.from_dict.

        Parameters
        ----------
        df: DataFrame whose columns will be renamed
        column_list: list of column names

        Returns
        -------
        dataframe with renamed columns
        '''

        rename_dict = {}
        for col_num in df.columns.values:
            rename_dict[col_num] = column_list[col_num]
        df.rename(columns=rename_dict, inplace=True)
        return df

    def _insert_first_row_into_dict(
            self, dict_to_insert, df, entity_id):
        num_rows = len(df)
        if num_rows > 1:
            warnings.warn(
                "Operations returned more than 1 result for entity " +
                str(entity_id) + ". This probably means you forgot to add " +
                " an aggregation operation. Arbitrarily picking the first " +
                " result.",
                RuntimeWarning)

        # handle edge case in which the df is empty but we still need to return
        # a shaped array
        if num_rows > 0:
            dict_to_insert[entity_id] = df.iloc[0].values

        return dict_to_insert

    def _execute_operations_on_df(self, df):
        '''
        Execute operations on df. This method assumes that data leakage/cutoff
            times have already been taken into account, and just blindly
            executes the operations.
        Parameters
        ----------
        df: dataframe to be operated on

        Returns
        -------
        df: dataframe after operations
        '''
        df = df.copy()
        for operation in self.operations:
            df = operation.execute(df)
        return df

    def __str__(self):
        """
        This function converts Prediction Problems to English.

        Parameters
        ----------
        None

        Returns
        -------
        description: natural language description

        """
        description = ""
        last_op_idx = len(self.operations) - 1
        for idx, operation in enumerate(self.operations):
            description += str(operation)
            if idx != last_op_idx:
                description += "->"
        return description

    def save(self, path, problem_name):
        '''
        Saves the pediction problem in two files.

        One file is a dill of the cutoff strategy.
        The other file is the jsonified operations and the relative path to
        that cutoff strategy.

        Parameters
        ----------
        path: str - the directory in which save the problem
        problem_name: str - the filename to assign the problem

        Returns
        -------
        dict
        {'saved_correctly': bool,
         'directory_created': bool,
         'problem_name': str}
        The new problem_name may have changed due to a filename collision

        '''
        json_saved = False
        dill_saved = False
        created_directory = False

        # create directory if it doesn't exist
        if not os.path.isdir(path):
            os.makedirs(path)
            created_directory = True

        # rename the problem_name if already exists
        json_file_exists = os.path.exists(
            os.path.join(path, problem_name + '.json'))
        dill_file_exists = os.path.exists(
            os.path.join(path, problem_name + '.dill'))

        i = 1
        while json_file_exists or dill_file_exists:
            problem_name += str(i)

            i += 1
            json_file_exists = os.path.exists(
                os.path.join(path, problem_name + '.json'))
            dill_file_exists = os.path.exists(
                os.path.join(path, problem_name + '.dill'))

        # get the cutoff_strategy bytes
        cutoff_dill_bytes = self._dill_cutoff_strategy()

        # add a key to the problem json
        json_dict = json.loads(self.to_json())
        json_dict['cutoff_dill'] = problem_name + '.dill'

        # write the files
        with open(os.path.join(path, problem_name + '.json'), 'w') as f:
            json.dump(obj=json_dict, fp=f, indent=4, sort_keys=True)
            json_saved = True

        with open(os.path.join(path, problem_name + '.dill'), 'wb') as f:
            f.write(cutoff_dill_bytes)
            dill_saved = True

        return({'saved_correctly': json_saved & dill_saved,
                'created_directory': created_directory,
                'problem_name': problem_name})

    @classmethod
    def load(cls, json_file_path):
        '''
        Load a prediction problem from json file.
        If the file links to a dill (binary) cutoff_srategy, also load that
        and assign it to the prediction problem.

        Parameters
        ----------
        json_file_path: str, path and filename for the json file

        Returns
        -------
        PredictionProblem

        '''
        with open(json_file_path, 'r') as f:
            problem_dict = json.load(f)
            problem = cls.from_json(problem_dict)

        cutoff_strategy_file_name = problem_dict.get('cutoff_dill', None)

        if cutoff_strategy_file_name:
            # reconstruct cutoff strategy filename
            pickle_path = os.path.join(
                os.path.dirname(json_file_path), cutoff_strategy_file_name)

            # load cutoff strategy from file
            with open(pickle_path, 'rb') as f:
                cutoff_strategy = dill.load(f)

            # assign cutoff strategy to problem
            problem.cutoff_strategy = cutoff_strategy

        return problem

    def to_json(self):
        """
        This function converts Prediction Problems to JSON

        Parameters
        ----------
        None

        Returns
        -------
        json: JSON representation of the Prediction Problem.

        """
        return json.dumps(
            {"operations": [json.loads(op_to_json(op)
                                       ) for op in self.operations],
             "filter_column_order_of_types":
             self.filter_column_order_of_types,
             "label_generating_column_order_of_types":
             self.label_generating_column_order_of_types})

    @classmethod
    def from_json(cls, json_data):
        """
        This function converts a JSON snippet
        to a prediction problem

        Parameters
        ----------
        json_data: JSON code or dict containing the prediction problem.

        Returns
        -------
        problem: Prediction Problem
        """

        data = json_data

        # only tries json.loads if json_data is not a dict
        if type(data) != dict:
            data = json.loads(json_data)

        operations = [op_from_json(json.dumps(item))
                      for item in data['operations']]
        problem = PredictionProblem(operations, cutoff_strategy=None)
        problem.filter_column_order_of_types = data[
            'filter_column_order_of_types']
        problem.label_generating_column_order_of_types = data[
            'label_generating_column_order_of_types']
        return problem

    def _dill_cutoff_strategy(self):
        '''
        Function creates a dill for the problem's associated cutoff strategy

        This function requires cutoff time to be assigned.

        Parameters
        ----------

        Returns
        -------
        a dill of the cutoff strategy
        '''
        cutoff_dill = dill.dumps(self.cutoff_strategy)
        return cutoff_dill

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False

    def _check_type(self, expected_type, actual_data):
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

        allowed_types_category = [bool, int, str, float]
        allowed_types_bool = [bool, np.bool_]
        allowed_types_text = [str]
        allowed_types_int = [int, np.int64]
        allowed_types_float = [float, np.float64, np.float32]
        allowed_types_time = allowed_types_bool + allowed_types_int + \
            allowed_types_text + allowed_types_float
        allowed_types_ordered = allowed_types_bool + \
            allowed_types_int + allowed_types_text + allowed_types_float
        allowed_types_id = allowed_types_int + allowed_types_text + \
            allowed_types_float

        if expected_type == TableMeta.TYPE_CATEGORY:
            assert(type(actual_data) in allowed_types_category)

        elif expected_type == TableMeta.TYPE_BOOL:
            assert(type(actual_data) in allowed_types_bool)

        elif expected_type == TableMeta.TYPE_ORDERED:
            assert(type(actual_data) in allowed_types_ordered)

        elif expected_type == TableMeta.TYPE_TEXT:
            assert(type(actual_data) in allowed_types_text)

        elif expected_type == TableMeta.TYPE_INTEGER:
            assert(type(actual_data) in allowed_types_int)

        elif expected_type == TableMeta.TYPE_FLOAT:
            assert(type(actual_data) in allowed_types_float)

        elif expected_type == TableMeta.TYPE_TIME:
            assert(type(actual_data) in allowed_types_time)

        elif expected_type == TableMeta.TYPE_IDENTIFIER:
            assert(type(actual_data) in allowed_types_id)

        else:
            logging.critical(
                'check_type function received an unexpected type.')
