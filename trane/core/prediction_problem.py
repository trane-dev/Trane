import json
import logging
import os
import warnings

import dill
import numpy as np
import pandas as pd

from ..ops.aggregation_ops import *  # noqa
from ..ops.filter_ops import *  # noqa
from ..ops.op_saver import op_from_json, op_to_json
from ..ops.row_ops import *  # noqa
from ..ops.transformation_ops import *  # noqa
from ..utils.table_meta import TableMeta

__all__ = ['PredictionProblem']


class PredictionProblem:

    """
    Prediction Problem is made up of a series of Operations. It also contains
    information about the types expected as the input and output of
    each operation.
    """

    def __init__(self, operations, entity_id_col,
                 label_col, table_meta=None, cutoff_strategy=None):
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
        self.label_col = label_col
        self.table_meta = table_meta
        self.cutoff_strategy = cutoff_strategy

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
        '''
        Executes the problem's operations on a dataframe. Generates
        '''

        if not self.is_valid(self.table_meta):
            raise ValueError(
                'Your Problem\'s specified operations do not match with the '
                'problem\'s table meta. Therefore, the problem is not '
                'valid.')

        df = df.copy()
        grouped = df.groupby(self.entity_id_col)

        res_dict = {}

        for entity_id, df_group in grouped:

            # generate the a cutoff date if the problem has a cutoff strategy
            cutoff = None
            if self.cutoff_strategy:
                cutoff = self.cutoff_strategy.generate_fn(
                    df_group, self.entity_id_col)

            label_series = self._execute_operations_on_df(
                df_group)[self.label_col]

            # add the label to the results dictionary
            res_dict = self._insert_single_label_into_dict(
                entity_id, label_series, cutoff, res_dict)

        res = pd.DataFrame.from_dict(data=res_dict, orient='index')
        self._rename_columns(res, [self.entity_id_col, 'cutoff', 'label'])
        return res

    def _insert_single_label_into_dict(
            self, entity_id, label_series, cutoff, res_dict):
        '''
        Inserts a single row of a dataframe into the passed dictionary and
        returns it.

        Parameters
        ----------
        label_df: dataframe
        cutoff: cutoff time
        res_dict: dictionary with key entity_id and value a two part tuple:
            (cutoff time, binary label)

        Returns
        -------
        dictionary
        '''
        num_rows = len(label_series)
        if num_rows > 1:
            warnings.warn(
                "Operations returned more than 1 result for entity " +
                str(entity_id) + ". This probably means you forgot to " +
                "add an aggregation operation. Arbitrarily picking the " +
                "first result.",
                RuntimeWarning)
        elif num_rows < 1:
            warnings.warn(
                "Operation returned fewer than 1 result for entity " +
                str(entity_id) + ". Returning an unedited res_dict",
                RuntimeWarning)
        if num_rows > 0:
            group_tuple = (cutoff, label_series.iloc[0])
            res_dict[entity_id] = group_tuple

        return res_dict

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
            rename_dict[col_num] = column_list[col_num + 1]

        df.index.names = [column_list[0]]
        df.rename(columns=rename_dict, inplace=True)
        return df

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
        description: str natural language description of the problem

        """
        desc_arr = []
        description = 'For each ' + self.entity_id_col + ' predict'
        # cycle through each operation to create dataops
        # dataops are a series of operations containing one and only one
        # aggregation op at its end
        dataop = []
        for op in self.operations:
            op_type = type(op)
            dataop.append(op)

            if issubclass(op_type, AggregationOpBase):
                # describe each dataop
                desc_arr = desc_arr + self._describe_dataop(dataop)
                dataop = []

        # cycle through ops, pick out filters and describe them
        filterop_desc_arr = []
        for op in self.operations:
            if issubclass(type(op), FilterOpBase):
                filterop_desc_arr.append(self._describe_filter(op))

        # join filter ops with ands and append to the description
        if len(filterop_desc_arr) > 0:
            description += ' and '.join(filterop_desc_arr)

        # join the dataops together with ' of '
        description += ' of '.join(desc_arr)

        # add the cutoff strategy description if it exists
        if self.cutoff_strategy:
            description += ' ' + self.cutoff_strategy.description

        return description

    def _describe_dataop(self, dataop):
        """
        A DataOp is a series of non-aggregation operations ending with one and
        only one aggregation op.

        In a dataop, filter operations are ignored. (They are dealt with
        elsewhere.) The NL description puts the agg op first, followed other
        operations in linear (left to right order.)

        So an operation which is row_op -> trans_op -> agg_op will be written
        as agg_op OF trans_op OF row_op.
        """
        descriptions = []

        agg_op = dataop[-1]
        if not issubclass(type(agg_op), AggregationOpBase):
            raise ValueError(
                'Last operation of dataop must be an aggregation op. '
                'It is currently a ' + type(agg_op) + '.')

        descriptions.append(self._describe_aggop(agg_op))
        # remove the last operation (aggregation) and reverse the list
        dataop = dataop[:-1]
        dataop.reverse()

        for op in dataop:
            op_type = type(op)
            description = ''
            if issubclass(op_type, FilterOpBase):
                # filter ops are described seperately
                break
            elif issubclass(op_type, AggregationOpBase):
                raise ValueError(
                    'There is more than one aggreagation op in your dataop. '
                    'A dataop must contain one and only one aggregation '
                    ' at its end.')
            elif issubclass(op_type, TransformationOpBase):
                description = self._describe_transop(op)
            elif issubclass(op_type, RowOpBase):
                description = self._describe_rowop(op)

            descriptions.append(description)

        # add the cutoff strategy description
        if self.cutoff_strategy:
            description += self.cutoff_strategy.description

        return descriptions

    def _describe_aggop(self, op):
            agg_op_str_dict = {
                FirstAggregationOp: "the first",
                LastAggregationOp: "the last",
                CountAggregationOp: "the number of",
                SumAggregationOp: "the sum of",
                LMFAggregationOp: "the last minus first"
            }
            if op.input_type == TableMeta.TYPE_BOOL and isinstance(
                    op, SumAggregationOp):
                return " the number of records whose"
            if type(op) in agg_op_str_dict:
                return agg_op_str_dict[type(op)]

    def _describe_rowop(self, op):
        row_op_str_dict = {
            GreaterRowOp: "greater than",
            EqRowOp: "equal to",
            NeqRowOp: "not equal to",
            LessRowOp: "less than"}

        if isinstance(op, IdentityRowOp):
            # return " {col}".format(col=op.column_name)
            return ""
        if isinstance(op, ExpRowOp):
            return " the exp of {col}".format(col=op.column_name)
        if type(op) in row_op_str_dict:
            return " {col} is {op} {threshold}".format(
                col=op.column_name, op=row_op_str_dict[type(op)],
                threshold=op.hyper_parameter_settings['threshold'])
        return " (unknown row op)"

    def _describe_transop(self, op):
        if isinstance(op, IdentityTransformationOp):
            return ""
        if isinstance(op, DiffTransformationOp):
            return "fluctuation of " + str(op.column_name)
        if isinstance(op, ObjectFrequencyTransformationOp):
            return "frequency of " + str(op.column_name)

    def _describe_filter(self, op):
        filter_op_str_dict = {
            GreaterFilterOp: "greater than",
            EqFilterOp: "equal to",
            NeqFilterOp: "not equal to",
            LessFilterOp: "less than"}

        filter_ops = [
            x for x in self.operations if issubclass(type(x), FilterOpBase)]

        # remove AllFilterOp
        filter_ops = [x for x in filter_ops if not isinstance(x, AllFilterOp)]

        desc = ' with '
        last_op_idx = len(filter_ops) - 1
        for idx, op in enumerate(filter_ops):
            op_desc = '{col} {op} {threshold}'.format(
                col=op.column_name,
                op=filter_op_str_dict[type(op)],
                threshold=op.hyper_parameter_settings.get('threshold', ''))
            desc += op_desc

            if idx != last_op_idx:
                desc += ' and '

        return desc

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
        This function converts Prediction Problems to JSON. It captures the
        table_meta, but not the cutoff_strategy

        Parameters
        ----------
        None

        Returns
        -------
        json: JSON representation of the Prediction Problem.

        """
        table_meta_json = None
        if self.table_meta:
            table_meta_json = self.table_meta.to_json()

        return json.dumps(
            {"operations": [
                json.loads(op_to_json(op)) for op in self.operations],
             "entity_id_col": self.entity_id_col,
             "label_col": self.label_col,
             "table_meta": table_meta_json})

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

        # only tries json.loads if json_data is not a dict
        if type(json_data) != dict:
            json_data = json.loads(json_data)

        operations = [
            op_from_json(json.dumps(item)) for item in json_data['operations']]
        entity_id_col = json_data['entity_id_col']
        label_col = json_data['label_col']
        table_meta = TableMeta.from_json(json_data.get('table_meta'))

        problem = PredictionProblem(
            operations=operations,
            entity_id_col=entity_id_col,
            label_col=label_col,
            table_meta=table_meta,
            cutoff_strategy=None)
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
