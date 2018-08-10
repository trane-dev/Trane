import sys
import unittest
import warnings

import numpy as np
import pandas as pd
from mock import MagicMock, patch

from trane.core.prediction_problem import PredictionProblem
from trane.ops.aggregation_ops import LastAggregationOp
from trane.ops.filter_ops import AllFilterOp
from trane.ops.row_ops import IdentityRowOp
from trane.ops.transformation_ops import IdentityTransformationOp
from trane.utils.table_meta import TableMeta

dataframe = pd.DataFrame(
    [(0, 0, 0, 5.32, 19.7, 53.89, 1),
     (0, 0, 1, 1.08, 6.78, 18.89, 2),
     (0, 0, 2, 4.69, 14.11, 41.35, 4)],
    columns=["vendor_id", "taxi_id", "trip_id", "distance",
             "duration", "fare", "num_passengers"])

json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", \
    "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"},\
    {"name": "taxi_id", "type": "id"}, \
    {"name": "trip_id", "type": "datetime"},\
    {"name": "distance", "type": "number", "subtype": "float"},\
    {"name": "duration", "type": "number", "subtype": "float"},\
    {"name": "fare", "type": "number", "subtype": "float"},\
    {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'

dataframe2 = pd.DataFrame([(0, 0),
                           (1, 1),
                           (2, 2),
                           (3, 3),
                           (4, 4),
                           (5, 5),
                           (6, 6),
                           (7, 7),
                           (8, 8),
                           (9, 9)], columns=['c1', 'c2'])


def test_to_and_from_json_with_order_of_types():
    label_generating_column = "fare"
    operations = [AllFilterOp(label_generating_column),
                  IdentityRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]
    prediction_problem = PredictionProblem(
        operations=operations, cutoff_strategy=None)
    prediction_problem.filter_column_order_of_types = [TableMeta.TYPE_INTEGER]
    prediction_problem.label_generating_column_order_of_types = \
        [TableMeta.TYPE_INTEGER, TableMeta.TYPE_BOOL, TableMeta.TYPE_CATEGORY]
    json_str = prediction_problem.to_json()
    prediction_problem_from_json = PredictionProblem.from_json(json_str)

    assert(prediction_problem == prediction_problem_from_json)


class TestPredictionProblemMethods(unittest.TestCase):

    def setUp(self):
        mock_op = self.create_patch(
            'trane.ops.OpBase')
        self.operations = [mock_op for x in range(4)]

        self.mock_cutoff_strategy = self.create_patch(
            'trane.core.CutoffStrategy')

        self.entity_col = 'entity_col'
        self.time_col = 'time_col'

        self.problem = PredictionProblem(
            operations=self.operations, entity_id_col=self.entity_col,
            time_col=self.time_col, cutoff_strategy=self.mock_cutoff_strategy)

        self.mock_dill = self.create_patch(
            'trane.core.prediction_problem.dill')

    def create_patch(self, name):
        # helper method for creating patches
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def test_attributes_assigned(self):
        self.assertEqual(self.problem.operations, self.operations)
        self.assertEqual(self.problem.entity_id_col, self.entity_col)
        self.assertEqual(self.problem.time_col, self.time_col)
        self.assertEqual(self.problem.cutoff_strategy,
                         self.mock_cutoff_strategy)

    def test_equality_false(self):
        entity_id = 'entity_col'
        mock_op = MagicMock()
        operations = [mock_op for x in range(4)]
        mock_cutoff_strategy = MagicMock()
        problem_1 = PredictionProblem(
            operations=operations, entity_id_col=entity_id,
            cutoff_strategy=mock_cutoff_strategy)

        # add a different magicmock for cutoff strategy
        problem_2 = PredictionProblem(
            operations=operations, entity_id_col=entity_id,
            cutoff_strategy=MagicMock())
        self.assertFalse(problem_1 == problem_2)

    def test_equality_true(self):
        entity_id = 'entity_col'
        mock_op = MagicMock()
        operations = [mock_op for x in range(4)]
        mock_cutoff_strategy = MagicMock()
        problem_1 = PredictionProblem(
            operations=operations, entity_id_col=entity_id,
            cutoff_strategy=mock_cutoff_strategy)

        problem_2 = PredictionProblem(
            operations=operations, entity_id_col=entity_id,
            cutoff_strategy=mock_cutoff_strategy)
        self.assertTrue(problem_1 == problem_2)

    def test_cutoff_strategy_exists(self):
        self.assertIsNotNone(self.problem.cutoff_strategy)

    def entity_id_col_exists(self):
        self.assertIsNot(self.problem.entity_id_col)

    def test_to_json_exists(self):
        self.assertIsNotNone(self.problem.to_json)

    def test_save(self):
        self.assertIsNotNone(self.problem.save)

        dill_cutoff_strategy_patch = self.create_patch(
            'trane.core.PredictionProblem._dill_cutoff_strategy')
        os_path_exists_patch = self.create_patch(
            'trane.core.prediction_problem.os.path.exists')
        os_path_exists_patch.return_value = False
        to_json_patch = self.create_patch(
            'trane.core.PredictionProblem.to_json')
        dill_cutoff_strategy_patch = self.create_patch(
            'trane.core.PredictionProblem._dill_cutoff_strategy')

        path = '../Data'
        problem_name = 'problem_name'

        try:
            self.problem.save(path=path, problem_name=problem_name)
        except Exception as e:
            print('test_save doesn\'t test i/o. '
                  'This exception statement catches the error.')
            print(e)

        self.assertTrue(to_json_patch.called)
        self.assertTrue(dill_cutoff_strategy_patch.called)
        self.assertTrue(os_path_exists_patch.called)

    def test_load(self):
        self.assertIsNotNone(PredictionProblem.load)

        open_patch = None
        if (sys.version_info > (3, 0)):
            open_patch = self.create_patch('builtins.open')
        else:
            open_patch = self.create_patch('__builtin__.open')

        json_patch = self.create_patch(
            'trane.core.prediction_problem.json')
        from_json_patch = self.create_patch(
            'trane.core.PredictionProblem.from_json')
        os_path_patch = self.create_patch(
            'trane.core.prediction_problem.os.path')
        dill_patch = self.create_patch(
            'trane.core.prediction_problem.dill')

        PredictionProblem.load('filepath.json')
        self.assertTrue(open_patch.called)
        self.assertTrue(json_patch.load.called)
        self.assertTrue(from_json_patch.called)
        self.assertTrue(os_path_patch.join.called)
        self.assertTrue(os_path_patch.dirname.called)
        self.assertTrue(dill_patch.load.called)

    def test_dill_cutoff_strategy(self):
        self.assertIsNotNone(self.problem._dill_cutoff_strategy)
        cutoff_dill = self.problem._dill_cutoff_strategy()

        self.assertEqual(
            cutoff_dill, self.mock_dill.dumps(self.mock_cutoff_strategy))

    def test_is_valid_succeeds(self):
        self.assertIsNotNone(self.problem.is_valid)

        table_meta_mock = MagicMock()
        table_meta_mock.copy.return_value = table_meta_mock

        for op in self.problem.operations:
            op.op_type_check.return_value = table_meta_mock

        self.assertTrue(
            self.problem.is_valid(table_meta=table_meta_mock))

        self.assertTrue(table_meta_mock.copy.called)
        for op in self.problem.operations:
            self.assertTrue(op.op_type_check.called)
            self.assertTrue(op.op_type_check.call_args[
                            0][0] == table_meta_mock)

    def test_is_valid_fails(self):
        table_meta_mock = MagicMock()
        table_meta_mock.copy.return_value = table_meta_mock

        for op in self.problem.operations:
            op.op_type_check.return_value = None

        self.assertFalse(
            self.problem.is_valid(table_meta=table_meta_mock))

        self.assertTrue(table_meta_mock.copy.called)

    def test_is_valid_substitutes_object_val(self):
        table_meta_mock = MagicMock(name='table_meta_mock')
        table_meta_mock.copy.return_value = table_meta_mock

        for op in self.problem.operations:
            op.op_type_check.return_value = table_meta_mock

        # fails without provided table_meta
        self.problem.table_meta = None
        with self.assertRaises(Exception):
            self.problem.is_valid()

        # uses local table_meta if assigned
        table_meta_mock = MagicMock(name='table_meta_mock')
        self.problem.table_meta = table_meta_mock
        self.problem.is_valid()
        # self.assertTrue(table_meta_mock.called)

    def test_execute_operations_on_df(self):
        mock_df = MagicMock()
        mock_df.copy.return_value = mock_df
        self.operations[0].execute.return_value = mock_df

        self.problem._execute_operations_on_df(mock_df)

        # the operation has been called as many times as there are operations
        # remember, all operations are actually the same mock in set_up
        self.assertEqual(
            self.operations[0].execute.call_count, len(self.operations))

        # the operation is passed the mock_df
        self.assertEqual(
            self.operations[0].execute.call_args[0][0], mock_df)

    def test_insert_first_row_into_dict_does_nothing_with_empty_arr(self):
        my_dict = {}
        my_empty_df = []
        entity_id = '12345'

        my_dict = self.problem._insert_first_row_into_dict(
            my_dict, my_empty_df, entity_id)

        self.assertEqual(len(my_dict.keys()), 0)

    def test_insert_first_row_into_dict_warns_with_more_than_one_res(self):
        # more integration than unit. It's *really* hard to mock out DataFrames
        my_dict = {}
        my_long_df = pd.DataFrame([[1, 2], [3, 4]])
        entity_id = '12345'

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Trigger a warning.
            my_dict = self.problem._insert_first_row_into_dict(
                my_dict, my_long_df, entity_id)
            # Verify number and type of warning
            assert len(w) == 1
            assert issubclass(w[-1].category, RuntimeWarning)

    def test_insert_first_row_into_dict_happy_path(self):
        # again, more integration than unittest
        my_dict = {}
        my_good_df = pd.DataFrame([[1, 2]])
        entity_id = '12345'

        # assert no warnings RuntimeWarningsed
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            my_dict = self.problem._insert_first_row_into_dict(
                my_dict, my_good_df, entity_id)
            assert len(w) == 0

        self.assertEqual(my_dict[entity_id][0], 1)
        self.assertEqual(my_dict[entity_id][1], 2)

    def test_execute(self):
        # this is an integration test. Unit tests with DataFrames are dastardly
        # complicated, and this method is central enough that it needs an
        # integration test anyhow.

        df = pd.DataFrame(
            [(0, 0, 0, 5.32, 19.7, 53.89, 1, np.datetime64('2000-01-01')),
             (1, 0, 1, 1.08, 6.78, 18.89, 2, np.datetime64('2000-01-01')),
             (0, 0, 2, 4.69, 14.11, 41.35, 4, np.datetime64('2000-01-01'))],
            columns=[
                "vendor_id", "taxi_id", "trip_id", "distance",
                "duration", "fare", "num_passengers", "date"])

        # set some things on this problem, since it's an integration test
        static_cutoff = (
            np.datetime64('1980-02-25'), np.datetime64('1980-02-25'))
        self.mock_cutoff_strategy.generate_fn.return_value = static_cutoff
        self.entity_col = 'vendor_id'
        self.time_col = 'date'

        # keep operations as mocks.
        self.problem = PredictionProblem(
            operations=self.operations, entity_id_col=self.entity_col,
            time_col=self.time_col, cutoff_strategy=self.mock_cutoff_strategy)

        # patch some helper methods
        mock_is_valid = self.create_patch(
            'trane.core.PredictionProblem.is_valid')
        mock_is_valid.return_value = True

        mock_execute_ops = self.create_patch(
            'trane.core.PredictionProblem._execute_operations_on_df')
        ops_return_val = {
            0: ['execute', 'ops', 'on', 'df', 'patch', 'lives', 'here',
                'io']}
        mock_execute_ops.return_value = ops_return_val

        mock_insert_first_row = self.create_patch(
            'trane.core.PredictionProblem._insert_first_row_into_dict')
        insert_return_val = {
            0: ['this', 'has', 'been', 'patched', 'so', 'don\'t', 'expect',
                'much']}
        mock_insert_first_row.return_value = insert_return_val

        # expected output
        expected_output = pd.DataFrame.from_dict(
            data=insert_return_val, orient='index')
        expected_output.rename(
            columns={
                0: 'vendor_id', 1: 'taxi_id', 2: 'trip_id', 3: 'distance',
                4: 'duration', 5: 'fare', 6: 'num_passengers', 7: 'date'},
            inplace=True)
        expected_output.index = expected_output['vendor_id']

        # Actually Execute
        pre_test_df, test_df = self.problem.execute(df)
        self.assertEqual(mock_insert_first_row.call_args[0][1], ops_return_val)

        self.assertTrue(expected_output.equals(pre_test_df))
        self.assertTrue(expected_output.equals(test_df))
