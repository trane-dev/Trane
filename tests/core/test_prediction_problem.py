import sys
import unittest

import pandas as pd
from mock import MagicMock, patch

from trane.core.prediction_problem import PredictionProblem
from trane.ops.aggregation_ops import LastAggregationOp
from trane.ops.filter_ops import AllFilterOp, GreaterFilterOp
from trane.ops.row_ops import IdentityRowOp
from trane.ops.transformation_ops import IdentityTransformationOp
from trane.utils.table_meta import TableMeta

dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1),
                          (0, 0, 1, 1.08, 6.78, 18.89, 2),
                          (0, 0, 2, 4.69, 14.11, 41.35, 4)],
                         columns=[
                            "vendor_id", "taxi_id", "trip_id", "distance",
                            "duration", "fare", "num_passengers"])

json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", \
    "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"},\
    {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"},\
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


def test_hashing_collisions():
    col1 = "a"
    col2 = "b"
    op1 = GreaterFilterOp(col1)
    op2 = GreaterFilterOp(col2)
    op3 = GreaterFilterOp(col1)

    op1_hash = hash(op1)
    op2_hash = hash(op2)
    op3_hash = hash(op3)

    assert(op1_hash != op2_hash)
    assert(op1_hash == op3_hash)


def test_execute():
    label_generating_column = "fare"
    df = dataframe

    time_column = "trip_id"
    cutoff_time = 100
    operations = [AllFilterOp(label_generating_column),
                  IdentityRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]

    prediction_problem = PredictionProblem(
        operations=operations, cutoff_strategy=None)

    expected = 41.35
    precutoff_time, all_data = prediction_problem.execute(
        df, time_column, cutoff_time, ['float', 'float'],
        ['float', 'float', 'float', 'float'])

    found = precutoff_time[label_generating_column].iloc[0]
    assert(expected == found)


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
