import sys
import unittest
import warnings

import numpy as np
import pandas as pd
from mock import MagicMock, PropertyMock, patch

from trane.core.prediction_problem import PredictionProblem


class TestPredictionProblemMethods(unittest.TestCase):

    def setUp(self):
        mock_op = self.create_patch(
            'trane.ops.OpBase')
        self.operations = [mock_op for x in range(2)]

        self.mock_cutoff_strategy = self.create_patch(
            'trane.core.FixWindowCutoffStrategy')

        self.entity_col = 'entity_col'

        self.problem = PredictionProblem(
            operations=self.operations, entity_col=self.entity_col,
            cutoff_strategy=self.mock_cutoff_strategy)

    def create_patch(self, name):
        # helper method for creating patches
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def test_attributes_assigned(self):
        self.assertEqual(self.problem.operations, self.operations)
        self.assertEqual(self.problem.entity_col, self.entity_col)
        self.assertEqual(self.problem.cutoff_strategy,
                         self.mock_cutoff_strategy)

    def test_equality_false(self):
        entity_col = 'entity_col'
        mock_op = MagicMock()
        operations = [mock_op for x in range(2)]
        mock_cutoff_strategy = MagicMock()
        problem_1 = PredictionProblem(
            operations=operations, entity_col=entity_col,
            cutoff_strategy=mock_cutoff_strategy)

        # add a different magicmock for cutoff strategy
        problem_2 = PredictionProblem(
            operations=operations, entity_col=entity_col,
            cutoff_strategy=MagicMock())
        self.assertFalse(problem_1 == problem_2)

    def test_equality_false_diff_types(self):
        other_obj = 'a string'
        self.assertFalse(self.problem == other_obj)

    def test_equality_true(self):
        entity_col = 'entity_col'
        mock_op = MagicMock()
        operations = [mock_op for x in range(2)]
        mock_cutoff_strategy = MagicMock()
        problem_1 = PredictionProblem(
            operations=operations, entity_col=entity_col,
            cutoff_strategy=mock_cutoff_strategy)

        problem_2 = PredictionProblem(
            operations=operations, entity_col=entity_col,
            cutoff_strategy=mock_cutoff_strategy)
        self.assertTrue(problem_1 == problem_2)

    def test_cutoff_strategy_exists(self):
        self.assertIsNotNone(self.problem.cutoff_strategy)

    def entity_id_col_exists(self):
        self.assertIsNot(self.problem.entity_col)

    def test_is_valid_succeeds(self):
        self.assertIsNotNone(self.problem.is_valid)

        table_meta_mock = MagicMock()
        table_meta_mock.copy.return_value = table_meta_mock

        for op in self.problem.operations[:-1]:
            op.op_type_check.return_value = table_meta_mock
        self.problem.operations[-1].op_type_check.return_value = "float"

        self.assertTrue(
            self.problem.is_valid(table_meta=table_meta_mock))

        self.assertTrue(table_meta_mock.copy.called)
        for op in self.problem.operations:
            self.assertTrue(op.op_type_check.called)

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

    def test_execute_raises_if_not_valid(self):
        is_valid_mock = MagicMock()
        self.problem.is_valid = is_valid_mock
        is_valid_mock.return_value = False

        with self.assertRaises(ValueError):
            self.problem.execute('foo')


# TODO test cutoff save load
class TestPredictionProblemSaveLoad(unittest.TestCase):

    def setUp(self):
        mock_op = self.create_patch(
            'trane.ops.OpBase')
        self.operations = [mock_op for x in range(2)]

        self.mock_cutoff_strategy = self.create_patch(
            'trane.core.FixWindowCutoffStrategy')
        self.table_meta = MagicMock(name='table_meta')

        self.entity_col = 'entity_col'

        self.problem = PredictionProblem(
            operations=self.operations, entity_col=self.entity_col,
            table_meta=self.table_meta,
            cutoff_strategy=self.mock_cutoff_strategy)

    def create_patch(self, name):
        # helper method for creating patches
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def test_to_json(self):
        self.assertIsNotNone(self.problem.to_json)
        json_patch = self.create_patch('trane.core.prediction_problem.json')

        op_to_json_patch = self.create_patch(
            'trane.core.prediction_problem.op_to_json')
        op_to_json_patch.return_value = {'op': 'exists'}

        self.problem.table_meta.to_json.return_value = {'table_meta': 'exists'}

        self.problem.to_json()

        self.assertTrue(json_patch.dumps.called)
        self.assertTrue(json_patch.loads.called)

    def test_from_json(self):
        json_loads_patch = self.create_patch(
            'trane.core.prediction_problem.json.loads')
        json_dumps_patch = self.create_patch(
            'trane.core.prediction_problem.json.dumps')
        op_from_json_patch = self.create_patch(
            'trane.core.prediction_problem.op_from_json')
        op_from_json_patch.return_value = op_from_json_patch

        table_meta_from_json_patch = self.create_patch(
            'trane.core.prediction_problem.TableMeta.from_json')
        table_meta_from_json_patch.return_value = table_meta_from_json_patch

        data = MagicMock(name='data')
        json_loads_patch.return_value = data

        # mock out json_data['operations']
        attr_dict = {'operations': [1, 2, 3],
                     'entity_col': 'entity_col',
                     'table_meta': 'table_meta'}
        data.__getitem__.side_effect = attr_dict.__getitem__

        json_dumps_patch.return_value = MagicMock(name='op')

        problem = PredictionProblem.from_json(data)
        self.assertEqual(problem.operations[0], op_from_json_patch)
        self.assertEqual(problem.entity_col, attr_dict['entity_col'])
        self.assertEqual(problem.table_meta, table_meta_from_json_patch)

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
        json_patch = self.create_patch(
            'trane.core.prediction_problem.json')

        path_patch = self.create_patch(
            'trane.core.prediction_problem.os.path.isdir')
        path_patch.return_value = False
        makedirs_patch = self.create_patch(
            'trane.core.prediction_problem.os.makedirs')

        path = '../Data'
        problem_name = 'problem_name'

        open_patch = None
        if (sys.version_info > (3, 0)):
            open_patch = self.create_patch('builtins.open')
        else:
            open_patch = self.create_patch('__builtin__.open')

        # Save
        self.problem.save(path=path, problem_name=problem_name)

        self.assertTrue(dill_cutoff_strategy_patch.called)
        self.assertTrue(os_path_exists_patch.called)
        self.assertTrue(to_json_patch.called)
        self.assertTrue(path_patch.called)
        self.assertTrue(makedirs_patch.called)
        self.assertTrue(json_patch.loads.called)
        self.assertTrue(open_patch.called)

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


class TestPredictionProblemDescription(unittest.TestCase):

    def setUp(self):
        mock_op = self.create_patch(
            'trane.ops.OpBase')
        self.operations = [mock_op for x in range(2)]

        self.mock_cutoff_strategy = self.create_patch(
            'trane.core.FixWindowCutoffStrategy')

        self.entity_col = 'entity_col'

        self.problem = PredictionProblem(
            operations=self.operations, entity_col=self.entity_col,
            cutoff_strategy=self.mock_cutoff_strategy)

    def create_patch(self, name):
        # helper method for creating patches
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def test_str(self):
        self.assertIsNotNone(self.problem.__str__)
        # don't do any more testing here, because this is likely to
        # be rewritten so frequently/quickly
