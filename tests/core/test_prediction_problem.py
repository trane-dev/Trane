import unittest

import pandas as pd
from mock import patch

from trane.core.prediction_problem import *  # noqa
from trane.core.prediction_problem import entropy_of_a_list
from trane.ops.aggregation_ops import *  # noqa
from trane.ops.filter_ops import *  # noqa
from trane.ops.row_ops import *  # noqa
from trane.ops.transformation_ops import *  # noqa
from trane.utils.table_meta import TableMeta

dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1),
                          (0, 0, 1, 1.08, 6.78, 18.89, 2),
                          (0, 0, 2, 4.69, 14.11, 41.35, 4)],
                         columns=["vendor_id", "taxi_id", "trip_id", "distance",
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


def test_hyper_parameter_generation():
    label_generating_column = "fare"
    filter_column = "taxi_id"
    entity_id_column = "taxi_id"

    operations = [GreaterFilterOp(filter_column),
                  GreaterRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]

    prediction_problem = PredictionProblem(
        operations=operations, cutoff_strategy=None)
    prediction_problem.generate_and_set_hyper_parameters(
        dataframe, label_generating_column, filter_column, entity_id_column)

    op1_hyper_parameter = prediction_problem.operations[0].hyper_parameter_settings
    op2_hyper_parameter = prediction_problem.operations[1].hyper_parameter_settings
    prediction_problem.operations[2].hyper_parameter_settings
    prediction_problem.operations[3].hyper_parameter_settings

    assert(op1_hyper_parameter['threshold'] == 0)
    assert(op2_hyper_parameter['threshold'] == 41.35)


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


def test_hyper_parameter_generation_2():
    label_generating_column = "c1"
    filter_column = "c2"
    operations = [GreaterFilterOp(filter_column),
                  GreaterRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]

    prediction_problem = PredictionProblem(
        operations=operations, cutoff_strategy=None)
    prediction_problem.generate_and_set_hyper_parameters(
        dataframe2, label_generating_column, filter_column,
        entity_id_column="c1")

    op1_hyper_parameter = prediction_problem.operations[0].hyper_parameter_settings
    op2_hyper_parameter = prediction_problem.operations[1].hyper_parameter_settings
    prediction_problem.operations[2].hyper_parameter_settings
    prediction_problem.operations[3].hyper_parameter_settings

    print(op1_hyper_parameter)
    print(op2_hyper_parameter)
    assert(op1_hyper_parameter['threshold'] == 1)
    assert(op2_hyper_parameter['threshold'] == 4)


def test_op_type_check():
    filter_column = "fare"
    label_generating_column = "fare"
    operations = [AllFilterOp(filter_column),
                  IdentityRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]
    table_meta = TableMeta.from_json(json_str)

    prediction_problem_correct_types = PredictionProblem(
        operations=operations, cutoff_strategy=None)

    label_generating_column = "vendor_id"
    operations = [AllFilterOp(filter_column),
                  IdentityRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LMFAggregationOp(label_generating_column)]
    prediction_problem_incorrect_types = PredictionProblem(
        operations=operations, cutoff_strategy=None)

    (
        correct_is_valid,
        filter_column_types_A,
        label_generating_column_types_A) = prediction_problem_correct_types.is_valid_prediction_problem(
        table_meta,
        filter_column,
        "fare")
    (
        incorrect_is_valid,
        filter_column_types_B,
        label_generating_column_types_B) = prediction_problem_incorrect_types.is_valid_prediction_problem(
        table_meta,
        filter_column,
        label_generating_column)

    assert(filter_column_types_A == ['float', 'float'])
    assert(label_generating_column_types_A == [
           'float', 'float', 'float', 'float'])

    assert(filter_column_types_B is None)
    assert(label_generating_column_types_B is None)

    assert(correct_is_valid)
    assert(not incorrect_is_valid)


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
        df, time_column, cutoff_time, ['float', 'float'], ['float', 'float', 'float', 'float'])

    found = precutoff_time[label_generating_column].iloc[0]
    assert(expected == found)


def test_to_and_from_json():
    label_generating_column = "fare"
    operations = [AllFilterOp(label_generating_column),
                  IdentityRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]
    prediction_problem = PredictionProblem(
        operations=operations, cutoff_strategy=None)
    json_str = prediction_problem.to_json()
    prediction_problem_from_json = PredictionProblem.from_json(json_str)

    assert(prediction_problem == prediction_problem_from_json)


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


def test_equality():
    label_generating_column = "fare"
    operations = [AllFilterOp(label_generating_column),
                  IdentityRowOp(label_generating_column),
                  IdentityTransformationOp(label_generating_column),
                  LastAggregationOp(label_generating_column)]
    prediction_problem = PredictionProblem(
        operations=operations, cutoff_strategy=None)
    prediction_problem_clone = PredictionProblem(
        operations=operations, cutoff_strategy=None)

    assert(prediction_problem_clone == prediction_problem)


def test_entropy():
    a = [1, 2, 3, 4, 5, 6, 7, 8]
    entropy_a = entropy_of_a_list(a)
    b = [1, 1, 2, 3, 4, 5, 6, 7]
    entropy_b = entropy_of_a_list(b)
    c = [1, 1, 1, 1, 1, 1, 1, 1]
    entropy_c = entropy_of_a_list(c)
    d = [1, 2]
    entropy_d = entropy_of_a_list(d)

    assert(entropy_a > entropy_b > entropy_d > entropy_c)
    assert(entropy_a == 2.0794415416798357)
    assert(entropy_b == 1.9061547465398496)
    assert(entropy_c == 0.0)
    assert(entropy_d == 0.6931471805599453)


class TestPredictionProblemMethods(unittest.TestCase):
    def setUp(self):
        mock_op = self.create_patch(
            'trane.ops.OpBase')
        operations = [mock_op for x in range(4)]

        self.mock_cutoff_strategy = self.create_patch(
            'trane.core.CutoffStrategy')

        self.entity_col = 'entity_col'

        self.problem = PredictionProblem(
            operations=operations, entity_id_col=self.entity_col,
            cutoff_strategy=self.mock_cutoff_strategy)

        self.mock_dill = self.create_patch(
            'trane.core.prediction_problem.dill')

    def create_patch(self, name):
        # helper method for creating patches
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def test_cutoff_strategy_exists(self):
        self.assertIsNotNone(self.problem.cutoff_strategy)

    def entity_id_col_exists(self):
        self.assertIsNot(self.problem.entity_id_col)

    def test_generate_cutoffs_method_exists(self):
        self.assertIsNotNone(self.problem.generate_cutoffs)

    def test_generate_cutoffs_method_calls_cutoff_strategy_gen_cutoffs(self):
        self.problem.generate_cutoffs(df=None)

        self.assertTrue(self.problem.cutoff_strategy.generate_cutoffs.called)

        # entity_column passed?
        self.assertTrue(
            self.entity_col in
            self.problem.cutoff_strategy.generate_cutoffs.call_args[0])

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

    def test_dill_cutoff_strategy(self):
        self.assertIsNotNone(self.problem._dill_cutoff_strategy)
        cutoff_dill = self.problem._dill_cutoff_strategy()

        self.assertEqual(
            cutoff_dill, self.mock_dill.dumps(self.mock_cutoff_strategy))
