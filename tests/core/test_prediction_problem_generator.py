"""TESTING STRATEGY
Function: generate(self)
1. Ensure the correct # of problems are generated.
2. Generated problems are of proper type
3. Ensure prediction problems are generated in order of
   Filter->Row->Transformation->Aggregation
"""
import unittest

from mock import MagicMock, call, patch

from trane.core.prediction_problem_generator import PredictionProblemGenerator
from trane.ops.aggregation_ops import *  # noqa
from trane.ops.filter_ops import *  # noqa
from trane.utils.table_meta import TableMeta


class TestPredictionProblemGenerator(unittest.TestCase):

    def setUp(self):
        self.table_meta_mock = MagicMock()
        self.entity_col = "taxi_id"

        self.ensure_valid_inputs_patch = self.create_patch(
            'trane.core.PredictionProblemGenerator.ensure_valid_inputs')

        self.generator = PredictionProblemGenerator(
            table_meta=self.table_meta_mock,
            entity_col=self.entity_col)

    def prep_for_integration(self):
        '''
        Creates a full fledged prediction problem generator without
        a mocked out ensure_valid_inputs method
        '''
        meta_json_str = ' \
            {"path": "", \
             "tables": [ \
                {"path": "synthetic_taxi_data.csv",\
                 "name": "taxi_data", \
                 "fields": [ \
                {"name": "vendor_id", "type": "id"},\
                {"name": "taxi_id", "type": "id"}, \
                {"name": "trip_id", "type": "datetime"}, \
                {"name": "distance", "type": "number", "subtype": "float"}, \
                {"name": "duration", "type": "number", "subtype": "float"}, \
                {"name": "fare", "type": "number", "subtype": "float"}, \
                {"name": "num_passengers", "type": "number", \
                    "subtype": "float"} \
                 ]}]}'

        self.table_meta = TableMeta.from_json(meta_json_str)

        self.generator = PredictionProblemGenerator(
            table_meta=self.table_meta,
            entity_col=self.entity_col)

    def create_patch(self, name, return_value=None):
        '''helper method for creating patches'''
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)

        if return_value:
            thing.return_value = return_value

        return thing

    def test_generate(self):
        self.prep_for_integration()
        self.assertIsNotNone(self.generator.generate)
        self.generator.generate()


class TestPredictionProblemGeneratorValidation(unittest.TestCase):
    '''
    TestPredictionProblemGeneratorValidation has its own class, because unlike
    other tests, the ensure_valid_inputs method cannot be mocked out.
    '''

    def create_patch(self, name, return_value=None):
        '''helper method for creating patches'''
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)

        if return_value:
            thing.return_value = return_value

        return thing

    def test_ensure_valid_imputs(self):
        table_meta_mock = MagicMock()
        entity_col = "taxi_id"

        # set up table_meta types
        table_meta_mock.get_type.return_value = True
        table_meta_patch = self.create_patch(
            'trane.core.prediction_problem_generator.TableMeta', 'tm_patch')
        table_meta_patch.TYPE_IDENTIFIER = True
        table_meta_patch.TYPE_FLOAT = True
        table_meta_patch.TYPE_TIME = True

        # create generator
        generator = PredictionProblemGenerator(
            table_meta=table_meta_mock,
            entity_col=entity_col)

        self.assertIsNotNone(generator.ensure_valid_inputs)
        table_meta_mock.get_type.assert_has_calls([
            call(entity_col)], any_order=True)
