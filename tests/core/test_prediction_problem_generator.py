"""TESTING STRATEGY
Function: generate(self)
1. Ensure the correct # of problems are generated.
2. Generated problems are of proper type
3. Ensure prediction problems are generated in order of
   Filter->Row->Transformation->Aggregation
"""
import unittest

import pandas as pd
from mock import MagicMock, call, patch

from trane.core.prediction_problem_generator import PredictionProblemGenerator
from trane.ops.aggregation_ops import *  # noqa
from trane.ops.filter_ops import *  # noqa
from trane.ops.row_ops import *  # noqa
from trane.ops.transformation_ops import *  # noqa
from trane.utils.table_meta import TableMeta


class TestPredictionProblemGenerator(unittest.TestCase):

    def setUp(self):
        self.table_meta_mock = MagicMock()
        self.entity_id_col = "taxi_id"
        self.label_generating_col = "fare"
        self.time_col = "trip_id"
        self.filter_col = "taxi_id"

        self.ensure_valid_inputs_patch = self.create_patch(
            'trane.core.PredictionProblemGenerator.ensure_valid_inputs')

        self.generator = PredictionProblemGenerator(
            table_meta=self.table_meta_mock,
            entity_id_col=self.entity_id_col,
            label_generating_col=self.label_generating_col,
            time_col=self.time_col,
            filter_col=self.filter_col)

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
        self.df = pd.DataFrame(
            [(0, 0, 0, 5.32, 19.7, 53.89, 1),
             (0, 0, 1, 1.08, 6.78, 18.89, 2),
             (0, 0, 2, 4.69, 14.11, 41.35, 4)],
            columns=["vendor_id", "taxi_id", "trip_id", "distance", "duration",
                     "fare", "num_passengers"])

        self.generator = PredictionProblemGenerator(
            table_meta=self.table_meta,
            entity_id_col=self.entity_id_col,
            label_generating_col=self.label_generating_col,
            time_col=self.time_col,
            filter_col=self.filter_col)

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
        self.generator.generate(self.df)


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
        entity_id_col = "taxi_id"
        label_generating_col = "fare"
        time_col = "trip_id"
        filter_col = "taxi_id"

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
            entity_id_col=entity_id_col,
            label_generating_col=label_generating_col,
            time_col=time_col,
            filter_col=filter_col)

        self.assertIsNotNone(generator.ensure_valid_inputs)
        table_meta_mock.get_type.assert_has_calls([
            call(entity_id_col),
            call(label_generating_col),
            call(time_col)], any_order=True)
