from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase
import sys
import numpy

AGGREGATION_OPS = ["FirstAggregationOp", "CountAggregationOp", "SumAggregationOp",
                   "LastAggregationOp", "LMFAggregationOp"]
__all__ = ["AggregationOpBase", "AGGREGATION_OPS"] + AGGREGATION_OPS

import logging


class AggregationOpBase(OpBase):
    """super class for all Aggregation Operations. (deprecated)"""
    pass


class FirstAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        return dataframe.head(1)


class LastAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        return dataframe.tail(1)


class LMFAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):

        #HACKY FIX
        dataframe[self.column_name] = dataframe[self.column_name].astype(numpy.float32)

        last = dataframe.tail(1)
        first = dataframe.head(1)

        logging.debug("Beginning exuection of LMFAggregationOp")
        logging.info('Last Value: {}\n'.format(last.at[last.index[0],
                                                       self.column_name]))
        logging.info('First Value: {}\n'.format(first.at[first.index[0],
                                                        self.column_name]))
        last.at[last.index[0],
                self.column_name] -= first.at[first.index[0], self.column_name]
        return last


class CountAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_FLOAT),
               (TM.TYPE_ORDERED, TM.TYPE_FLOAT), (TM.TYPE_TEXT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_FLOAT), (TM.TYPE_IDENTIFIER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        first = dataframe.head(1)
        first.at[first.index[0], self.column_name] = dataframe.shape[0]
        return first


class SumAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        first = dataframe.head(1)
        first.at[first.index[0], self.column_name] = dataframe[
            self.column_name].sum()
        return first
