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
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        return dataframe.head(1)


class LastAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY), (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED), (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME), (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        return dataframe.tail(1)


class LMFAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()

        #HACKY FIX
        dataframe[self.column_name] = dataframe[self.column_name].astype(numpy.float32)

        last = dataframe.tail(1)
        first = dataframe.head(1)
        last.at[last.index[0],
                self.column_name] -= first.at[first.index[0], self.column_name]
        return last


class CountAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_INTEGER), (TM.TYPE_BOOL, TM.TYPE_INTEGER),
               (TM.TYPE_ORDERED, TM.TYPE_INTEGER), (TM.TYPE_TEXT, TM.TYPE_INTEGER),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER), (TM.TYPE_FLOAT, TM.TYPE_INTEGER),
               (TM.TYPE_TIME, TM.TYPE_INTEGER), (TM.TYPE_IDENTIFIER, TM.TYPE_INTEGER)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        head = dataframe.head(1)
        head[self.column_name] = int(dataframe.shape[0])
        return head


class SumAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
        head = dataframe.head(1)
        head[self.column_name] = float(dataframe[self.column_name].sum())
        return head
