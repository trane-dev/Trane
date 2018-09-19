from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

AGGREGATION_OPS = [
    "FirstAggregationOp", "CountAggregationOp", "SumAggregationOp",
    "LastAggregationOp"]
__all__ = ["AggregationOpBase", "AGGREGATION_OPS"] + AGGREGATION_OPS


class AggregationOpBase(OpBase):

    """
    Super class for all Aggregation Operations. The class is empty and is
    currently a placeholder for any AggregationOpBase level methods we want to
    make.

    Aggregation operations represent the 4th and final operation
    in a prediction problem. They aggregate data from many rows into
    a single row. The final output of the problem is the value in that row
    at the label generating column. Aggregation operations are defined as
    classes that inherit the AggregationOpBase class and instantiate the
    execute method.

    Make Your Own
    -------------
    Simply make a new class that follows the requirements below and issue a
    pull request.

    Requirements
    ------------
    REQUIRED_PARAMETERS: the hyper parameters needed for the operation
    IOTYPES: the input and output types of the operation using TableMeta types
    execute method: transform dataframe according to the operation and return
      the new dataframe

    """


class FirstAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY),
               (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED),
               (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER),
               (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME),
               (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        if len(dataframe) > 0:
            return dataframe.head(1)[self.column_name]
        return None


class LastAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_CATEGORY),
               (TM.TYPE_BOOL, TM.TYPE_BOOL),
               (TM.TYPE_ORDERED, TM.TYPE_ORDERED),
               (TM.TYPE_TEXT, TM.TYPE_TEXT),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER),
               (TM.TYPE_FLOAT, TM.TYPE_FLOAT),
               (TM.TYPE_TIME, TM.TYPE_TIME),
               (TM.TYPE_IDENTIFIER, TM.TYPE_IDENTIFIER)]

    def execute(self, dataframe):
        if len(dataframe) > 0:
            return dataframe.last(1)[self.column_name]
        return None


class CountAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_CATEGORY, TM.TYPE_INTEGER),
               (TM.TYPE_BOOL, TM.TYPE_INTEGER),
               (TM.TYPE_ORDERED, TM.TYPE_INTEGER),
               (TM.TYPE_TEXT, TM.TYPE_INTEGER),
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER),
               (TM.TYPE_FLOAT, TM.TYPE_INTEGER),
               (TM.TYPE_TIME, TM.TYPE_INTEGER),
               (TM.TYPE_IDENTIFIER, TM.TYPE_INTEGER)]

    def execute(self, dataframe):
        return len(dataframe)


class SumAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        return float(dataframe[self.column_name].sum())
