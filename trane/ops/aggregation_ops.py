from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

AGGREGATION_OPS = ["FirstAggregationOp", "CountAggregationOp", "SumAggregationOp",
                   "LastAggregationOp", "LMFAggregationOp"]
__all__ = ["AggregationOpBase", "AGGREGATION_OPS"] + AGGREGATION_OPS


class AggregationOpBase(OpBase):

    """
    Super class for all Aggregation Operations. The class is empty and is currently a
    placeholder for any AggregationOpBase level methods we want to make.

    Aggregation operations represent the 4th and final operation
    in a prediction problem. They aggregate data from many rows into
    a single row. The final output of the problem is the value in that row
    at the label generating column. Aggregation operations are defined as classes
    that inherit the AggregationOpBase class and instantiate the execute method.

    Make Your Own
    -------------
    Simply make a new class that follows the requirements below and issue a pull request.

    Requirements
    ------------
    REQUIRED_PARAMETERS: the hyper parameters needed for the operation
    IOTYPES: the input and output types of the operation using TableMeta types
    execute method: transform dataframe according to the operation and return
      the new dataframe

    """


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
               (TM.TYPE_INTEGER, TM.TYPE_INTEGER)]

    def execute(self, dataframe):
        dataframe = dataframe.copy()
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
        head = dataframe.head(1).copy()
        count = int(dataframe.shape[0])
        head[self.column_name] = count
        return head


class SumAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_FLOAT), (TM.TYPE_BOOL, TM.TYPE_FLOAT),
               (TM.TYPE_INTEGER, TM.TYPE_FLOAT)]

    def execute(self, dataframe):
        head = dataframe.head(1).copy()
        total = float(dataframe[self.column_name].sum())
        head[self.column_name] = total
        return head
