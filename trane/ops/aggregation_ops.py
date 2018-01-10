from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

AGGREGATION_OPS = ["FirstAggregationOp", "CountAggregationOp", "SumAggregationOp",\
    "LastAggregationOp", "LMFAggregationOp"]
__all__ = ["AggregationOpBase", "AGGREGATION_OPS"] + AGGREGATION_OPS

class AggregationOpBase(OpBase):
    """
    super class for all Aggregation Operations. (deprecated)
    """
    pass

class FirstAggregationOp(AggregationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        return dataframe.head(1)
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return " whether the next"
        return " the next"

class LastAggregationOp(AggregationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, dataframe):
        return dataframe.tail(1)
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return " whether the last"
        return " the last"

class LMFAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, dataframe):
        last = dataframe.tail(1)
        first = dataframe.head(1)
        last.at[last.index[0], self.column_name] -= first.at[first.index[0], self.column_name]
        return last
    def generate_nl_description(self):
        return " the last minus first"

class CountAggregationOp(AggregationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_VALUE)]
    def execute(self, dataframe):
        first = dataframe.head(1)
        first.at[first.index[0], self.column_name] = dataframe.shape[0]
        return first
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return " the number of records whose"
        return " the number of records over"

class SumAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, dataframe):
        first = dataframe.head(1)
        first.at[first.index[0], self.column_name] = dataframe[self.column_name].sum()
        return first
    def generate_nl_description(self):
        return " the total value of"
