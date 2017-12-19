from ..utils.table_meta import TableMeta as TM
from .op_base import OpBase

AGGREGATION_OPS = ["FirstAggregationOp", "LastAggregationOp", "FMLAggregationOp",
    "CountAggregationOp", "SumAggregationOp"]

class AggregationOpBase(OpBase):
    pass

class FirstAggregationOp(AggregationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return " whether the next"
        return " the next"

class LastAggregationOp(AggregationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_BOOL)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return " whether the last"
        return " the last"

class FMLAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " the last minus first"

class CountAggregationOp(AggregationOpBase):
    PARAMS = [{}, {}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE), (TM.TYPE_BOOL, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        if self.itype == TM.TYPE_BOOL:
            return " the number of records whose"
        return " the number of records over"

class SumAggregationOp(AggregationOpBase):
    PARAMS = [{}]
    IOTYPES = [(TM.TYPE_VALUE, TM.TYPE_VALUE)]
    def execute(self, data_frame):
        pass
    def generate_nl_description(self):
        return " the total value of"
