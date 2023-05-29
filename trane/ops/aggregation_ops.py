from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Double,
    Integer,
)

from trane.ops.op_base import OpBase

AGGREGATION_OPS = [
    "CountAggregationOp",
    "SumAggregationOp",
    "AvgAggregationOp",
    "MaxAggregationOp",
    "MinAggregationOp",
    "MajorityAggregationOp",
]


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


class CountAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(),
            ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        ),
    ]

    def execute(self, dataframe):
        return len(dataframe)


class SumAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(semantic_tags={"numeric"}),
            ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        ),
    ]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return dataframe[self.column_name].sum()


class AvgAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(semantic_tags={"numeric"}),
            ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        ),
    ]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return dataframe[self.column_name].mean()


class MaxAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(semantic_tags={"numeric"}),
            ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        ),
    ]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return dataframe[self.column_name].max()


class MinAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(semantic_tags={"numeric"}),
            ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        ),
    ]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return dataframe[self.column_name].min()


class MajorityAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(semantic_tags={"category"}),
            ColumnSchema(semantic_tags={"category"}),
        ),
        (
            ColumnSchema(semantic_tags={"index"}),
            ColumnSchema(semantic_tags={"index"}),
        ),
    ]

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return str(dataframe[self.column_name].mode()[0])
