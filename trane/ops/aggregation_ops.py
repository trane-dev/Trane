from trane.column_schema import ColumnSchema
from trane.logical_types import Double, Integer
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
    Given a dataframe, and column, return 1 value.

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

    Filter operations filter data
    row operations transform data within a row and return a dataframe of the same dimensions,
    transformation operations transform data across rows and return a new dataset with fewer rows,
    aggregation operations accumulate the dataframe into a single row.

    """


class CountAggregationOp(AggregationOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(),
            ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        ),
    ]

    def op_type_check(self, table_meta):
        self.output_type = ColumnSchema(logical_type=Integer, semantic_tags={"numeric"})
        return ColumnSchema(logical_type=Integer, semantic_tags={"numeric"})

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

    def __init__(self, column_name):
        self.column_name = column_name
        self.input_type = ColumnSchema(semantic_tags={"numeric"})
        self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        if table_meta[self.column_name].is_numeric:
            return table_meta

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

    def __init__(self, column_name):
        self.column_name = column_name
        self.input_type = ColumnSchema(semantic_tags={"numeric"})
        self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        if table_meta[self.column_name].is_numeric:
            return table_meta

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

    def __init__(self, column_name):
        self.column_name = column_name
        self.input_type = ColumnSchema(semantic_tags={"numeric"})
        self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        if table_meta[self.column_name].is_numeric:
            return table_meta

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

    def __init__(self, column_name):
        self.column_name = column_name
        self.input_type = ColumnSchema(semantic_tags={"numeric"})
        self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        if table_meta[self.column_name].is_numeric:
            return table_meta

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

    def __init__(self, column_name):
        self.column_name = column_name
        self.input_type = ColumnSchema(semantic_tags={"category"})
        # doesn't seem right
        # self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        if table_meta[self.column_name].is_numeric:
            self.output_type = ColumnSchema(
                logical_type=table_meta[self.column_name].logical_type,
                semantic_tags={"numeric"},
            )
            return table_meta
        if table_meta[self.column_name].is_categorical:
            self.output_type = ColumnSchema(
                logical_type=table_meta[self.column_name].logical_type,
                semantic_tags={"category"},
            )
            return table_meta

    def execute(self, dataframe):
        if len(dataframe) == 0:
            return None
        return str(dataframe[self.column_name].mode()[0])
