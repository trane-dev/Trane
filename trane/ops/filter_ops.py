from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Categorical,
    Double,
    Integer,
    Ordinal,
    PostalCode,
)

from trane.ops.op_base import OpBase

FILTER_OPS = [
    "AllFilterOp",
    "GreaterFilterOp",
    "EqFilterOp",
    "NeqFilterOp",
    "LessFilterOp",
]


class FilterOpBase(OpBase):
    """
    Super class for all Filter Operations. The class is empty and is currently
    a placeholder for any FilterOpBase level methods we want to make.

    Filter operations represent the 1st operation in a prediction problem.
    They filter out rows based on values in the filter_column. Filter
    operations are defined as classes that inherit the FilterOpBase class and
    instantiate the execute method.

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


class AllFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = []

    def op_type_check(self, table_meta):
        return table_meta

    def execute(self, dataframe):
        return dataframe


class EqFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": None}]
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
        self.input_type = [
            ColumnSchema(semantic_tags={"index"}),
            ColumnSchema(semantic_tags={"category"}),
        ]
        # doesn't seem right
        # self.output_type = ColumnSchema(logical_type=Boolean)
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        semantic_tags = table_meta[self.column_name].semantic_tags
        if "index" not in semantic_tags or "category" not in semantic_tags:
            return None
        if not isinstance(
            table_meta[self.column_name].logical_type,
            (Integer, Categorical, Ordinal, PostalCode),
        ):
            return None

        if "numeric" in semantic_tags:
            self.output_type = ColumnSchema(
                logical_type=table_meta[self.column_name].logical_type,
                semantic_tags={"numeric"},
            )
        if "category" in semantic_tags:
            self.output_type = ColumnSchema(
                logical_type=table_meta[self.column_name].logical_type,
                semantic_tags={"category"},
            )

        return table_meta

    def execute(self, dataframe):
        return dataframe[
            dataframe[self.column_name] == self.hyper_parameter_settings["threshold"]
        ]


class NeqFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": None}]
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
        self.input_type = [
            ColumnSchema(semantic_tags={"index"}),
            ColumnSchema(semantic_tags={"category"}),
        ]
        # doesn't seem right
        # self.output_type = ColumnSchema(logical_type=Boolean)
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        semantic_tags = table_meta[self.column_name].semantic_tags
        if "index" not in semantic_tags or "category" not in semantic_tags:
            return None
        if not isinstance(
            table_meta[self.column_name].logical_type,
            (Integer, Categorical, Ordinal, PostalCode),
        ):
            return None

        if "numeric" in semantic_tags:
            self.output_type = ColumnSchema(
                logical_type=table_meta[self.column_name].logical_type,
                semantic_tags={"numeric"},
            )
        if "category" in semantic_tags:
            self.output_type = ColumnSchema(
                logical_type=table_meta[self.column_name].logical_type,
                semantic_tags={"category"},
            )

        return table_meta

    def execute(self, dataframe):
        return dataframe[
            dataframe[self.column_name] != self.hyper_parameter_settings["threshold"]
        ]


class GreaterFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": None}]
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
        self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        if "numeric" not in table_meta[self.column_name].semantic_tags:
            return None
        table_meta[self.column_name] = ColumnSchema(
            logical_type=Double,
            semantic_tags={"numeric"},
        )
        return table_meta

    def execute(self, dataframe):
        return dataframe[
            dataframe[self.column_name] > self.hyper_parameter_settings["threshold"]
        ]


class LessFilterOp(FilterOpBase):
    REQUIRED_PARAMETERS = [{"threshold": None}]
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
        self.output_type = ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
        if "numeric" not in table_meta[self.column_name].semantic_tags:
            return None
        table_meta[self.column_name] = ColumnSchema(
            logical_type=Double,
            semantic_tags={"numeric"},
        )
        return table_meta

    def execute(self, dataframe):
        return dataframe[
            dataframe[self.column_name] < self.hyper_parameter_settings["threshold"]
        ]
