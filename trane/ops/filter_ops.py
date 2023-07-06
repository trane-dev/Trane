import pandas as pd

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

    Filter operations filter data
    row operations transform data within a row and return a dataframe of the same dimensions,
    transformation operations transform data across rows and return a new dataset with fewer rows,
    aggregation operations accumulate the dataframe into a single row.

    """


class AllFilterOp(FilterOpBase):
    input_output_types = [(None, None)]
    description = ""

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return pd.NA
        return dataslice


class EqFilterOp(FilterOpBase):
    input_output_types = [("category", "category")]
    # input_output_types = [("category", "category"), ("index", "index")]
    description = "equal to"

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataslice):
        return dataslice[dataslice[self.column_name] == self.threshold]


class NeqFilterOp(FilterOpBase):
    input_output_types = [("category", "category")]
    # input_output_types = [("category", "category"), ("index", "index")]
    description = "not equal to"

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataframe):
        return dataframe[dataframe[self.column_name] != self.threshold]


class GreaterFilterOp(FilterOpBase):
    input_output_types = [("numeric", "Double")]
    description = "greater than"

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataframe):
        return dataframe[dataframe[self.column_name] > self.threshold]


class LessFilterOp(FilterOpBase):
    input_output_types = [("numeric", "Double")]
    description = "less than"

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataframe):
        return dataframe[dataframe[self.column_name] < self.threshold]


FILT_OPS = [
    AllFilterOp,
    GreaterFilterOp,
    EqFilterOp,
    NeqFilterOp,
    LessFilterOp,
]
