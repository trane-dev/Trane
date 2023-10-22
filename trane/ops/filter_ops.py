import pandas as pd

from trane.ops.op_base import OpBase


class FilterOpBase(OpBase):
    """
    Super class for all Filter Operations. The class is empty and is currently
    a placeholder for any FilterOpBase level methods we want to make.

    Filter operations represent the 1st operation in a prediction problem.
    They filter out rows based on values in the filter_column. Filter
    operations are defined as classes that inherit the FilterOpBase class and
    instantiate the execute method.

    Old, v1 version
    Filter operations filter data
    row operations transform data within a row and return a dataframe of the same dimensions,
    transformation operations transform data across rows and return a new dataset with fewer rows,
    aggregation operations accumulate the dataframe into a single row.

    New, v2 version
    Filter operations filter data (return a subset of the rows)
    Aggregation operations aggregate data from many rows into a single row.

    """

    restricted_ops = set()

    def has_parameters_set(self):
        if self.required_parameters is None:
            return True
        for parameter in self.required_parameters:
            if getattr(self, parameter) is None:
                return False
        return True

    def generate_description(self):
        if self.required_parameters is None:
            return self.description
        else:
            return self.description.format(
                self.column_name,
                self.threshold or self.required_parameters["threshold"].__name__,
            )


class AllFilterOp(FilterOpBase):
    input_output_types = [("None", "None")]
    description = ""
    required_parameters = None

    def generate_description(self):
        return self.description

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return pd.NA
        return dataslice


class EqFilterOp(FilterOpBase):
    input_output_types = [("category", "category")]
    description = " with <{}> equal to <{}>"
    required_parameters = {"threshold": str}

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataslice):
        return dataslice[dataslice[self.column_name] == self.threshold]


class NeqFilterOp(FilterOpBase):
    input_output_types = [("category", "category")]
    description = " with <{}> not equal to <{}>"
    required_parameters = {"threshold": str}

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataslice):
        return dataslice[dataslice[self.column_name] != self.threshold]


class GreaterFilterOp(FilterOpBase):
    input_output_types = [("numeric", "Double")]
    description = " with <{}> greater than <{}>"
    required_parameters = {"threshold": float}

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataslice):
        return dataslice[dataslice[self.column_name] > self.threshold]


class LessFilterOp(FilterOpBase):
    input_output_types = [("numeric", "Double")]
    description = " with <{}> less than <{}>"
    required_parameters = {"threshold": float}

    def set_parameters(self, threshold: float):
        self.threshold = threshold

    def label_function(self, dataslice):
        return dataslice[dataslice[self.column_name] < self.threshold]
