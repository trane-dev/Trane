from trane.ops.op_base import OpBase


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

    Filter operations filter data
    row operations transform data within a row and return a dataframe of the same dimensions,
    transformation operations transform data across rows and return a new dataset with fewer rows,
    aggregation operations accumulate the dataframe into a single row.

    """


class CountAggregationOp(AggregationOpBase):
    """
    CountAggregation will not be given any columns.
    It will apply to the whole dataslice and return 1 number (integer).
    Basically, its then number of rows in the dataslice.
    So a customer's transactions (within the window_size).
    """

    input_output_types = [("None", "Integer")]
    description = " the number of records"
    restricted_semantic_tags = {"time_index", "primary_key"}

    def label_function(self, dataslice):
        return len(dataslice)


class SumAggregationOp(AggregationOpBase):
    input_output_types = [("numeric", "Double")]
    description = " the total <{}> in all related records"

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return None
        return dataslice[self.column_name].sum()


class AvgAggregationOp(AggregationOpBase):
    input_output_types = [("numeric", "Double")]
    description = " the average <{}> in all related records"

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return None
        return dataslice[self.column_name].mean()


class MaxAggregationOp(AggregationOpBase):
    input_output_types = [("numeric", "Double")]
    description = " the maximum <{}> in all related records"

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return None
        return dataslice[self.column_name].max()


class MinAggregationOp(AggregationOpBase):
    input_output_types = [("numeric", "Double")]
    description = " the minimum <{}> in all related records"

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return None
        return dataslice[self.column_name].min()


class MajorityAggregationOp(AggregationOpBase):
    input_output_types = [("category", "category")]
    # input_output_types = [("category", "category"), ("primary_key", "primary_key")]
    description = " the majority <{}> in all related records"

    def label_function(self, dataslice):
        if len(dataslice) == 0:
            return None
        return str(dataslice[self.column_name].mode()[0])


class ExistsAggregationOp(AggregationOpBase):
    input_output_types = [("None", "Boolean")]
    description = " if there exists a record"
    restricted_semantic_tags = {"time_index", "primary_key"}

    def label_function(self, dataslice):
        return len(dataslice) > 0


# class FirstAggregationOp(AggregationOpBase):
#     input_output_types = [("category", "category")]
#     description = " the first <{}> in all related records"
#     def label_function(self, dataslice):
#         return dataslice[self.column_name].iloc[0]

# class LastAggregationOp(AggregationOpBase):
#     input_output_types = [("category", "category")]
#     description = " the last <{}> in all related records"
#     def label_function(self, dataslice):
#         return dataslice[self.column_name].iloc[-1]
