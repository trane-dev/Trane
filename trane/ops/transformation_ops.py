from trane.ops.op_base import OpBase


class TransformationOpBase(OpBase):
    """
    Super class for all Transformation Operations. The class is empty and is currently
    a placeholder for any TransformationOpBase level methods we want to make.

    The output of one transformation operation produces a new table
    which can be fed into the input of another one. Transformation
    operations are defined as classes that inherit the
    TransformationOpBase class and instantiate the execute method.
    """


class IdentityOp(TransformationOpBase):
    input_output_types = [("None", "None")]
    description = ""

    def generate_description(self):
        return self.description

    def label_function(self, dataslice):
        return dataslice


class OrderByOp(TransformationOpBase):
    input_output_types = [("numeric", "Double")]
    description = " sorted by <{}>"
    restricted_ops = {
        "CountAggregationOp",
        "SumAggregationOp",
        "AvgAggregationOp",
        "MaxAggregationOp",
        "MinAggregationOp",
        "MajorityAggregationOp",
        "ExistsAggregationOp",
    }

    def generate_description(self):
        return self.description.format(self.column_name)

    def label_function(self, dataslice):
        return dataslice.sort_values(by=self.column_name)
