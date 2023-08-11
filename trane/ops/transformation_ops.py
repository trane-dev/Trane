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


class OrderByOp(TransformationOpBase):
    input_output_types = [("numeric", "Double")]
    description = " sorted by <{}>"

    def generate_description(self):
        if self.column_name is None:
            return ""
        return self.description.format(self.column_name)

    def label_function(self, dataslice):
        return dataslice.sort_values(by=self.column_name)
