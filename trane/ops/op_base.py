__all__ = ['OpBase']


class OpBase(object):
    """
    Super class of all operations.
    All operations should have REQUIRED_PARAMETERS and IOTYPES.

    IOTYPES is a list of possible input and output type pairs.
        For example `greater` can operate on int and str and output bool.
        [(int, bool), (str, bool), ...]

    REQUIRED_PARAMETERS is a list of parameter and type dicts.

    hyper_parameter_settings is a dict of parameter name and value.

    """

    REQUIRED_PARAMETERS = None
    IOTYPES = None

    def __init__(self, column_name):
        """
        Initalization of all operations. Subclasses shouldn't have their own init.

        Parameters
        ----------
        column_name: the column this operation applies to

        Returns
        ----------
        None
        """
        self.column_name = column_name
        self.input_type = None
        self.output_type = None
        self.hyper_parameter_settings = {}

    def op_type_check(self, table_meta):
        """
        Data type check for the operation.
        Operations may change the data type of a column, eg. int -> bool.
        One operation can only be applied on a few data types, eg. `greater` can
        be applied on int but can't be applied on bool.
        This function checks whether the current operation can be applied on the data.
        It returns the updated TableMeta for next operation or None if it's not valid.

        Parameters
        ----------
        table_meta: table meta before this operation.

        Returns
        ----------
        table_meta: table meta after this operation. None if not compatable.

        """
        self.input_type = table_meta.get_type(self.column_name)
        for idx, (input_type, output_type) in enumerate(self.IOTYPES):
            if self.input_type == input_type:
                self.output_type = output_type
                table_meta.set_type(self.column_name, output_type)
                return table_meta
        return None

    def set_hyper_parameter(self, hyper_parameter):
        """
        Set the hyper parameter of the operation.

        Parameters
        ----------
        hyper_parameter: value for the hyper parameter

        Returns
        ----------
        None

        """
        for parameter_requirement in self.REQUIRED_PARAMETERS:
            for parameter_name, parameter_type in parameter_requirement.items():
                self.hyper_parameter_settings[parameter_name] = hyper_parameter

    def __call__(self, dataframe):
        return self.execute(dataframe)

    def execute(self, dataframe):
        raise NotImplementedError

    def __hash__(self):
        return hash((type(self).__name__, self.column_name))

    def __str__(self):
        return "%s(%s)" % (type(self).__name__, self.column_name)

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False
