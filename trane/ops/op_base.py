import json

__all__ = ['OpBase']

class OpBase(object):
    """Super class of all operations. 
        All operations should have PARAMS and IOTYPES.
    
    IOTYPES is a list of possible input and output type pairs.
        For example `greater` can operate on int and str and output bool.
        [(int, bool), (str, bool), ...]
    
    PARAMS is a list of parameter and type dicts. 
        PARAMS have the same length as IOTYPES.
        With different input types, parameters may have different types. 
        For example the PARAMS of `greater` is
        [\{threshold: int\}, \{threshold: str\}, ...]
    
    itype and otype are the actual input and output type.
        param_values is a dict of parameter name and value.
    
    """
    
    PARAMS = None
    IOTYPES = None
    
    def __init__(self, column_name):
        """Initalization of all operations. Subclasses shouldn't have their own init.
            This function checks whether PARAMS and IOTYPES are defined and compatable. 

        args:
            column_name: the column this operation is applied on. 
        
        """
        self.column_name = column_name
        self.itype = self.otype = None
        self.param_values = {}
        assert(self.PARAMS and self.IOTYPES), type(self).__name__
        assert(len(self.PARAMS) == len(self.IOTYPES)), type(self).__name__

    def op_type_check(self, table_meta):
        """Data type check for the operation. 
            Operations may change the data type of a column, eg. int -> bool. 
            One operation can only be applied on a few data types, eg. `greater` can 
            be applied on int but can't be applied on bool.
            This function checks whether current operation can be applied on the data.
            It returns the updated TableMeta for next operation.
        
        args:
            table_meta: table meta before this operation.
        
        returns:
            TableMeta: table meta after this operation. None if not compatable.
        
        """
        self.itype = table_meta.get_type(self.column_name)
        for idx, (itype, otype) in enumerate(self.IOTYPES):
            if self.itype == itype:
                self.otype = otype
                table_meta.set_type(self.column_name, otype)
                # NOTE when preprocessing, we set default param_values.
                for param, ptype in self.PARAMS[idx].items():
                    self.param_values[param] = 0
                return table_meta
        return None

    def __call__(self, dataframe):
        return self.execute(dataframe)

    def execute(self, dataframe):
        raise NotImplementedError

    def __str__(self):
        return "%s(%s)" % (type(self).__name__, self.column_name)
