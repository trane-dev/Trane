import json

__all__ = ['OpBase']

class OpBase(object):
    """docstring for OpBase."""
    
    PARAMS = None
    IOTYPES = None
    
    def __init__(self, column_name):
        self.column_name = column_name
        self.itype = self.otype = None
        self.param_values = {}
        assert(self.PARAMS and self.IOTYPES), type(self).__name__
        assert(len(self.PARAMS) == len(self.IOTYPES)), type(self).__name__

    def preprocess(self, table_meta):
        self.itype = table_meta.get_type(self.column_name)
        for idx, (itype, otype) in enumerate(self.IOTYPES):
            if self.itype == itype:
                self.otype = otype
                table_meta.set_type(self.column_name, otype)
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

    def generate_nl_description(self):
        raise NotImplementedError
