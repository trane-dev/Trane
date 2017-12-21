import copy
import json

__all__ = ['TableMeta']

class TableMeta(object):
    """
    Meta data of a database table. 
    """
    TYPE_IDENTIFIER = 'identifier'
    TYPE_TEXT = 'text'
    TYPE_TIME = 'time'
    TYPE_VALUE = 'value'
    TYPE_CATEGORY = 'category'
    TYPE_BOOL = 'boolean'
    TYPES = [TYPE_IDENTIFIER, TYPE_TEXT, TYPE_TIME, TYPE_VALUE, TYPE_CATEGORY, TYPE_BOOL]
    
    def __init__(self, table_meta):
        """
        args:
            table_meta: a list of dict. each dict describe a column. the dict includes 'name' and 'type'.
            [{'name': 'col1', 'type': 'value'}, ...]
        """
        table_meta = [(item['name'], item) for item in table_meta]
        self.table_meta = dict(table_meta)
            
    def get_type(self, column_name):
        """
        Get the type of a column.
        args:
            column_name: str
        returns:
            str: column type
        """
        return self.table_meta[column_name]['type']
    
    def set_type(self, column_name, dtype):
        """
        Change the type of a column.
        args:
            column_name: str
            dtype: str in TYPES
        returns:
            None
        """
        self.table_meta[column_name]['type'] = dtype
    
    def get_columns(self):
        """
        Get all the column names.
        returns:
            list of str: column names
        """
        return self.table_meta.keys()
        
    def copy(self):
        """
        Make a deep copy of self.
        returns:
            TableMeta: a copy
        """
        return copy.deepcopy(self)

    def to_json(self):
        """
        Convert to json str
        returns:
            str: json str of self
        """
        return json.dumps(list(self.table_meta.values()))

    def from_json(json_data):
        """
        Load from json str.
        args:
            json_data: json str
        returns:
            TableMeta
        """
        return TableMeta(json.loads(json_data))
