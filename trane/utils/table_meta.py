import copy
import json

__all__ = ['TableMeta']

class TableMeta(object):
    """
    
    """
    TYPE_IDENTIFIER = 'identifier'
    TYPE_TEXT = 'text'
    TYPE_TIME = 'time'
    TYPE_VALUE = 'value'
    TYPE_CATEGORY = 'category'
    TYPE_BOOL = 'boolean'
    TYPES = [TYPE_IDENTIFIER, TYPE_TEXT, TYPE_TIME, TYPE_VALUE, TYPE_CATEGORY, TYPE_BOOL]
    
    def __init__(self, table_meta):
        table_meta = [(item['name'], item) for item in table_meta]
        self.table_meta = dict(table_meta)
            
    def get_type(self, column_name):
        return self.table_meta[column_name]['type']
    
    def set_type(self, column_name, dtype):
        self.table_meta[column_name]['type'] = dtype
    
    def get_columns(self):
        return self.table_meta.keys()
        
    def copy(self):
        return copy.deepcopy(self)

    def to_json(self):
        return json.dumps(list(self.table_meta.values()))

    def from_json(json_data):
        return TableMeta(json.loads(json_data))
