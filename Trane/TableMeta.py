class TableMeta(object):
    """
    
    """
    TYPE_IDENTIFIER = 'identifier'
    TYPE_TEXT = 'text'
    TYPE_TIME = 'time'
    TYPE_VALUE = 'value'
    TYPE_CATEGORY = 'category'
    TYPES = [TYPE_IDENTIFIER, TYPE_TEXT, TYPE_TIME, TYPE_VALUE, TYPE_CATEGORY]
    
    def __init__(self, table_meta):
        table_meta = [(item['name'], item)for item in table_meta]
        self.table_meta = dict(table_meta)
        
    def get_type(self, column_name):
        return self.table_meta[column_name]['type']
    
    def get_columns(self):
        return self.table_meta.keys()
        
        
