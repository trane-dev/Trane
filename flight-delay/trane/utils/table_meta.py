import copy
import json

__all__ = ['TableMeta']


class TableMeta(object):
    """
    Meta data object of a database table.
    More information available here: https://hdi-project.github.io/MetaData.json/index
    """
    SUPERTYPE = {}
    # categorical
    TYPE_CATEGORY = 'categorical'
    TYPE_BOOL = 'boolean'
    TYPE_ORDERED = 'ordered'
    SUPERTYPE['categorical'] = 'categorical'
    SUPERTYPE['boolean'] = 'categorical'
    SUPERTYPE['ordered'] = 'categorical'
    # text
    TYPE_TEXT = 'text'
    # number
    TYPE_INTEGER = 'integer'
    TYPE_FLOAT = 'float'
    SUPERTYPE['integer'] = 'number'
    SUPERTYPE['float'] = 'number'
    # datetime
    TYPE_TIME = 'datetime'
    # id
    TYPE_IDENTIFIER = 'id'

    TYPES = [TYPE_CATEGORY, TYPE_BOOL, TYPE_ORDERED, TYPE_TEXT,
             TYPE_INTEGER, TYPE_FLOAT, TYPE_TIME, TYPE_IDENTIFIER]

    def __init__(self, table_meta):
        """
        Initalization of all operations. Subclasses shouldn't have their own init.

        Parameters
        ----------
        table_meta: a dict describe meta data of a database.

        Returns
        -------
        None
        """
        self.table_meta = table_meta.copy()
        self.all_columns = {}
        for table_id, table in enumerate(self.table_meta['tables']):
            for field_id, field in enumerate(table['fields']):
                self.all_columns[field['name']] = {
                    'table_id': table_id,
                    'field_id': field_id,
                    'type': field['subtype'] if 'subtype' in field else field['type'],
                    'properties': field['properties'] if 'properties' in field else None
                }

    def add_column(self, field_name, field_type):
        self.all_columns[field_name] = {
            "type": field_type
        }

    def get_type(self, column_name):
        """
        Get type of a column

        Parameters
        ----------
        column_name: the column

        Returns
        -------
        type: the type of the data in the column
        """
        return self.all_columns[column_name]['type']

    def set_type(self, column_name, dtype):
        """
        Set the type of a column.

        Parameters
        ----------
        column_name: the column this operation applies to
        dtype: the data type to change the column to

        Returns
        -------
        None
        """
        self.all_columns[column_name]['type'] = dtype
        column_data = self.all_columns[column_name]

        # TODO Remove the hierarchical structure of Types.
        try:
            del self.table_meta['tables'][column_data['table_id']][
                'fields'][column_data['field_id']]['type']
            del self.table_meta['tables'][column_data['table_id']][
                'fields'][column_data['field_id']]['subtype']
        except BaseException:
            pass
        if dtype in TableMeta.SUPERTYPE:
            table_id = column_data['table_id']
            field_id = column_data['field_id']
            self.table_meta['tables'][table_id]['fields'][field_id]['type'] = TableMeta.SUPERTYPE[dtype]
            self.table_meta['tables'][table_id]['fields'][field_id]['subtype'] = dtype
        else:
            self.table_meta['tables'][column_data['table_id']][
                'fields'][column_data['field_id']]['type'] = dtype

    def get_property(self, column_name, property_name):
        """
        Get column property.

        Parameters
        ----------
        column_name: the column this operation applies to
        property_name: the property wanted

        Returns
        -------
        propery: the property
        """
        return self.all_columns[column_name]['properties'][property_name]

    def get_columns(self):
        """
        Get all column names.

        Parameters
        ----------
        None

        Returns
        -------
        names: all the column names
        """
        return self.all_columns.keys()

    def copy(self):
        """
        Make a deep copy of self

        Parameters
        ----------
        None

        Returns
        -------
        copy: a deep copy of self
        """
        return copy.deepcopy(self)

    def to_json(self):
        """
        Convert to json str

        Parameters
        ----------
        None

        Returns
        -------
        str: JSON str of self

        """
        return json.dumps(self.table_meta)

    @classmethod
    def from_json(cls, json_data):
        """
        Load from json str.

        Parameters
        ----------
        json_data: JSON str

        Returns
        -------
        table_meta: a TableMeta object

        """
        if json_data is None:
            return None

        return TableMeta(json.loads(json_data))

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False
