from trane.utils.table_meta import TableMeta

def test_table_meta_set_get():
    meta = TableMeta([{'name': 'col1', 'type': TableMeta.TYPE_IDENTIFIER}, 
                    {'name': 'col2', 'type': TableMeta.TYPE_VALUE}])
    assert meta.get_type('col1') == TableMeta.TYPE_IDENTIFIER
    assert meta.get_type('col2') == TableMeta.TYPE_VALUE
    meta.set_type('col2', TableMeta.TYPE_TEXT)
    assert meta.get_type('col1') == TableMeta.TYPE_IDENTIFIER
    assert meta.get_type('col2') == TableMeta.TYPE_TEXT
    
def test_table_meta_get_columns():
    meta = TableMeta([{'name': 'col1', 'type': TableMeta.TYPE_IDENTIFIER}, 
                    {'name': 'col2', 'type': TableMeta.TYPE_VALUE}])
    columns = meta.get_columns()
    assert len(columns) == 2
    assert 'col1' in columns and 'col2' in columns

def test_table_meta_copy():
    meta = TableMeta([{'name': 'col1', 'type': TableMeta.TYPE_IDENTIFIER}, 
                    {'name': 'col2', 'type': TableMeta.TYPE_VALUE}])
    meta2 = meta.copy()
    meta2.set_type('col2', TableMeta.TYPE_TIME)
    meta.set_type('col1', TableMeta.TYPE_BOOL)
    assert meta.get_type('col1') == TableMeta.TYPE_BOOL
    assert meta.get_type('col2') == TableMeta.TYPE_VALUE
    assert meta2.get_type('col1') == TableMeta.TYPE_IDENTIFIER
    assert meta2.get_type('col2') == TableMeta.TYPE_TIME


def test_table_meta_load_save():
    meta = TableMeta([{'name': 'col1', 'type': TableMeta.TYPE_IDENTIFIER}, 
                    {'name': 'col2', 'type': TableMeta.TYPE_VALUE}])
    meta_json = meta.to_json()
    assert type(meta_json) == str
    meta2 = TableMeta.from_json(meta_json)
    assert meta2.get_type('col1') == TableMeta.TYPE_IDENTIFIER
    assert meta2.get_type('col2') == TableMeta.TYPE_VALUE
    
