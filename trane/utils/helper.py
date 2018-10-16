from .table_meta import TableMeta as TM


def overall_prediction_helper(df, meta):
    df['__fake_root_entity__'] = 0
    meta.add_column("__fake_root_entity__", TM.TYPE_IDENTIFIER)
    return df, meta
