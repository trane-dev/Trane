from .prediction_problem import PredictionProblem
from ..utils.table_meta import TableMeta
import json

def to_json(prediction_problems, table_meta, entity_id_column, label_generating_column, time_column):
    """
    Convert a list of prediction problems into a json str.
    args:
        prediction_problems: a list of PredictionProblem
        table_meta: TableMeta
        entity_id_column: str
        label_generating_column: str
        time_column: str
    returns:
        str: a json format str
    """
    prediction_problems_json = [prob.to_json() for prob in prediction_problems]
    return json.dumps({
        "prediction_problems": [json.loads(prob_json) for prob_json in prediction_problems_json],
        "table_meta": json.loads(table_meta.to_json()),
        "entity_id_column": entity_id_column,
        "label_generating_column": label_generating_column,
        "time_column": time_column
    })
    
def from_json(json_data):
    """
    Convert json into a list of prediction problems and extra information.
    args:
        json_data: a json format str
    returns:
        list of PredictionProblem
        TableMeta: tablemeta
        str: entity_id_column
        str: label_generating_column
        str: time_column
    """
    data = json.loads(json_data)
    prediction_problems = data['prediction_problems']
    prediction_problems = [PredictionProblem.from_json(json.dumps(prob)) for prob in prediction_problems]
    table_meta = TableMeta.from_json(json.dumps(data['tabel_meta']))
    entity_id_column = data['entity_id_column']
    label_generating_column = data['label_generating_column']
    time_column = data['time_column']
    return prediction_problems, table_meta, entity_id_column, label_generating_column, time_column
