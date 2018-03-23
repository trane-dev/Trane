from .prediction_problem import PredictionProblem
from ..utils.table_meta import TableMeta
import json

__all__ = ["prediction_problems_to_json_file",
           "prediction_problems_from_json_file"]


def prediction_problems_to_json_file(prediction_problems, table_meta,
                                     entity_id_column, label_generating_column, time_column, filename):
    """Convert a list of prediction problems to a JSON representation and store it in a file named filename.

    args:
        prediction_problems: a list of PredictionProblem
        table_meta: TableMeta
        entity_id_column: str
        label_generating_column: str
        time_column: str
        filename: str, ending in .json

    returns:
        None

    """
    prediction_problems_json = [prob.to_json() for prob in prediction_problems]

    json_str = json.dumps({
        "prediction_problems": [json.loads(prob_json) for prob_json in prediction_problems_json],
        "table_meta": json.loads(table_meta.to_json()),
        "entity_id_column": entity_id_column,
        "label_generating_column": label_generating_column,
        "time_column": time_column
    })

    with open(filename, "w") as f:
        json.dump(json.loads(json_str), f, indent=4, separators=(',', ': '))


def prediction_problems_from_json_file(filename):
    """Read json data from a file and convert it to a list of prediction problems and extra information.

    args:
        filename: a string, ending in .json

    returns:
        list of PredictionProblem
        TableMeta: tablemeta
        str: entity_id_column
        str: label_generating_column
        str: time_column

    """
    with open(filename) as f:
        json_data = f.read()

    data = json.loads(json_data)
    prediction_problems = data['prediction_problems']
    prediction_problems = [PredictionProblem.from_json(
        json.dumps(prob)) for prob in prediction_problems]
    table_meta = TableMeta.from_json(json.dumps(data['table_meta']))
    entity_id_column = data['entity_id_column']
    label_generating_column = data['label_generating_column']
    time_column = data['time_column']
    return prediction_problems, table_meta, entity_id_column, label_generating_column, time_column
