import json

from trane.core.prediction_problem import PredictionProblem
from trane.utils.table_meta import TableMeta

__all__ = ["prediction_problems_to_json_file", "prediction_problems_from_json_file"]


def prediction_problems_to_json_file(
    prediction_problems,
    table_meta,
    entity_id_column,
    label_generating_column,
    time_column,
    filename,
):
    """
    Convert a list of prediction problems to a JSON file

    Parameters
    ----------
    prediction_problems: a list of Prediction Problems.
    table_meta: TableMeta object. Contains
        meta information about the data
    entity_id_column: column name of
        the column containing entities in the data
    label_generating_column: column name of the
        column of interest in the data
    time_column: column name of the column
        containing time information in the data
    filename: name of the file to write to. must end in .json

    Returns
    ----------
    None
    """

    prediction_problems_json = [prob.to_json() for prob in prediction_problems]
    json_str = json.dumps(
        {
            "prediction_problems": [
                json.loads(prob_json) for prob_json in prediction_problems_json
            ],
            "table_meta": json.loads(table_meta.to_json()),
            "entity_id_column": entity_id_column,
            "label_generating_column": label_generating_column,
            "time_column": time_column,
        },
    )

    with open(filename, "w") as f:
        json.dump(json.loads(json_str), f, indent=4, separators=(",", ": "))


def prediction_problems_from_json_file(filename):
    """
    Read Prediction Problems from a JSON structured file

    Parameters
    ----------
    filename: filename to read problems from.

    Returns
    ----------
    prediction_problems: a list of Prediction Problems.
    table_meta: TableMeta object. Contains
        meta information about the data
    entity_id_column: column name of
        the column containing entities in the data
    label_generating_column: column name of the
        column of interest in the data
    time_column: column name of the column
        containing time information in the data
    """
    with open(filename) as f:
        json_data = f.read()

    data = json.loads(json_data)
    prediction_problems = data["prediction_problems"]
    prediction_problems = [
        PredictionProblem.from_json(json.dumps(prob)) for prob in prediction_problems
    ]
    table_meta = TableMeta.from_json(json.dumps(data["table_meta"]))
    entity_id_column = data["entity_id_column"]
    label_generating_column = data["label_generating_column"]
    time_column = data["time_column"]
    return (
        prediction_problems,
        table_meta,
        entity_id_column,
        label_generating_column,
        time_column,
    )
