import json
from .prediction_problem import PredictionProblem

__all__ = ['LabelGenerator']


class LabelGenerator():
    """
    Generate labels of prediction problems using a given dateset.
    """

    def __init__(self, prediction_problems):
        """
        args:
            list of PredictionProblem
        """
        self.prediction_problems = prediction_problems

    def execute(self, dataframe):
        """Generate the labels and cutoff times for each entity.
        Args:
            dataframe: Pandas dataframe
        Returns:
            list of tuples: [(prediction_problem, label), ...]
        """
        results = []
        # a list of tuples (json prediction problem, 2 column entity label table)

        for prediction_problem in self.prediction_problems:
            results.append(
                (prediction_problem, prediction_problem.execute(dataframe)))

        return results

    def set_global_cutoff_time_for_all_prediction_problems(self, global_cutoff_time):
        """
        A method to set the cutoff time for all prediction problems.
        Args:
            global_cutoff_time: A global cutoff_time
        Returns:
            None
        """
        for prediction_problem in self.prediction_problems:
            prediction_problem.set_global_cutoff_time(global_cutoff_time)

    def to_json(self):
        """
        Convert to json str
        returns:
            str: json str of self
        """
        return json.dumps({
            'problems': [json.loads(item.to_json()) for item in self.prediction_problems]
        })

    def from_json(json_data):
        """
        Load from json str.
        args:
            json_data: json str
        returns:
            LabelGenerator
        """
        data = json.loads(json_data)
        probs = [PredictionProblem.from_json(
            json.dumps(item)) for item in data['problems']]
        return LabelGenerator(probs)
