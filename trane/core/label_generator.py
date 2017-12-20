import json
from .prediction_problem import PredictionProblem

class LabelGenerator():

	def __init__(self, prediction_problems):
		self.prediction_problems = prediction_problems
	
	"""Generate the labels and cutoff times for each entity.
	Args:
		(Dict (Entity_Id -> Dataframe)): Entity Id to relevant data of that entity mapping
		(List): A list of Prediction Problem objects.
	Returns:
		(Dict (Prediction_Problem -> (Entity_Id -> Results))) : A mapping from prediction problem
			to an entity dictionary. The entity dictionary maps entity id's to a label and cutoff time.
	"""
	def execute(self, dataframe):
		results = []
		# a list of tuples (json prediction problem, 2 column entity label table)
		
		for prediction_problem in self.prediction_problems:
			results.append((prediction_problem, prediction_problem.execute(dataframe)))

		return results

	"""
	A method to set the cutoff time for all prediction problems.
	Args:
		(??): A global cutoff_time
	Returns:
		None
	"""
	def set_global_cutoff_time_for_all_prediction_problems(self, global_cutoff_time):
		for prediction_problem in self.prediction_problems:
			prediction_problem.set_global_cutoff_time(global_cutoff_time)

	def to_json(self):
		return json.dumps(
			{'problems': [json.loads(item.to_json()) for item in self.prediction_problems]
			})
	
	def from_json(json_data):
		data = json.loads(json_data)
		probs = [PredictionProblem.from_json(json.dumps(item)) for item in data['problems']]
		return LabelGenerator(probs)
