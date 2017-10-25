
#------------------------------------STEP 1---------------------------------------
#All operations code architecture

#PARENT CLASS that all operations will be derived from
class Operation:

#Child classes of the parent class, Operation
class RowOperation(Operation):
	#instance variables:
	#column names
	#X relevant data (dataframe)

	#ALL POSSIBLE ROW OPERATIONS---
	row_operations = ...

	def __init__(self, column_names):

class ColumnOperation(Operation):
	#instance variables:
	#column names
	#X relevant data (dataframe)
	def __init__(self, all_column_names, columns_to_operate_over):
		

class FilterOperation(Operation):
	#instance variables:
	#column names
	#X relevant data (dataframe)
	def __init__(self, column_names):


class TransformationOperation(Operation):
	#instance variables:
	#X relevant data (dataframe)
	#column names
	def __init__(self, column_names):


class AggregationOperation(Operation):
	#instance variables:
	#X relevant data (dataframe)
	#column names
	def __init__(self, column_names):

#------------------------------------STEP 2---------------------------------------
#Generate possible prediction problems architecture
#STANDALONE BLACKBOX DEFINITION OF THE PREDICTION PROBLEM
class PredictionProblem:
	def __init__(Operation one, Operation two, Operation three ...):
		#instance variables:
		#ordered list of objects. Each object represents an operation to be applied. The objects
		#	contain information telling them which columns they are going to use.
	def describe():
		#NLP of what the prediction problem is
	def calculate(dataframe):
		#pass in dataframe and move it through the layers of the predction problem

#GENERATE PREDICTION PROBLEM OJBJECTS
class PredictionProblemGenerator:
	#input dataset (dataframe)
	#output a list of objects with type PredictionProblem. This list of PredictionProblem's represents 
	#	the cartesian product of prediction problems capable of being assembled. (list)

#------------------------------------STEP 3---------------------------------------
#Identifying cutoff times for each entity architecture

#Instance variable of the PredictionProblem instance. Should be a dict mapping entity instances (e.g. userID 456) to 
#the cutoff time associated with that instance

#Code for determining the cutoff times should reside in a method within the PredictionProblem class.
def determine_cutoff_time(entity_id, ...)

#------------------------------------STEP 4---------------------------------------
#Runtime architecture - aka generating prediction problems, generating labels etc.

#Takes in specific prediction problem, data, cutoff times, applies cutoff time for each instance and
#applies the prediction function to generate label

class PredictionProblemSolver():
	#input: dataset. should then call functions to generate prediction problems, and cutoff times for each instance.
		#Then, using the derived information, train the label. ?? Not totally sure what this step needs to accomplish. Perhaps 
		#this is where we train the model and evaluate accuracy?







