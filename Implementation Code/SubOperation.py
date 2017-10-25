class SubOperation:
	"""
	Args:
	    (String) Name: The name of the operation in English.
	    (Function) function: The function to be applied.
	    (List) hyper_parameters: parameters of the function.
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, name, function, hyper_parameters = []):
		self.name = name
		self.function = function
		self.hyper_parameters = hyper_parameters
	
	def execute(self, value):
		args = [value] + self.hyper_parameters
		output = self.function(*args)
		return output
