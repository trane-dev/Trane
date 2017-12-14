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
		self.set_params(hyper_parameters)
		self.check_rep()

	def set_params(self, hyper_parameters):
		if type(hyper_parameters) is list:
			self.hyper_parameters = hyper_parameters
		else:
			self.hyper_parameters = [hyper_parameters]
		self.check_rep()

	def execute(self, value):
		args = [value] + self.hyper_parameters
		output = self.function(*args)
		self.check_rep()
		return output

	def __str__(self):
		return "SubOperation with name {} and parameters {}".format(self.name, self.hyper_parameters)
	def check_rep(self):
		assert(type(self.hyper_parameters) is list)
		assert(callable(self.function))
		assert(type(self.name) is str)
