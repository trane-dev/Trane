class Pet:

    def __init__(self, name, species):
        self.name = name
        self.species = species

    def getName(self):
        return self.name

    def getSpecies(self):
        return self.species

    def __str__(self):
        return "%s is a %s" % (self.name, self.species)

class Dog(Pet):

	def __init__(self, name, is_good_doggo):
		self.good_dog = is_good_doggo
		Pet.__init__(self, name, "Dog")

	def is_good_doggo(self):
		return self.good_dog


hector = Dog("Hector", True)


print hector.getName()
print hector.getSpecies()
print hector.is_good_doggo()