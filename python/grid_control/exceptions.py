import sys

# our exception base class
class GridError(Exception):
	def __init__(self, message):
		self.message = message

	def showMessage(self):
		print >> sys.stderr, "An error occured:", self.message


# some error with the Grid installation
class InstallationError(GridError):
	pass
