import sys

# our exception base class
class GridError(Exception):
	def __init__(self, msg):
		self.msg = msg

	def showMessage(self):
		sys.stderr.write("%s: %s\n" % (sys.argv[0], self.msg))

# some error with the Grid installation
class InstallationError(GridError):
	pass	# just inherit everything from GridError

# some error with the user config file
class ConfigError(GridError):
	pass	# just inherit everything from GridError

# some error with the user (PEBKAC)
class UserError(GridError):
	pass	# just inherit everything from GridError

# some error with the runtime
class RuntimeError(GridError):
	pass	# just inherit everything from GridError

# some error with the runtime
class AbstractError(GridError):
	def __init__(self):
		GridError.__init__(self, "%s is an abstract function!" % sys._getframe(1).f_code.co_name)

# some error with the dataset
class DatasetError(GridError):
	pass	# just inherit everything from GridError
