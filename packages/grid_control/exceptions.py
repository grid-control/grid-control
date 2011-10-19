import sys

# our exception base class
class GCError(Exception):
	def __init__(self, msg):
		GCError.message = "%s: %s\n" % (sys.argv[0], msg)
		Exception.__init__(self, GCError.message)

class GridError(GCError):
	pass

class ConfigError(GCError):
	pass

class InstallationError(GCError):
	pass	# some error with installed programs

class UserError(GCError):
	pass	# some error with the user (PEBKAC)

class RuntimeError(GCError):
	pass	# some error with the runtime

class APIError(GCError):
	pass	# some error in using the API

class DatasetError(GCError):
	pass	# some error with the dataset

class AbstractError(APIError):
	def __init__(self):
		APIError.__init__(self, "%s is an abstract function!" % sys._getframe(1).f_code.co_name)

class RethrowError(GCError):	# rethrow error message
	def __init__(self, msg):
		if isinstance(sys.exc_info()[1], KeyboardInterrupt):
			GCError.__init__(self, 'Aborted by user')
		else:
			GCError.__init__(self, msg)
			import traceback
			traceback.print_exception(*sys.exc_info())
