import sys
from python_compat import *

# Exception handler which outputs a stack trace to the logging facility
def logException_internal(exClass, exValue, stack):
	import logging, linecache, time
	log = logging.getLogger('exception')
	counter = 0
	log.critical('Exception occured: %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
	log.critical('')
	while stack:
		counter += 1
		code = stack.tb_frame.f_code
		# Output relevant code fragment
		linecache.checkcache(code.co_filename)
		log.critical('Stack #%s [%s:%d] %s', counter, code.co_filename, stack.tb_lineno, code.co_name)
		fmtLine = lambda line: line.rstrip().replace('\t', '  ')
		log.critical('\t  | %s', fmtLine(linecache.getline(code.co_filename, stack.tb_lineno - 1)))
		log.critical('\t=>| %s', fmtLine(linecache.getline(code.co_filename, stack.tb_lineno + 0)))
		log.critical('\t  | %s', fmtLine(linecache.getline(code.co_filename, stack.tb_lineno + 1)))
		log.critical('')
		# Output local and class variables
		log.critical('\tLocal variables:')
		tmp = dict(stack.tb_frame.f_locals)
		maxlen = max(map(len, tmp.keys()) + [0])
		for var in sorted(filter(lambda v: v != 'self', tmp)):
			log.critical('\t\t%s = %r', var.ljust(maxlen), tmp[var])
		if 'self' in tmp:
			log.critical('\tClass variables:')
			for var in sorted(tmp['self'].__dict__):
				log.critical('\t\tself.%s = %r', var.ljust(maxlen), tmp['self'].__dict__[var])
		log.critical('')
		stack = stack.tb_next
	exMessage = '%s: %s' % (exClass.__name__, str.join(' ', exValue.args))
	log.critical(exMessage)
	del stack
	return exMessage + '\n'

def logException():
	return logException_internal(*sys.exc_info())

class GCError(Exception):
	pass	# grid-control exception base class

class ConfigError(GCError):
	pass	# some error with the configuration

class InstallationError(GCError):
	pass	# some error with installed programs

class RuntimeError(GCError):
	pass	# some error with the runtime

class UserError(GCError):
	pass	# some error caused by the user

class APIError(GCError):
	pass	# some error in using the API

class GridError(GCError):
	pass	# some error with the Backend

class DatasetError(GCError):
	pass	# some error with the dataset

# some error related to abstract functions
class AbstractError(APIError):
	def __init__(self):
		APIError.__init__(self, "%s is an abstract function!" % sys._getframe(1).f_code.co_name)

# Rethrow error message to add additional information
class RethrowError(GCError):
	def __init__(self, msg, exClass = GCError):
		prevInfo = logException() # 
		if isinstance(sys.exc_info()[1], KeyboardInterrupt):
			GCError.__init__(self, 'Aborted by user')
		else:
			raise exClass(msg + '\n' + prevInfo)
