import sys, logging
from python_compat import sorted

# Exception handler which outputs a stack trace to the logging facility
def logException_internal(exClass, exValue, stack):
	import linecache, time
	log = logging.getLogger('exception')
	counter = 0
	log.critical('Exception occured: %s' % time.strftime("%Y-%m-%d %H:%M:%S"))
	try:
		import utils
		log.critical('grid-control: %s' % utils.getVersion())
	except:
		log.critical('grid-control: Unknown version')
	log.critical('')
	while stack:
		counter += 1
		code = stack.tb_frame.f_code
		# Output relevant code fragment
		linecache.checkcache(code.co_filename)
		log.critical('Stack #%s [%s:%d] %s', counter, code.co_filename, stack.tb_lineno, code.co_name)
		fmtLine = lambda line: linecache.getline(code.co_filename, line).rstrip().replace('\t', '  ')
		log.critical('\t  | %s', fmtLine(stack.tb_lineno - 1))
		log.critical('\t=>| %s', fmtLine(stack.tb_lineno + 0))
		log.critical('\t  | %s', fmtLine(stack.tb_lineno + 1))
		log.critical('')
		# Output local and class variables
		log.critical('\tLocal variables:')
		tmp = dict(stack.tb_frame.f_locals)
		maxlen = max(map(len, tmp.keys()) + [0])
		for var in sorted(filter(lambda v: v != 'self', tmp)):
			log.critical('\t\t%s = %r', var.ljust(maxlen), tmp[var])
		if 'self' in tmp:
			log.critical('\tClass variables:')
			try:
				for var in sorted(tmp['self'].__dict__):
					log.critical('\t\tself.%s = %r', var.ljust(maxlen), tmp['self'].__dict__[var])
			except:
				pass
		log.critical('')
		stack = stack.tb_next
	exMessage = '%s: %s' % (exClass.__name__, str.join(' ', map(str, exValue.args)))
	log.critical(exMessage)
	del stack
	return exMessage + '\n'

def logException():
	return logException_internal(*sys.exc_info())

# Function to warp a main function in correct exception handling
def handleException(fun, *args, **kwargs):
	try:
		fun(*args, **kwargs)
	except SystemExit: # Forward SystemExit exit code
		sys.exit(sys.exc_info()[1].code)
	except:
		sys.stderr.write(logException())
		for handler in logging.getLogger('exception').handlers:
			if isinstance(handler, logging.FileHandler):
				sys.stderr.write('In case this is caused by a bug, please send the log file:\n')
				sys.stderr.write('\t"%s"\nto grid-control-dev@googlegroups.com\n' % handler.baseFilename)
				break
		sys.exit(1)

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
