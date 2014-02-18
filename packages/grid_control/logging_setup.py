import os, sys, logging

def logging_defaults():
	# Default logging to stdout
	sh = logging.StreamHandler(sys.stdout)
	logging.getLogger().addHandler(sh)
	# Maybe needed for debugging because logging_setup(config) is called after config.stored is used
	#logging.getLogger('config.stored').addHandler(sh)

	# Default exception logging to file in gc / tmp / user directory
	# Convention: sys.path[1] == python dir of gc
	handler_ex = None
	for fnLog in [os.path.join(sys.path[1], '..', 'debug.log'), '/tmp/gc.debug.%d' % os.getuid(), '~/gc.debug']:
		fnLog = os.path.abspath(os.path.normpath(os.path.expanduser(fnLog)))
		try:
			handler_ex = logging.FileHandler(fnLog, 'w')
			break
		except:
			pass
	if handler_ex:
		logging.getLogger("exception").propagate = False
		logging.getLogger("exception").addHandler(handler_ex)

	# Register new log levels
	levelDict = {'DEFAULT_VERBOSITY': 14, # setLevel(logging.DEFAULT_VERBOSITY - <verbosity level>)
		'INFO1': 13, 'INFO2': 12, 'INFO3': 11, 'DEBUG1': 9, 'DEBUG2': 8, 'DEBUG3': 7}
	for name in levelDict:
		setattr(logging, name.upper(), levelDict[name]) # Add numerical constant
		logging.addLevelName(levelDict[name], name)     # Register with logging module
logging_defaults()


def logging_setup(config):
	def getLoggerFromOption(option):
		if ' ' in option: # eg. 'exception handler = stdout' configures the exception handler
			return logging.getLogger(option.split()[0])
		return logging.getLogger() # eg. 'handler = stdout' configures the root handler

	# Configure general setup of loggers - destinations, level and propagation
	for option in config.getOptions():
		logger = getLoggerFromOption(option)
		if option.endswith('handler'): # Contains list of handlers to add to logger
			for dest in config.getList(option, [], onChange = None):
				if dest == 'stdout':
					logger.addHandler(logging.StreamHandler(sys.stdout))
				elif dest == 'stderr':
					logger.addHandler(logging.StreamHandler(sys.stderr))
				elif dest == 'file':
					option = option.replace('handler', 'file')
					logger.addHandler(logging.FileHandler(config.get(option, onChange = None), 'w'))
		elif option.endswith('level'):
			logger.setLevel(config.get(option, onChange = None).upper())
		elif option.endswith('propagate'):
			logger.propagate = config.getBool(option, onChange = None)

	# Formatting affects all handlers and needs to be done after the handlers are setup
	for option in config.getOptions():
		logger = getLoggerFromOption(option)
		if option.endswith('format'):
			for handler in logger.handlers:
				fmt = config.get(option, '$(message)s', onChange = None).replace('$', '%')
				handler.setFormatter(logging.Formatter(fmt))
