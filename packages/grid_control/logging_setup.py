import os, sys, logging

def logging_defaults():
	# Default logging to stdout
	sh = logging.StreamHandler(sys.stdout)
	#sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
	logging.getLogger().addHandler(sh)
	# Default exception logging to file in gc directory
	# Convention: sys.path[1] == python dir of gc
	handler_ex = logging.FileHandler(os.path.join(sys.path[1], '..', 'debug.log'), 'w')
	logging.getLogger("exception").propagate = False
	logging.getLogger("exception").addHandler(handler_ex)
	# Register new log levels
	levelDict = {'DEFAULT_VERBOSITY': 14, # setLevel(logging.DEFAULT_VERBOSITY - opts.verbosity)
		'INFO1': 13, 'INFO2': 12, 'INFO3': 11, 'DEBUG1': 9, 'DEBUG2': 8, 'DEBUG3': 7}
	for name in levelDict:
		setattr(logging, name.upper(), levelDict[name]) # Add numerical constant
		logging.addLevelName(levelDict[name], name)     # Register with logging module
logging_defaults()


def logging_setup(config):
	for option in config.getOptions('logging'):
		logger = logging.getLogger() # eg. 'handler = stdout' configures the root handler
		if ' ' in option: # eg. 'exception handler = stdout' configures the exception handler
			logger = logging.getLogger(option.split()[0])

		if option.endswith('handler'):
			for dest in config.getList('logging', option, []):
				if dest == 'stdout':
					logger.addHandler(logging.StreamHandler(sys.stdout))
				elif dest == 'stderr':
					logger.addHandler(logging.StreamHandler(sys.stderr))
				elif dest == 'file':
					option = option.replace('handler', 'file')
					logger.addHandler(logging.FileHandler(config.get('logging', option), 'w'))
		elif option.endswith('level'):
			logger.setLevel(config.get('logging', option).upper())
		elif option.endswith('propagate'):
			logger.propagate = config.getBool('logging', option)
