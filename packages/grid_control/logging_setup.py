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
#	from exception import ConfigError
	def getLogger(name):
		name = name.split(' ')[0]
		if name == 'root':
			return logging.getLogger()
		return logging.getLogger(name)

	for option in config.getOptions('logging'):
		if option.endswith('level'):
			getLogger(option).setLevel(config.get('logging', option).upper())
