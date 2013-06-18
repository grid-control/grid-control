import os, sys, logging

def logging_defaults():
	# Default logging to stdout
	logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
	# Default exception logging to file in gc directory
	# Convention: sys.path[1] == python dir of gc
	handler_ex = logging.FileHandler(os.path.join(sys.path[1], '..', 'exception.last'), 'w')
	logging.getLogger("exception").propagate = False
	logging.getLogger("exception").addHandler(handler_ex)

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
