#-#  Copyright 2013-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, sys, logging
from python_compat import set

logLevelDict = {'DEFAULT': 14, # setLevel(logging.DEFAULT - <verbosity level>)
	'INFO1': 13, 'INFO2': 12, 'INFO3': 11, 'DEBUG1': 9, 'DEBUG2': 8, 'DEBUG3': 7}

def logging_defaults():
	def setupLogStream(logger, logFormat):
		handler = logging.StreamHandler(sys.stdout)
		handler.setFormatter(logging.Formatter(fmt=logFormat, datefmt='%Y-%m-%d %H:%M:%S'))
		logger.addHandler(handler)
		return logger

	setupLogStream(logging.getLogger(), '%(asctime)s - %(name)s:%(levelname)s - %(message)s')
	setupLogStream(logging.getLogger('user'), '%(message)s').propagate = False
	setupLogStream(logging.getLogger('user.time'), '%(asctime)s - %(message)s').propagate = False

	class UniqueFilter(logging.Filter):
		def __init__(self):
			self._memory = set()
		def filter(self, record):
			isNew = record.msg not in self._memory
			self._memory.add(record.msg)
			return isNew
	logging.getLogger('user.once').addFilter(UniqueFilter())
	logging.getLogger('user.time.once').addFilter(UniqueFilter())

	# Default exception logging to file in gc / tmp / user directory
	handler_ex = None
	for fnLog in [os.path.join(os.environ['GC_PACKAGES_PATH'], '..', 'debug.log'), '/tmp/gc.debug.%d' % os.getuid(), '~/gc.debug']:
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
	for name in logLevelDict:
		setattr(logging, name.upper(), logLevelDict[name]) # Add numerical constant
		logging.addLevelName(logLevelDict[name], name)     # Register with logging module
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
			# remove any standard handlers:
			for handler in list(logger.handlers):
				logger.removeHandler(handler)
			for dest in config.getList(option, [], onChange = None):
				if dest == 'stdout':
					logger.addHandler(logging.StreamHandler(sys.stdout))
				elif dest == 'stderr':
					logger.addHandler(logging.StreamHandler(sys.stderr))
				elif dest == 'file':
					option = option.replace('handler', 'file')
					logger.addHandler(logging.FileHandler(config.get(option, onChange = None), 'w'))
				else:
					raise Exception('Unknown handler [logging] %s = %s' % (option, dest))
		elif option.endswith('level'):
			logger.setLevel(logLevelDict.get(config.get(option, onChange = None).upper(), 0))
		elif option.endswith('propagate'):
			logger.propagate = config.getBool(option, onChange = None)

	# Formatting affects all handlers and needs to be done after the handlers are setup
	for option in config.getOptions():
		logger = getLoggerFromOption(option)
		if option.endswith('format'):
			for handler in logger.handlers:
				fmt = config.get(option, '$(message)s', onChange = None).replace('$', '%')
				handler.setFormatter(logging.Formatter(fmt))
