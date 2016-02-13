#-#  Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, sys, time, logging
from grid_control.gc_exceptions import GCLogHandler
from hpfwk import ExceptionFormatter
from python_compat import irange, set

class LogOnce(logging.Filter):
	def __init__(self):
		self._memory = set()

	def filter(self, record):
		isNew = record.msg not in self._memory
		self._memory.add(record.msg)
		return isNew


class LogEveryNsec(logging.Filter):
	def __init__(self, delta):
		self._memory = {}
		self._delta = delta

	def filter(self, record):
		accept = self._memory.get(record.msg, 0) - time.time() > self._delta
		if accept:
			self._memory[record.msg] = time.time()
		return accept


def getFilteredLogger(name, logFilter = None):
	logger = logging.getLogger(name)
	if name not in getFilteredLogger._memory: # not setup yet
		if logFilter:
			logger.addFilter(logFilter)
		getFilteredLogger._memory.append(name)
	return logger
getFilteredLogger._memory = []


def logging_defaults():
	def setupLogStream(logger, logFormat):
		handler = logging.StreamHandler(sys.stdout)
		handler.setFormatter(logging.Formatter(fmt=logFormat, datefmt='%Y-%m-%d %H:%M:%S'))
		logger.addHandler(handler)
		return logger

	setupLogStream(logging.getLogger(), '%(asctime)s - %(name)s:%(levelname)s - %(message)s')
	setupLogStream(logging.getLogger('user'), '%(message)s').propagate = False
	setupLogStream(logging.getLogger('user.time'), '%(asctime)s - %(message)s').propagate = False
	getFilteredLogger('user.once', LogOnce())
	getFilteredLogger('user.time.once', LogOnce())

	# Default exception logging to stderr and file in gc / tmp / user directory
	excFormatterQuiet = ExceptionFormatter(showCodeContext = 0, showVariables = 0, showFileStack = 0)
	excFormatterVerbose = ExceptionFormatter(showCodeContext = 2, showVariables = 1, showFileStack = 1)

	logException = logging.getLogger("exception")
	handlerException_stdout = logging.StreamHandler(sys.stderr)
	handlerException_stdout.setFormatter(excFormatterVerbose)
	logException.addHandler(handlerException_stdout)

	handlerException_file = None
	for fnLog in [os.path.join(os.environ['GC_PACKAGES_PATH'], '..', 'debug.log'), '/tmp/gc.debug.%d' % os.getuid(), '~/gc.debug']:
		fnLog = os.path.abspath(os.path.normpath(os.path.expanduser(fnLog)))
		try:
			handlerException_file = GCLogHandler(fnLog, 'w')
			handlerException_file.setFormatter(excFormatterVerbose)
			logException.addHandler(handlerException_file)
			handlerException_stdout.setFormatter(excFormatterQuiet)
			break
		except Exception:
			pass
	logException.propagate = False


def logging_setup(config):
	logLevelDict = {}
	for level in irange(51):
		logLevelDict[logging.getLevelName(level).upper()] = level

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
					handler = logging.StreamHandler(sys.stdout)
				elif dest == 'stderr':
					handler = logging.StreamHandler(sys.stderr)
				elif dest == 'file':
					option = option.replace('handler', 'file')
					handler = logging.FileHandler(config.get(option, onChange = None), 'w')
				else:
					raise Exception('Unknown handler [logging] %s = %s' % (option, dest))
				if option.startswith('exception'):
					handler.setFormatter(ExceptionFormatter(showCodeContext = 2, showVariables = 1, showFileStack = 1))
				logger.addHandler(handler)
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
