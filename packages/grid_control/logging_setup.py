# | Copyright 2013-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys, time, logging
from grid_control.gc_exceptions import GCError, GCLogHandler
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.file_objects import VirtualFile
from hpfwk import ExceptionFormatter
from python_compat import irange, lmap, set, sorted, tarfile

class LogOnce(logging.Filter):
	def __init__(self):
		logging.Filter.__init__(self)
		self._memory = set()

	def filter(self, record):
		isNew = record.msg not in self._memory
		self._memory.add(record.msg)
		return isNew


class LogEveryNsec(logging.Filter):
	def __init__(self, delta):
		logging.Filter.__init__(self)
		self._memory = {}
		self._delta = delta

	def filter(self, record):
		accept = self._memory.get(record.msg, 0) - time.time() > self._delta
		if accept:
			self._memory[record.msg] = time.time()
		return accept


# In contrast to StreamHandler, this logging handler doesn't keep a stream copy
class StdoutStreamHandler(logging.Handler):
	def get_stream(self):
		return sys.stdout

	def emit(self, record):
		try:
			stream = self.get_stream()
			stream.write(self.format(record) + '\n')
			stream.flush()
		except (KeyboardInterrupt, SystemExit):
			raise
		except Exception:
			self.handleError(record)


class StderrStreamHandler(StdoutStreamHandler):
	def get_stream(self):
		return sys.stderr


class ProcessArchiveHandler(logging.Handler):
	def __init__(self, fn, log = None):
		logging.Handler.__init__(self)
		self._fn = fn
		self._log = log or logging.getLogger('archive')

	def emit(self, record):
		if record.pathname == '<process>':
			entry = '%s_%s.%03d' % (record.name, time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(record.created)), int(record.msecs))
			files = record.files
			files['info'] = 'call=%s\nexit=%s\n' % (repr(record.proc.get_call()), record.proc.status(0))
			files['stdout'] = record.proc.stdout.read_log()
			files['stderr'] = record.proc.stderr.read_log()
			files['stdin'] = record.proc.stdin.read_log()
			try:
				tar = tarfile.TarFile.open(self._fn, 'a')
				for key, value in record.additional.items():
					if os.path.exists(value):
						value = open(value, 'r').read()
					fileObj = VirtualFile(os.path.join(entry, key), [value])
					info, handle = fileObj.getTarInfo()
					tar.addfile(info, handle)
					handle.close()
				tar.close()
			except Exception:
				raise GCError('Unable to log results of external call "%s" to "%s"' % (record.proc.get_call(), self._fn))
			self._log.warning('All logfiles were moved to %s', self._fn)


def setupLogStream(logger_name, log_handler = None, log_filter = None, log_format = None, log_propagate = True):
	logger = logging.getLogger(logger_name.lower())
	logger.propagate = log_propagate
	if log_filter:
		logger.addFilter(log_filter)
	if log_handler:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)
		logger.addHandler(log_handler)
	if log_format:
		for handler in logger.handlers:
			handler.setFormatter(logging.Formatter(fmt = log_format, datefmt = '%Y-%m-%d %H:%M:%S'))


def logging_defaults():
	setupLogStream('', log_handler = StdoutStreamHandler(),
		log_format = '%(asctime)s - %(name)s:%(levelname)s - %(message)s')
	setupLogStream('user', log_handler = StdoutStreamHandler(),
		log_format = '%(message)s', log_propagate = False)
	setupLogStream('user.time', log_handler = StdoutStreamHandler(),
		log_format = '%(asctime)s - %(message)s', log_propagate = False)
	setupLogStream('user.once', log_filter = LogOnce())
	setupLogStream('user.time.once', log_filter = LogOnce())

	# External libraries
	logging.getLogger('requests').setLevel(logging.WARNING)

	# Default exception logging to stderr and file in gc / tmp / user directory
	excFormatterQuiet = ExceptionFormatter(showCodeContext = 0, showVariables = 0, showFileStack = 0)
	excFormatterVerbose = ExceptionFormatter(showCodeContext = 2, showVariables = 1, showFileStack = 1)

	logException = logging.getLogger("exception")
	handlerException_stdout = StderrStreamHandler()
	handlerException_stdout.setFormatter(excFormatterVerbose)
	logException.addHandler(handlerException_stdout)

	try:
		handlerException_file = GCLogHandler(None, 'w')
		handlerException_file.setFormatter(excFormatterVerbose)
		logException.addHandler(handlerException_file)
		handlerException_stdout.setFormatter(excFormatterQuiet)
	except Exception:
		pass
	logException.propagate = False

	# Adding log_process_result to Logging class
	def log_process(self, proc, level = logging.WARNING, files = None):
		status = proc.status(timeout = 0)
		record = self.makeRecord(self.name, level, '<process>', 0,
			'Process %s finished with exit code %s' % (proc.get_call(), status), None, None)
		record.proc = proc
		record.call = proc.get_call()
		record.proc_status = status
		record.files = files or {}
		self.handle(record)
	logging.Logger.log_process = log_process


# Display logging setup
def dump_log_setup(level):
	root = logging.getLogger()

	def display_logger(indent, logger, name):
		propagate_symbol = '+'
		if hasattr(logger, 'propagate') and not logger.propagate:
			propagate_symbol = 'o'
		desc = name
		if hasattr(logger, 'level'):
			desc += ' (level = %s)' % logging.getLevelName(logger.level)
		root.log(level, '%s%s %s', '|  ' * indent, propagate_symbol, desc)
		if hasattr(logger, 'filters'):
			for lf in logger.filters:
				root.log(level, '%s# %s', '|  ' * (indent + 1), lf.__class__.__name__)
		if hasattr(logger, 'handlers'):
			for handler in logger.handlers:
				root.log(level, '%s> %s', '|  ' * (indent + 1), handler.__class__.__name__)
				fmt = handler.formatter
				if fmt:
					desc = fmt.__class__.__name__
					if isinstance(fmt, ExceptionFormatter):
						desc = repr(fmt)
					elif isinstance(fmt, logging.Formatter):
						desc += '(%s, %s)' % (repr(getattr(fmt, '_fmt')), repr(fmt.datefmt))
					root.log(level, '%s|  %% %s', '|  ' * (indent + 1), desc)

	display_logger(0, root, '<root>')
	for key, logger in sorted(root.manager.loggerDict.items()):
		display_logger(key.count('.') + 1, logger, key)


# Configure formatting of handlers
def logging_configure_handler(config, logger_name, handler_str, handler):
	def get_handler_option(postfix):
		return ['%s %s' % (logger_name, postfix), '%s %s %s' % (logger_name, handler_str, postfix)]
	if logger_name.startswith('exception'):
		ex_code = config.getInt(get_handler_option('code context'), 2, onChange = None)
		ex_var = config.getInt(get_handler_option('variables'), 1, onChange = None)
		ex_file = config.getInt(get_handler_option('file stack'), 1, onChange = None)
		fmt = ExceptionFormatter(showCodeContext = ex_code, showVariables = ex_var, showFileStack = ex_file)
	else:
		fmt_str = config.get(get_handler_option('format'), '$(message)s', onChange = None)
		fmt = logging.Formatter(fmt_str.replace('$', '%'))
	handler.setFormatter(fmt)
	return handler


# Configure general setup of loggers - destinations, level and propagation
def logging_create_handlers(config, logger_name):
	LogLevelEnum = makeEnum(lmap(lambda level: logging.getLevelName(level).upper(), irange(51)))

	logger = logging.getLogger(logger_name.lower())
	# Set logging level
	logger.setLevel(config.getEnum(logger_name + ' level', LogLevelEnum, logger.level, onChange = None))
	# Set propagate status
	logger.propagate = config.getBool(logger_name + ' propagate', bool(logger.propagate), onChange = None)
	# Setup handlers
	if logger_name + ' handler' in config.getOptions():
		# remove any standard handlers:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)
		handler_list = config.getList(logger_name + ' handler', [], onChange = None)
		for handler_str in set(handler_list): # add only unique output handlers
			if handler_str == 'stdout':
				handler = StdoutStreamHandler()
			elif handler_str == 'stderr':
				handler = StderrStreamHandler()
			elif handler_str == 'file':
				handler = logging.FileHandler(config.get(logger_name + ' file', onChange = None), 'w')
			elif handler_str == 'debug_file':
				handler = GCLogHandler(config.get(logger_name + ' debug file', '', onChange = None), 'w')
			else:
				raise Exception('Unknown handler %s for logger %s' % (handler_str, logger_name))
			logger.addHandler(logging_configure_handler(config, logger_name, handler_str, handler))


# Apply configuration to logging setup
def logging_setup(config):
	if config.getBool('debug mode', False, onChange = None):
		config.set('level', 'DEBUG3', '?=')
		config.set('exception handler', 'stdout debug_file', '?=')
		config.setInt('exception code context', 2)
		config.setInt('exception variables', 2)
		config.setInt('exception file stack', 2)

	# Find logger names in options
	logger_names = set()
	for option in config.getOptions():
		if option in ['debug mode', 'display logger']:
			pass
		elif option.count(' ') == 0:
			logger_names.add('')
		else:
			logger_names.add(option.split(' ')[0].strip())
	for logger_name in logger_names:
		logging_create_handlers(config, logger_name)

	if config.getBool('display logger', False, onChange = None):
		dump_log_setup(logging.WARNING)
