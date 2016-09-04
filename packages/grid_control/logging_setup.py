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

import os, sys, time, logging, threading
from grid_control.gc_exceptions import GCError, GCLogHandler
from grid_control.utils.data_structures import UniqueList, makeEnum
from grid_control.utils.file_objects import SafeFile, VirtualFile
from grid_control.utils.thread_tools import GCLock
from hpfwk import AbstractError, clear_current_exception, format_exception
from python_compat import irange, lmap, set, sorted, tarfile

LogLevelEnum = makeEnum(lmap(lambda level: logging.getLevelName(level).upper(), irange(51)), useHash = False)

class LogEveryNsec(logging.Filter):
	def __init__(self, interval):
		logging.Filter.__init__(self)
		self._memory = {}
		self._interval = interval

	def filter(self, record):
		accept = (time.time() - self._memory.get(record.msg, 0) > self._interval)
		if accept:
			self._memory[record.msg] = time.time()
		return accept


# In contrast to StreamHandler, this logging handler doesn't keep a stream copy
class GCStreamHandler(logging.Handler):
	def __init__(self):
		logging.Handler.__init__(self)
		self._lock = GCLock(threading.RLock())

	def get_stream(self):
		raise AbstractError

	def emit(self, record):
		self._lock.acquire()
		try:
			stream = self.get_stream()
			stream.write(self.format(record) + '\n')
			stream.flush()
		finally:
			self._lock.release()


class StdoutStreamHandler(GCStreamHandler):
	def get_stream(self):
		return sys.stdout


class StderrStreamHandler(GCStreamHandler):
	def get_stream(self):
		return sys.stderr


class ProcessArchiveHandler(logging.Handler):
	def __init__(self, fn, log = None):
		logging.Handler.__init__(self)
		self._fn = fn
		self._log = log or logging.getLogger('logging.process')
		# safeguard against multiple tar file
		self._lock = ProcessArchiveHandler.tar_locks.setdefault(os.path.abspath(fn), threading.RLock())
		# overwrite python2.3 non-reentrant lock
		self.lock = threading.RLock()

	def _write_process_log(self, record):
		entry = '%s_%s.%03d' % (record.name, time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(record.created)), int(record.msecs))
		files = record.files
		files['info'] = 'call=%s\nexit=%s\n' % (repr(record.proc.get_call()), record.proc.status(0))
		files['stdout'] = record.proc.stdout.read_log()
		files['stderr'] = record.proc.stderr.read_log()
		files['stdin'] = record.proc.stdin.read_log()
		try:
			tar = tarfile.TarFile.open(self._fn, 'a')
			for key, value in record.files.items():
				if os.path.exists(value):
					value = SafeFile(value).read()
				fileObj = VirtualFile(os.path.join(entry, key), [value])
				info, handle = fileObj.getTarInfo()
				tar.addfile(info, handle)
				handle.close()
			tar.close()
		except Exception:
			raise GCError('Unable to log results of external call "%s" to "%s"' % (record.proc.get_call(), self._fn))

	def emit(self, record):
		if record.pathname == '<process>':
			self._lock.acquire()
			try:
				self._write_process_log(record)
			finally:
				self._lock.release()
			self._log.warning('All logfiles were moved to %s', self._fn)
ProcessArchiveHandler.tar_locks = {}


class GCFormatter(logging.Formatter):
	def __init__(self, details_lt = logging.DEBUG, details_gt = logging.ERROR, ex_context = 0, ex_vars = 0, ex_fstack = 0, ex_tree = 2):
		logging.Formatter.__init__(self)
		self._force_details_range = (details_lt, details_gt)
		(self._ex_context, self._ex_vars, self._ex_fstack, self._ex_tree) = (ex_context, ex_vars, ex_fstack, ex_tree)

	def __repr__(self):
		return '%s(quiet = %r, code = %r, var = %r, file = %r, tree = %r)' % (self.__class__.__name__,
			tuple(map(logging.getLevelName, self._force_details_range)), self._ex_context, self._ex_vars, self._ex_fstack, self._ex_tree)

	def format(self, record):
		record.message = record.getMessage()
		try:
			force_time = record.print_time
		except Exception:
			force_time = False
		force_details = (record.levelno <= self._force_details_range[0]) or (record.levelno >= self._force_details_range[1])
		if force_time or force_details:
			record.asctime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created))
		if force_details:
			msg = '%(asctime)s - %(name)s:%(levelname)s - %(message)s' % record.__dict__
		elif force_time:
			msg = '%(asctime)s - %(message)s' % record.__dict__
		else:
			msg = record.message
		if record.exc_info:
			if not msg.endswith('\n'):
				msg += ': '
			msg += format_exception(record.exc_info, self._ex_context, self._ex_vars, self._ex_fstack, self._ex_tree)
		return msg


def clean_logger(logger_name = None):
	logger = logging.getLogger(logger_name)
	logger.handlers = []
	return logger


def register_handler(logger, handler, formatter):
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	return handler


def get_debug_file_candidates():
	return [
		os.path.join(os.environ['GC_PACKAGES_PATH'], '..', 'debug.log'),
		'/tmp/gc.debug.%d.%d' % (os.getuid(), os.getpid()),
		'~/gc.debug'
	]


def logging_defaults():
	formatter_verbose = GCFormatter(ex_context = 2, ex_vars = 1, ex_fstack = 1, ex_tree = 2)
	root_logger = clean_logger()
	root_logger.manager.loggerDict.clear()
	root_logger.setLevel(logging.DEFAULT)
	root_handler = register_handler(root_logger, StdoutStreamHandler(), formatter_verbose)

	# Setup logger used for abort messages
	abort_logger = clean_logger('abort')
	abort_logger.propagate = False
	abort_handler = register_handler(abort_logger, StderrStreamHandler(), formatter_verbose)

	# Output verbose exception information into dedicated GC log (in gc / tmp / user directory) if possible
	try:
		register_handler(abort_logger, GCLogHandler(get_debug_file_candidates(), mode = 'w'), formatter_verbose)
		formatter_quiet = GCFormatter(ex_context = 0, ex_vars = 0, ex_fstack = 0, ex_tree = 1)
		abort_handler.setFormatter(formatter_quiet)
		root_handler.setFormatter(formatter_quiet)
	except Exception: # otherwise use verbose settings for default output
		clear_current_exception()

	# External libraries
	logging.getLogger('requests').setLevel(logging.WARNING)

	# Adding log_process_result to Logging class
	def log_process(self, proc, level = logging.WARNING, files = None, msg = None):
		msg = msg or 'Process %(call)s finished with exit code %(proc_status)s'
		status = proc.status(timeout = 0)
		record = self.makeRecord(self.name, level, '<process>', 0, msg, tuple(), None)
		record.proc = proc
		record.call = proc.get_call()
		record.proc_status = status
		record.files = files or {}
		record.msg = record.msg % record.__dict__
		self.handle(record)
	logging.Logger.log_process = log_process

	# Adding log with time prefix to Logging class
	def log_time(self, level, msg, *args, **kwargs):
		if self.isEnabledFor(level):
			tmp = self.findCaller()
			record = self.makeRecord(self.name, level, tmp[0], tmp[1], msg, args, kwargs.pop('exc_info', None))
			record.print_time = True
			self.handle(record)
	logging.Logger.log_time = log_time


# Display logging setup
def dump_log_setup(level):
	output = logging.getLogger('logging')

	def display_logger(indent, logger, name):
		propagate_symbol = '+'
		if hasattr(logger, 'propagate') and not logger.propagate:
			propagate_symbol = 'o'
		desc = name
		if hasattr(logger, 'level'):
			desc += ' (level = %s)' % logging.getLevelName(logger.level)
		output.log(level, '%s%s %s', '|  ' * indent, propagate_symbol, desc)
		if hasattr(logger, 'handlers'):
			for handler in logger.handlers:
				output.log(level, '%s> %s', '|  ' * (indent + 1), handler.__class__.__name__)
				fmt = handler.formatter
				if fmt:
					desc = fmt.__class__.__name__
					if isinstance(fmt, GCFormatter):
						desc = repr(fmt)
					elif isinstance(fmt, logging.Formatter):
						desc += '(%s, %s)' % (repr(getattr(fmt, '_fmt')), repr(fmt.datefmt))
					output.log(level, '%s|  %% %s', '|  ' * (indent + 1), desc)
				if hasattr(handler, 'filters'):
					for lf in handler.filters:
						output.log(level, '%s  # %s', '|  ' * (indent + 1), lf.__class__.__name__)

	display_logger(0, logging.getLogger(), '<root>')
	for key, logger in sorted(logging.getLogger().manager.loggerDict.items()):
		display_logger(key.count('.') + 1, logger, key)


# Configure formatting of handlers
def logging_configure_handler(config, logger_name, handler_str, handler):
	def get_handler_option(postfix):
		return ['%s %s' % (logger_name, postfix), '%s %s %s' % (logger_name, handler_str, postfix)]
	fmt = GCFormatter(
		details_lt = config.getEnum(get_handler_option('detail lower limit'), LogLevelEnum, logging.DEBUG, onChange = None),
		details_gt = config.getEnum(get_handler_option('detail upper limit'), LogLevelEnum, logging.ERROR, onChange = None),
		ex_context = config.getInt(get_handler_option('code context'), 2, onChange = None),
		ex_vars = config.getInt(get_handler_option('variables'), 1, onChange = None),
		ex_fstack = config.getInt(get_handler_option('file stack'), 1, onChange = None),
		ex_tree = config.getInt(get_handler_option('tree'), 2, onChange = None))
	handler.setFormatter(fmt)
	return handler


# Configure general setup of loggers - destinations, level and propagation
def logging_create_handlers(config, logger_name):
	logger = logging.getLogger(logger_name.lower().replace('exception', 'abort').replace('root', ''))
	# Setup handlers
	handler_list = config.getList(logger_name + ' handler', [], onChange = None)
	if handler_list: # remove any standard handlers:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)
	else:
		for handler in logger.handlers:
			logging_configure_handler(config, logger_name, '', handler)
	for handler_str in UniqueList(handler_list): # add only unique output handlers
		if handler_str == 'stdout':
			handler = StdoutStreamHandler()
		elif handler_str == 'stderr':
			handler = StderrStreamHandler()
		elif handler_str == 'file':
			handler = logging.FileHandler(config.get(logger_name + ' file', onChange = None), 'w')
		elif handler_str == 'debug_file':
			handler = GCLogHandler(config.getPaths(logger_name + ' debug file', get_debug_file_candidates(), onChange = None, mustExist = False), 'w')
		else:
			raise Exception('Unknown handler %s for logger %s' % (handler_str, logger_name))
		logger.addHandler(logging_configure_handler(config, logger_name, handler_str, handler))
		logger.propagate = False
	# Set propagate status
	logger.propagate = config.getBool(logger_name + ' propagate', bool(logger.propagate), onChange = None)
	# Set logging level
	logger.setLevel(config.getEnum(logger_name + ' level', LogLevelEnum, logger.level, onChange = None))


# Apply configuration to logging setup
def logging_setup(config):
	if config.getBool('debug mode', False, onChange = None):
		config.set('level', 'NOTSET', '?=')
		config.set('detail lower limit', 'NOTSET')
		config.set('detail upper limit', 'NOTSET')
		config.set('abort handler', 'stdout debug_file', '?=')
		config.setInt('abort code context', 2)
		config.setInt('abort variables', 2)
		config.setInt('abort file stack', 2)
		config.setInt('abort tree', 2)
	display_logger = config.getBool('display logger', False, onChange = None)

	# Find logger names in options
	logger_names = set()
	for option in config.getOptions():
		if option in ['debug mode', 'display logger']:
			pass
		elif option.count(' ') == 0:
			logger_names.add('')
		else:
			logger_names.add(option.split(' ')[0].strip())
	logger_names = sorted(logger_names)
	logger_names.reverse()
	for logger_name in logger_names:
		logging_create_handlers(config, logger_name)

	if display_logger:
		dump_log_setup(logging.WARNING)
