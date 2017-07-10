# | Copyright 2013-2017 Karlsruhe Institute of Technology
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
from grid_control.stream_base import ActivityMonitor
from grid_control.utils.data_structures import UniqueList, make_enum
from grid_control.utils.file_tools import SafeFile, VirtualFile
from grid_control.utils.thread_tools import GCLock, with_lock
from hpfwk import AbstractError, format_exception, ignore_exception, rethrow
from python_compat import any, imap, irange, lmap, set, sorted, tarfile


LogLevelEnum = make_enum(lmap(lambda level: logging.getLevelName(level).upper(), irange(51)),  # pylint:disable=invalid-name
	use_hash=False, register=False)


def clean_logger(logger_name=None):
	logger = logging.getLogger(logger_name)
	logger.handlers = []
	return logger


def dump_log_setup(level):
	# Display logging setup
	output = logging.getLogger('logging')

	def _display_logger(indent, logger, name):
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
					for log_filter in handler.filters:
						output.log(level, '%s  # %s', '|  ' * (indent + 1), log_filter.__class__.__name__)

	_display_logger(0, logging.getLogger(), '<root>')
	for key, logger in sorted(logging.getLogger().manager.loggerDict.items()):
		_display_logger(key.count('.') + 1, logger, key)


def get_debug_file_candidates():
	return [
		os.path.join(os.environ['GC_PACKAGES_PATH'], '..', 'debug.log'),
		'/tmp/gc.debug.%d.%d' % (os.getuid(), os.getpid()),
		'~/gc.debug'
	]


def logging_configure_handler(config, logger_name, handler_str, handler):
	# Configure formatting of handlers
	def _get_handler_option(postfix):
		return ['%s %s' % (logger_name, postfix), '%s %s %s' % (logger_name, handler_str, postfix)]
	fmt = GCFormatter(
		details_lt=config.get_enum(_get_handler_option('detail lower limit'),
			LogLevelEnum, logging.DEBUG, on_change=None),
		details_gt=config.get_enum(_get_handler_option('detail upper limit'),
			LogLevelEnum, logging.ERROR, on_change=None),
		ex_context=config.get_int(_get_handler_option('code context'), 2, on_change=None),
		ex_vars=config.get_int(_get_handler_option('variables'), 200, on_change=None),
		ex_fstack=config.get_int(_get_handler_option('file stack'), 1, on_change=None),
		ex_tree=config.get_int(_get_handler_option('tree'), 2, on_change=None),
		ex_threads=config.get_int(_get_handler_option('thread stack'), 1, on_change=None))
	handler.setFormatter(fmt)
	return handler


def logging_create_handlers(config, logger_name):
	# Configure general setup of loggers - destinations, level and propagation
	logger = logging.getLogger(logger_name.lower().replace('exception', 'abort').replace('root', ''))
	# Setup handlers
	handler_list = config.get_list(logger_name + ' handler', [], on_change=None)
	if handler_list:  # remove any standard handlers:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)
	else:
		for handler in logger.handlers:
			logging_configure_handler(config, logger_name, '', handler)
	for handler_str in UniqueList(handler_list):  # add only unique output handlers
		if handler_str == 'stdout':
			handler = StdoutStreamHandler()
		elif handler_str == 'stderr':
			handler = StderrStreamHandler()  # pylint:disable=redefined-variable-type
		elif handler_str == 'file':
			handler = logging.FileHandler(config.get(logger_name + ' file', on_change=None), 'w')
		elif handler_str == 'debug_file':
			handler = GCLogHandler(config.get_fn_list(logger_name + ' debug file',
				get_debug_file_candidates(), on_change=None, must_exist=False), 'w')
		else:
			raise Exception('Unknown handler %s for logger %s' % (handler_str, logger_name))
		logger.addHandler(logging_configure_handler(config, logger_name, handler_str, handler))
		logger.propagate = False
	# Set propagate status
	logger.propagate = config.get_bool(logger_name + ' propagate',
		bool(logger.propagate), on_change=None)
	# Set logging level
	logger.setLevel(config.get_enum(logger_name + ' level',
		LogLevelEnum, logger.level, on_change=None))


def logging_defaults():
	formatter_verbose = GCFormatter(ex_context=2, ex_vars=200, ex_fstack=1, ex_tree=2, ex_threads=1)
	root_logger = clean_logger()
	root_logger.manager.loggerDict.clear()
	root_logger.setLevel(logging.DEFAULT)
	root_handler = register_handler(root_logger, StdoutStreamHandler(), formatter_verbose)

	# Setup logger used for abort messages
	abort_logger = clean_logger('abort')
	abort_logger.propagate = False
	abort_handler = register_handler(abort_logger, StderrStreamHandler(), formatter_verbose)

	# Output verbose exception information into dedicated GC log (in gc / tmp / user directory)
	ignore_exception(Exception, None, _register_debug_log,
		abort_logger, abort_handler, formatter_verbose, root_handler)

	# External libraries
	logging.getLogger('requests').setLevel(logging.WARNING)

	# Adding log_process_result to Logging class
	def _log_process(self, proc, level=logging.WARNING, files=None, msg=None):
		msg = msg or 'Process %(call)s finished with exit code %(proc_status)s'
		status = proc.status(timeout=0)
		record = self.makeRecord(self.name, level, '<process>', 0, msg, tuple(), None)
		record.proc = proc
		record.call = proc.get_call()
		record.proc_status = status
		record.files = files or {}
		record.msg = record.msg % record.__dict__
		self.handle(record)
	logging.Logger.log_process = _log_process

	# Adding log with time prefix to Logging class
	def _log_time(self, level, msg, *args, **kwargs):
		if self.isEnabledFor(level):
			tmp = self.findCaller()
			record = self.makeRecord(self.name, level,
				tmp[0], tmp[1], msg, args, kwargs.pop('exc_info', None))
			record.print_time = True
			self.handle(record)
	logging.Logger.log_time = _log_time


def logging_setup(config):
	# Apply configuration to logging setup
	if config.get_bool('debug mode', False, on_change=None):
		config.set('level', 'NOTSET', '?=')
		config.set('detail lower limit', 'NOTSET', '?=')
		config.set('detail upper limit', 'NOTSET', '?=')
		config.set('abort handler', 'stdout debug_file', '?=')
		config.set_int('abort code context', 2, '?=')
		config.set_int('abort variables', 1000, '?=')
		config.set_int('abort file stack', 2, '?=')
		config.set_int('abort tree', 2, '?=')

	do_display_logger = config.get_bool('display logger', False, on_change=None)

	# Find logger names in options
	logger_names_set = set()
	for option in config.get_option_list():
		if any(imap(option.startswith, ['debug mode', 'display logger', 'activity stream'])):
			pass
		elif option.count(' ') == 0:
			logger_names_set.add('')
		else:
			logger_names_set.add(option.split(' ')[0].strip())
	logger_names = sorted(logger_names_set)
	logger_names.reverse()
	for logger_name in logger_names:
		logging_create_handlers(config, logger_name)

	logging.getLogger().addHandler(ProcessArchiveHandler(config.get_work_path('error.tar')))

	if do_display_logger:
		dump_log_setup(logging.WARNING)

	# Setup activity logs
	GCStreamHandler.push_std_stream(
		config.get_plugin(['activity stream', 'activity stream stdout'], 'DefaultActivityMonitor',
			cls=ActivityMonitor, require_plugin=False, pargs=(sys.stdout,), on_change=None,
			pkwargs={'register_callback': True}),
		config.get_plugin(['activity stream', 'activity stream stderr'], 'DefaultActivityMonitor',
			cls=ActivityMonitor, require_plugin=False, pargs=(sys.stderr,), on_change=None))


def parse_logging_args(arg_list):
	for entry in arg_list:
		tmp = entry.replace(':', '=').split('=')
		if len(tmp) == 1:
			if tmp[0] in LogLevelEnum.enum_name_list:
				tmp.insert(0, '')  # use root logger
			else:
				tmp.append('DEBUG')  # default is to set debug level
		yield (tmp[0], tmp[1])


def register_handler(logger, handler, formatter=None):
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	return handler


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


class GCFormatter(logging.Formatter):
	def __init__(self, details_lt=logging.DEBUG, details_gt=logging.ERROR,
			ex_context=0, ex_vars=0, ex_fstack=0, ex_tree=2, ex_threads=1):
		logging.Formatter.__init__(self)
		self._force_details_range = (details_lt, details_gt)
		(self._ex_context, self._ex_vars) = (ex_context, ex_vars)
		(self._ex_fstack, self._ex_tree, self._ex_threads) = (ex_fstack, ex_tree, ex_threads)

	def __repr__(self):
		return '%s(quiet = %r, code = %r, var = %r, file = %r, tree = %r, thread = %r)' % (
			self.__class__.__name__, tuple(imap(logging.getLevelName, self._force_details_range)),
			self._ex_context, self._ex_vars, self._ex_fstack, self._ex_tree, self._ex_threads)

	def format(self, record):
		record.message = record.getMessage()
		force_time = False
		if hasattr(record, 'print_time'):
			force_time = record.print_time
		force_details = (record.levelno <= self._force_details_range[0])
		force_details = force_details or (record.levelno >= self._force_details_range[1])
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
			msg += format_exception(record.exc_info,
				self._ex_context, self._ex_vars, self._ex_fstack, self._ex_tree, self._ex_threads)
		return msg


class GCStreamHandler(logging.Handler):
	global_instances = []
	global_lock = GCLock()

	# In contrast to StreamHandler, this logging handler doesn't keep a stream copy
	def __init__(self):
		logging.Handler.__init__(self)
		self.lock = GCLock(threading.RLock())  # default-allocated lock is sometimes non-reentrant
		self.global_lock = None
		GCStreamHandler.global_instances.append(self)

	def __del__(self):
		GCStreamHandler.global_instances.remove(self)

	def emit(self, record):  # locking done by handle
		stream = self.get_stream()
		stream.write(self.format(record) + '\n')
		stream.flush()

	def get_stream(self):
		raise AbstractError

	def handle(self, record):
		filter_result = self.filter(record)
		if filter_result:
			with_lock(self.global_lock or self.lock, self.emit, record)
		return filter_result

	def pop_std_stream(cls):
		def _pop_std_stream(handler_cls):
			ignore_exception(AttributeError, None, lambda stream: stream.disable(), handler_cls.stream[-1])
			handler_cls.stream.pop()
			ignore_exception(AttributeError, None, lambda stream: stream.enable(), handler_cls.stream[-1])
		_pop_std_stream(StdoutStreamHandler)
		_pop_std_stream(StderrStreamHandler)
	pop_std_stream = classmethod(pop_std_stream)

	def push_std_stream(cls, stream_stdout, stream_stderr):
		def _push_std_stream(handler_cls, user_stream):
			ignore_exception(AttributeError, None, lambda stream: stream.disable(), handler_cls.stream[-1])
			handler_cls.stream.append(user_stream)
			ignore_exception(AttributeError, None, lambda stream: stream.enable(), handler_cls.stream[-1])
		_push_std_stream(StdoutStreamHandler, stream_stdout)
		_push_std_stream(StderrStreamHandler, stream_stderr)
	push_std_stream = classmethod(push_std_stream)

	def set_global_lock(cls, lock=None):
		GCStreamHandler.global_lock.acquire()
		for instance in GCStreamHandler.global_instances:
			instance.acquire()
			instance.global_lock = lock
			instance.release()
		GCStreamHandler.global_lock.release()
	set_global_lock = classmethod(set_global_lock)


class ProcessArchiveHandler(logging.Handler):
	tar_locks = {}

	def __init__(self, fn, log=None):
		logging.Handler.__init__(self)
		self._fn = fn
		self._log = log or logging.getLogger('logging.process')
		# safeguard against multiple tar file
		self._lock = ProcessArchiveHandler.tar_locks.setdefault(os.path.abspath(fn), threading.RLock())
		# overwrite python2.3 non-reentrant lock
		self.lock = threading.RLock()

	def emit(self, record):
		if record.pathname == '<process>':
			with_lock(self._lock, self._write_process_log, record)
			self._log.warning('All logfiles were moved to %s', self._fn)

	def _write_process_log(self, record):
		entry_time = time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(record.created))
		entry = '%s_%s.%03d' % (record.name, entry_time, int(record.msecs))
		files = record.files
		files['info'] = 'call=%s\nexit=%s\n' % (repr(record.proc.get_call()), record.proc.status(0))
		files['stdout'] = record.proc.stdout.read_log()
		files['stderr'] = record.proc.stderr.read_log()
		files['stdin'] = record.proc.stdin.read_log()

		def _log_tar():
			tar = tarfile.TarFile.open(self._fn, 'a')
			for key, value in record.files.items():
				if os.path.exists(value):
					value = SafeFile(value).read_close()
				file_obj = VirtualFile(os.path.join(entry, key), [value])
				info, handle = file_obj.get_tar_info()
				tar.addfile(info, handle)
				handle.close()
			tar.close()
		rethrow(GCError('Unable to log results of external call "%s" to "%s"' % (
			record.proc.get_call(), self._fn)), _log_tar)


class StderrStreamHandler(GCStreamHandler):
	stream = [sys.stderr]

	def get_stream(self):
		return StderrStreamHandler.stream[-1]


class StdoutStreamHandler(GCStreamHandler):
	stream = [sys.stdout]

	def get_stream(self):
		return StdoutStreamHandler.stream[-1]


def _register_debug_log(abort_logger, abort_handler, formatter_verbose, root_handler):
	register_handler(abort_logger,
		GCLogHandler(get_debug_file_candidates(), mode='w'), formatter_verbose)
	formatter_quiet = GCFormatter(ex_context=0, ex_vars=0, ex_fstack=0, ex_tree=1, ex_threads=0)
	abort_handler.setFormatter(formatter_quiet)
	root_handler.setFormatter(formatter_quiet)
