#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
import sys, time, logging
from testFwk import create_config, remove_files_testsuite, run_test, setPath, try_catch, write_file
from grid_control.logging_setup import GCFormatter, GCStreamHandler, LogEveryNsec, LogLevelEnum, ProcessArchiveHandler, StderrStreamHandler, StdoutStreamHandler, dump_log_setup, logging_defaults, logging_setup
from grid_control.utils.process_base import LocalProcess


def test_levels(log):
	for level in [logging.NOTSET, logging.DEBUG3, logging.DEBUG2, logging.DEBUG1, logging.DEBUG,
		logging.INFO3, logging.INFO2, logging.INFO1, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]:
			log.log(level, LogLevelEnum.enum2str(level))

def create_log_config(settings = None):
	return create_config(config_dict = {'logging': settings or {}}).change_view(setSections = ['logging'])

class Test_LoggingSetup:
	"""
	>>> remove_files_testsuite(['test.log'])

	>>> logging_defaults()
	>>> dump_log_setup(logging.INFO)
	+ <root> (level = DEFAULT)
	|  > StdoutStreamHandler
	|  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  o abort (level = NOTSET)
	|  |  > StderrStreamHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  |  > GCLogHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 200, file = 1, tree = 2)
	|  + logging (level = NOTSET)
	|  + requests (level = WARNING)
	>>> test_levels(logging.getLogger('random'))
	INFO
	WARNING
	0000-00-00 00:00:00 - random:ERROR - ERROR
	0000-00-00 00:00:00 - random:CRITICAL - CRITICAL

	>>> logging_defaults()
	>>> logging_setup(create_log_config())
	>>> dump_log_setup(logging.INFO)
	+ <root> (level = DEFAULT)
	|  > StdoutStreamHandler
	|  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  > ProcessArchiveHandler
	|  o abort (level = NOTSET)
	|  |  > StderrStreamHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  |  > GCLogHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 200, file = 1, tree = 2)
	|  + classloader
	|  |  + classloader.configfiller (level = NOTSET)
	|  + config
	|  |  o config.stored (level = NOTSET)
	|  |  + config.unnamed (level = NOTSET)
	|  + logging (level = NOTSET)
	|  |  + logging.process (level = NOTSET)
	|  + requests (level = WARNING)
	>>> test_levels(logging.getLogger('random'))
	INFO
	WARNING
	0000-00-00 00:00:00 - random:ERROR - ERROR
	0000-00-00 00:00:00 - random:CRITICAL - CRITICAL

	>>> logging_defaults()
	>>> logging_setup(create_log_config({'level': 'DEBUG1'}))
	>>> dump_log_setup(logging.INFO)
	+ <root> (level = DEBUG1)
	|  > StdoutStreamHandler
	|  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 200, file = 1, tree = 2)
	|  > ProcessArchiveHandler
	|  o abort (level = NOTSET)
	|  |  > StderrStreamHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  |  > GCLogHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 200, file = 1, tree = 2)
	|  + classloader
	|  |  + classloader.configfiller (level = NOTSET)
	|  + config
	|  |  o config.stored (level = NOTSET)
	|  |  + config.unnamed (level = NOTSET)
	|  + logging (level = NOTSET)
	|  |  + logging.process (level = NOTSET)
	|  + requests (level = WARNING)
	>>> test_levels(logging.getLogger('random'))
	0000-00-00 00:00:00 - random:DEBUG1 - DEBUG1
	0000-00-00 00:00:00 - random:DEBUG - DEBUG
	INFO3
	INFO2
	INFO1
	INFO
	WARNING
	0000-00-00 00:00:00 - random:ERROR - ERROR
	0000-00-00 00:00:00 - random:CRITICAL - CRITICAL

	>>> logging_defaults()
	>>> logging_setup(create_log_config({'debug mode': True, 'handler': 'stdout file', 'file': 'test.log', 'logging handler': 'stdout', 'logging detail lower limit': 'NOTSET', 'logging detail upper limit': 'Level 2'}))
	>>> dump_log_setup(1)
	o <root> (level = NOTSET)
	|  > StdoutStreamHandler
	|  |  % GCFormatter(quiet = ('NOTSET', 'NOTSET'), code = 2, var = 200, file = 1, tree = 2)
	|  > FileHandler
	|  |  % GCFormatter(quiet = ('NOTSET', 'NOTSET'), code = 2, var = 200, file = 1, tree = 2)
	|  > ProcessArchiveHandler
	|  o abort (level = NOTSET)
	|  |  > StdoutStreamHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 1000, file = 2, tree = 2)
	|  |  > GCLogHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 1000, file = 2, tree = 2)
	|  + classloader
	|  |  + classloader.configfiller (level = NOTSET)
	|  + config
	|  |  o config.stored (level = NOTSET)
	|  |  + config.unnamed (level = NOTSET)
	|  + detail (level = NOTSET)
	|  o logging (level = NOTSET)
	|  |  > StdoutStreamHandler
	|  |  |  % GCFormatter(quiet = ('NOTSET', 'Level 2'), code = 2, var = 200, file = 1, tree = 2)
	|  |  + logging.process (level = NOTSET)
	|  + requests (level = WARNING)
	>>> test_levels(logging.getLogger('random'))
	0000-00-00 00:00:00 - random:DEBUG3 - DEBUG3
	0000-00-00 00:00:00 - random:DEBUG2 - DEBUG2
	0000-00-00 00:00:00 - random:DEBUG1 - DEBUG1
	0000-00-00 00:00:00 - random:DEBUG - DEBUG
	0000-00-00 00:00:00 - random:INFO3 - INFO3
	0000-00-00 00:00:00 - random:INFO2 - INFO2
	0000-00-00 00:00:00 - random:INFO1 - INFO1
	0000-00-00 00:00:00 - random:INFO - INFO
	0000-00-00 00:00:00 - random:WARNING - WARNING
	0000-00-00 00:00:00 - random:ERROR - ERROR
	0000-00-00 00:00:00 - random:CRITICAL - CRITICAL

	>>> remove_files_testsuite(['test.log'])
	"""

def isolated_logger():
	log = logging.getLogger('test')
	log.propagate = False
	log.handlers = []
	return log

def f(log):
	try:
		raise Exception('test error')
	except Exception:
		log.warning('log message', exc_info = True)

class Test_ExceptionLogging:
	"""
	>>> logging_defaults()
	>>> log = isolated_logger()
	>>> handler = StdoutStreamHandler()
	>>> handler.setFormatter(GCFormatter(details_gt = logging.CRITICAL, ex_tree = 1))
	>>> log.addHandler(handler)
	>>> f(log)
	log message: Exception: test error
	"""

class Test_Logging:
	"""
	>>> logging_defaults()
	>>> handler = GCStreamHandler()
	>>> try_catch(handler.get_stream, 'AbstractError', 'is an abstract function!')
	caught

	>>> handler = StdoutStreamHandler()
	>>> handler.addFilter(LogEveryNsec(interval = 1))
	>>> log = isolated_logger()
	>>> log.addHandler(handler)
	>>> dump_log_setup(logging.INFO)
	+ <root> (level = DEFAULT)
	|  > StdoutStreamHandler
	|  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  o abort (level = NOTSET)
	|  |  > StderrStreamHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  |  > GCLogHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 200, file = 1, tree = 2)
	|  + logging (level = NOTSET)
	|  + requests (level = WARNING)
	>>> log.critical('space is low')
	space is low
	>>> log.critical('space is low')
	>>> log.critical('space is low')
	>>> time.sleep(2)
	>>> log.critical('space is low')
	space is low
	"""

class Test_LoggingException:
	"""
	>>> logging_defaults()
	>>> sys.stderr = None
	>>> log = isolated_logger()
	>>> log.addHandler(StderrStreamHandler())
	>>> try_catch(lambda: log.warning('test'), 'AttributeError', 'object has no attribute')
	caught
	"""

class Test_ProcessLogging:
	"""
	>>> logging_defaults()
	>>> setPath('bin')
	>>> write_file('log.stdout', 'STDOUT Test')
	>>> write_file('log.stderr', 'STDERR Test')
	>>> p1 = LocalProcess('io_test', 'Hallo', '"Welt !"', environment = {'GC_TEST_STDOUT': 'log.stdout', 'GC_TEST_STDERR': 'log.stderr', 'GC_TEST_RET': '123'})
	>>> p1.status(timeout = 10)
	123
	>>> log = logging.getLogger()
	>>> log.addHandler(ProcessArchiveHandler('log.tar'))
	>>> log.log_process(p1)
	Process '<testsuite dir>/bin/io_test' 'Hallo' '"Welt !"' finished with exit code 123
	All logfiles were moved to log.tar

	>>> p2 = LocalProcess('io_test', 'Hallo', '"Welt !"', environment = {'GC_TEST_RET': '0'})
	>>> p2.status(timeout = 10)
	0
	>>> log.log_process(p2, msg = 'Process p2 has finished!')
	Process p2 has finished!
	All logfiles were moved to log.tar

	>>> remove_files_testsuite(['log.*'])
	"""

class Test_Logging:
	"""
	>>> logging_defaults()
	>>> handler = GCStreamHandler()
	>>> try_catch(handler.get_stream, 'AbstractError', 'is an abstract function!')
	caught

	>>> handler = StdoutStreamHandler()
	>>> handler.addFilter(LogEveryNsec(interval = 1))
	>>> log = isolated_logger()
	>>> log.addHandler(handler)
	>>> dump_log_setup(logging.INFO)
	+ <root> (level = DEFAULT)
	|  > StdoutStreamHandler
	|  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  o abort (level = NOTSET)
	|  |  > StderrStreamHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 0, var = 0, file = 0, tree = 1)
	|  |  > GCLogHandler
	|  |  |  % GCFormatter(quiet = ('DEBUG', 'ERROR'), code = 2, var = 200, file = 1, tree = 2)
	|  + logging (level = NOTSET)
	|  + requests (level = WARNING)
	|  o test (level = NOTSET)
	|  |  > StdoutStreamHandler
	|  |    # LogEveryNsec

	>>> log.critical('space is low')
	space is low
	>>> log.critical('space is low')
	>>> log.critical('space is low')
	>>> time.sleep(2)
	>>> log.critical('space is low')
	space is low
	"""

run_test()
