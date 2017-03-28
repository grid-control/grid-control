#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, sys, signal
from testfwk import TestsuiteStream, run_test, testfwk_remove_files, try_catch
from grid_control import utils
from grid_control_api import _gc_run, _trigger_debug_signal, gc_create_config, handle_abort_interrupt
from python_compat import StringBuffer


def my_exit_with_usage(*args, **kwargs):
	args = tuple([os.path.basename(args[0])] + list(args[1:]))
	if len(args) > 1 and ('Show this helpful message' in args[1]):
		args = tuple([args[0], '<help message>'] + list(args[2:]))
	print('exit_with_usage:%s' % repr((args, kwargs)))
	sys.exit(1)
sys.modules['grid_control_api'].exit_with_usage = my_exit_with_usage


def my_deprecated(text):
	print('deprecated:%r' % text)
	sys.exit(1)
sys.modules['grid_control_api'].deprecated = my_deprecated


def check_trigger():
	def _handler(*args):
		check_trigger.flag = True
	signal.signal(signal.SIGURG, _handler)
	_trigger_debug_signal(0.1)
	while not check_trigger.flag:
		pass
check_trigger.flag = False
check_trigger()

os.environ['GC_TERM'] = ''

class Test_Run:
	"""
	>>> try_catch(lambda: gc_create_config([]), 'SystemExit')
	exit_with_usage:(('TEST_go.py [OPTIONS] <config file>', 'Config file not specified!'), {})
	Exit with 1

	>>> try_catch(lambda: gc_create_config(['--help']), 'SystemExit')
	exit_with_usage:(('TEST_go.py [OPTIONS] <config file>', '<help message>'), {'show_help': False})
	Exit with 1

	>>> try_catch(lambda: gc_create_config(['123.conf', '456.conf']), 'SystemExit')
	exit_with_usage:(('TEST_go.py [OPTIONS] <config file>', "Invalid command line arguments: ['123.conf', '456.conf']"), {})
	Exit with 1

	>>> try_catch(lambda: gc_create_config(['--time-report', '123.conf']), 'SystemExit')
	deprecated:'Please use the more versatile report tool in the scripts directory!'
	Exit with 1

	>>> gc_create_config(['test.conf']).write(TestsuiteStream(), print_default=False)
	[global]
	cmdargs = -G
	gui = ANSIGUI
	plugin paths += <testsuite dir>
	submission = True
	-----
	[logging]
	debug mode = False
	-----

	>>> gc_create_config(['test.conf', '--logging', 'process', '--action', 's'], use_default_files=False).write(TestsuiteStream(), print_default=False)
	[global]
	cmdargs = -G
	gui = ANSIGUI
	plugin paths += <testsuite dir>
	submission = True
	-----
	[logging]
	debug mode = False
	process level = DEBUG
	-----
	[workflow]
	action = s
	-----

	>>> utils.abort()
	False
	>>> handle_abort_interrupt(signal.SIGINT, None, StringBuffer())
	>>> utils.abort()
	True
	>>> utils.abort(False)
	False

	>>> args_base = ['$GC_PACKAGES_PATH/../docs/examples/Example03_include.conf', '-o', '[logging] activity stream = null', '-o', '[global] workdir create = True', '-o', '[global] gui = TestsuiteGUIFail', '-o', '[global] backend = Host']
	>>> try_catch(lambda: _gc_run(args_base + ['-o', '[global] workdir = gcrun_test1']), 'GUIException', 'GUI init exception')
	caught
	>>> testfwk_remove_files(['gcrun_test1/*', 'gcrun_test1'])
	>>> utils.abort(False)
	False
	>>> _gc_run(args_base + ['-o', '[global] workdir = gcrun_test2', '-o', '[global] task = TestsuiteAbortTask'])
	>>> testfwk_remove_files(['gcrun_test2/*', 'gcrun_test2'])
	>>> utils.abort(False)
	False
	>>> _gc_run(args_base + ['-o', '[global] workdir = gcrun_test3', '-o', '[global] backend = TestsuiteAbortBackend'])
	>>> testfwk_remove_files(['gcrun_test3/*', 'gcrun_test3'])
	>>> utils.abort(False)
	False
	>>> _gc_run(args_base + ['-o', '[global] workdir = gcrun_test4', '-o', '[jobs] monitor = TestsuiteAbortMonitor'])
	>>> testfwk_remove_files(['gcrun_test4/*', 'gcrun_test4'])
	>>> utils.abort(False)
	False
	"""

run_test()
