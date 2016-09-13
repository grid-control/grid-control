#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
import os, sys, signal
from testFwk import TestStream, run_test, try_catch
from gcTool import gc_create_config, handle_abort_interrupt
from grid_control import utils


def my_exit_with_usage(*args, **kwargs):
	args = tuple([os.path.basename(args[0])] + list(args[1:]))
	if len(args) > 1 and ('Show this helpful message' in args[1]):
		args = tuple([args[0], '<help message>'] + list(args[2:]))
	print('exit_with_usage:%s' % repr((args, kwargs)))
	sys.exit(1)
utils.exit_with_usage = my_exit_with_usage

def my_deprecated(text):
	print('deprecated:%r' % text)
	sys.exit(1)
utils.deprecated = my_deprecated

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

	>>> gc_create_config(['test.conf']).write(TestStream(), print_default = False)
	[global]
	cmdargs = -G
	gui = ANSIGUI
	plugin paths += <testsuite dir>
	submission = True
	-----
	[logging]
	debug mode = False
	-----

	>>> gc_create_config(['test.conf', '--logging', 'process', '--action', 's'], use_default_files = False).write(TestStream(), print_default = False)
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
	>>> handle_abort_interrupt(signal.SIGINT, None)
	>>> utils.abort()
	True
	"""

run_test()
