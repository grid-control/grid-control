#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import TestStream, create_config, remove_files_testsuite, run_test, try_catch, write_file
from grid_control.config.cfiller_base import ConfigFiller


def try_parse(content, msg):
	write_file('parser.conf', content)
	try_catch(lambda: create_config('parser.conf'), 'ConfigError', msg)

def test_filler(name, *args, **kwargs):
	cf = ConfigFiller.create_instance(name, *args, **kwargs)
	config = create_config(additional = [cf])
	config.write(TestStream(), print_default = False)

class Test_ConfigFiller:
	"""
	>>> try_catch(lambda: test_filler('ConfigFiller'), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_StringConfigFiller:
	"""
	>>> try_catch(lambda: test_filler('StringConfigFiller', ['key = value']), 'ConfigError', 'Unable to parse section')
	caught
	>>> try_catch(lambda: test_filler('StringConfigFiller', ['[section] key']), 'ConfigError', 'Unable to parse option')
	caught
	>>> test_filler('StringConfigFiller', ['[section] key = value', '[a] b = c'])
	[a]
	b = c
	-----
	[section]
	key = value
	-----
	"""

class Test_FileConfigFiller:
	"""
	>>> try_parse('key = value', 'Found config option outside of config section')
	caught
	>>> try_parse('  key = value', 'Invalid indentation')
	caught
	>>> try_parse('[section', 'Unable to parse config section')
	caught
	>>> try_parse('[section]\\nkey', 'Please use "key = value" syntax or indent values!')
	caught
	"""

class Test_PythonConfigFiller:
	"""
	>>> test_filler('GeneralFileConfigFiller', ['test.confpy'])
	vvvvvvvvvv
	[jobs]
	jobs = 2
	---
	[jobs test workflow:global]
	wall time = 1:00
	---
	[usertask]
	arguments = arg1 arg2 arg3
	dataset = Example05_dataset.dbs
	  :file:/bin/sh|3
	  :file:/bin/bash|3
	files per job = 2
	---
	[workflow]
	backend = Host
	^^^^^^^^^^
	[jobs]
	jobs = 2
	-----
	[jobs test workflow:global]
	wall time = 1:00
	-----
	[usertask]
	arguments = arg1 arg2 arg3
	dataset =
	  Example05_dataset.dbs
	  :file:/bin/sh|3
	  :file:/bin/bash|3
	files per job = 2
	-----
	[workflow]
	backend = Host
	-----
	"""

class Test_CompatConfigFiller:
	"""
	>>> test_filler('CompatConfigFiller', 'test.ini')
	[jobs]
	seeds = 1000 2000 3000 4000
	-----
	[parameters]
	parameter hash = abcdabcdabcdabcd
	-----
	[task]
	task date = 0000-00-00
	task id = GC12345678
	-----
	"""

run_test(exit_fun = lambda: remove_files_testsuite(['parser.conf']))
