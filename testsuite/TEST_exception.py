#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import re, sys, logging
from testfwk import DummyObj, TestsuiteStream, create_config, get_logger, run_test, try_catch
from grid_control.gc_exceptions import GCLogHandler, gc_excepthook
from grid_control.logging_setup import GCFormatter
from hpfwk import NestedException, rethrow
from hpfwk.hpf_exceptions import impl_detail


gc_excepthook.restore_old_hook = False


def test(value, name):
	try:
		def _test(_name):
			raise Exception('Test' + _name)
		rethrow(value, _test, name)
	except Exception:
		gc_excepthook(*sys.exc_info())

def modify_log(lines):
	skip_next = False
	for line in lines:
		if skip_next:
			skip_next = False
			continue
		line = line.replace('./testsuite/', '')
		line = re.sub(r'\[(.*\.py):\d*\]', r'[\1:<lineno>]', line)
		line = re.sub(r'T(.*) (.*) (\d*) (.*)', r'T\1 \2 <lineno> \4', line)
		line = re.sub(r'Exception occured in grid-control \[\d+.*\]', 'Exception occured in grid-control...', line)
		if 'invalid syntax' in line:
			line = re.sub('invalid syntax.*', 'invalid syntax <details>', line)
		line = re.sub('ZeroDivisionError:.*', 'ZeroDivisionError: <details>', line)
		if ('Stack of threads' in line) or ('Stack of thread #' in line):
			yield 'Stack of threads...'
			break
		if 'MainThread' in line:
			continue
		yield line
TestsuiteStream.set_modify(modify_log)


def t3():
	local_3 = DummyObj(a=1, b=2)
	try:
		return 1/0
	except Exception:
		raise NestedException('Tier3')

class t2:
	def __init__(self):
		self._local_2 = 2
		self._password = 'secret'

	def __repr__(self):
		return '<t2 instance>'

	def run(self):
		try:
			t3()
		except Exception:
			raise NestedException('Tier2')

def t1():
	local_1 = 1
	local_1x = 'x' * 1000
	try:
		try:
			t2().run()
		except Exception:
			raise NestedException('Tier1_2')
	except Exception:
		raise NestedException('Tier1_1')

def t0():
	try:
		t1()
	except Exception:
		gc_excepthook(*sys.exc_info())

class BuggedObject(object):
	def __repr__(self):
		raise Exception('repr failed')

class SlotObject(object):
	__slots__ = ('a', 'b',)
	def __init__(self):
		self.a = 1
		self.b = 9

	def __repr__(self):
		return '<%s instance>' % self.__class__.__name__

	def fail(self):
		eval('syntax: error')

def tA():
	tmp1 = BuggedObject()
	tmp2 = 'x' * 1000
	tmp3 = SlotObject()
	try:
		tmp3.fail()
	except Exception:
		gc_excepthook(*sys.exc_info())

abort_logger = logging.getLogger('abort')
abort_logger.handlers = []
abort_logger.propagate = True

class Test_Fallback:
	"""
	>>> impl_detail(sys, 'getsizeof', ('HALLO',), -1) < 1000
	True
	>>> impl_detail(sys, '_test_fun', ('HALLO',), 1001) > 1000
	True
	"""

class Test_Exception:
	"""
	>>> get_logger().setFormatter(GCFormatter(ex_context=0, ex_vars=None, ex_fstack=0, ex_tree=10))
	>>> test(Exception('Hallo Welt'), '123')
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	Exception: Hallo Welt
	>>> test(NestedException('Hallo Welt'), 'xyz')
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	NestedException: Hallo Welt
	  Exception: Testxyz

	>>> get_logger().setFormatter(GCFormatter(ex_context=2, ex_vars=None, ex_fstack=10, ex_tree=10))
	>>> tA()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	Stack #T-01 [<testsuite dir>/TEST_exception.py:<lineno>] tA
	    |   try:
	  =>|     tmp3.fail()
	    |   except Exception:
	-----
	  Local variables:
	    tmp1 = unable to display!
	    tmp2 = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	    tmp3 = <SlotObject instance>
	-----
	Stack #T-02 [<testsuite dir>/TEST_exception.py:<lineno>] fail
	    |   def fail(self):
	  =>|     eval('syntax: error')
	    |
	-----
	  Class variables (<SlotObject instance>):
	    self.a = 1
	    self.b = 9
	-----
	File stack:
	T|01 <testsuite dir>/TEST_exception.py <lineno> (tA)
	T|02 <testsuite dir>/TEST_exception.py <lineno> (fail)
	-----
	SyntaxError: invalid syntax <details>
	             ('invalid syntax <details>

	>>> t0()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	Stack #T-01 [<testsuite dir>/TEST_exception.py:<lineno>] t0
	    |   try:
	  =>|     t1()
	    |   except Exception:
	-----
	Stack #T|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	    |     try:
	  =>|       t2().run()
	    |     except Exception:
	-----
	  Local variables:
	    local_1  = 1
	    local_1x = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	-----
	Stack #T|00|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] run
	    |     try:
	  =>|       t3()
	    |     except Exception:
	-----
	  Class variables (<t2 instance>):
	    self._local_2  = 2
	    self._password = <redacted>
	-----
	Stack #T|00|00|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t3
	    |   try:
	  =>|     return 1/0
	    |   except Exception:
	-----
	  Local variables:
	    local_3 = DummyObj({'a': 1, 'b': 2})
	-----
	Stack #T|00|00|00-02 [<testsuite dir>/TEST_exception.py:<lineno>] t3
	    |   except Exception:
	  =>|     raise NestedException('Tier3')
	    |
	-----
	  Local variables:
	    local_3 = DummyObj({'a': 1, 'b': 2})
	-----
	Stack #T|00|00-02 [<testsuite dir>/TEST_exception.py:<lineno>] run
	    |     except Exception:
	  =>|       raise NestedException('Tier2')
	    |
	-----
	  Class variables (<t2 instance>):
	    self._local_2  = 2
	    self._password = <redacted>
	-----
	Stack #T|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	    |     except Exception:
	  =>|       raise NestedException('Tier1_2')
	    |   except Exception:
	-----
	  Local variables:
	    local_1  = 1
	    local_1x = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	-----
	Stack #T-02 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	    |   except Exception:
	  =>|     raise NestedException('Tier1_1')
	    |
	-----
	  Local variables:
	    local_1  = 1
	    local_1x = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	-----
	File stack:
	T|01 <testsuite dir>/TEST_exception.py <lineno> (t0)
	T|00|00|01 <testsuite dir>/TEST_exception.py <lineno> (t1)
	T|00|00|00|01 <testsuite dir>/TEST_exception.py <lineno> (run)
	T|00|00|00|00|01 <testsuite dir>/TEST_exception.py <lineno> (t3)
	T|00|00|00|02 <testsuite dir>/TEST_exception.py <lineno> (t3)
	T|00|00|02 <testsuite dir>/TEST_exception.py <lineno> (run)
	T|00|01 <testsuite dir>/TEST_exception.py <lineno> (t1)
	T|02 <testsuite dir>/TEST_exception.py <lineno> (t1)
	-----
	NestedException: Tier1_1
	  NestedException: Tier1_2
	    NestedException: Tier2
	      NestedException: Tier3
	        ZeroDivisionError: <details>

	>>> get_logger().setFormatter(GCFormatter(ex_context=1, ex_vars=-1, ex_fstack=0, ex_tree=0))
	>>> t0()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	Stack #T-01 [<testsuite dir>/TEST_exception.py:<lineno>] t0
	  =>|     t1()
	-----
	Stack #T|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	  =>|       t2().run()
	-----
	Stack #T|00|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] run
	  =>|       t3()
	-----
	Stack #T|00|00|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t3
	  =>|     return 1/0
	-----
	Stack #T|00|00|00-02 [<testsuite dir>/TEST_exception.py:<lineno>] t3
	  =>|     raise NestedException('Tier3')
	-----
	Stack #T|00|00-02 [<testsuite dir>/TEST_exception.py:<lineno>] run
	  =>|       raise NestedException('Tier2')
	-----
	Stack #T|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	  =>|       raise NestedException('Tier1_2')
	-----
	Stack #T-02 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	  =>|     raise NestedException('Tier1_1')
	-----

	>>> get_logger().setFormatter(GCFormatter(ex_context=1, ex_vars=200, ex_fstack=0, ex_tree=0))
	>>> t0()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	Stack #T-01 [<testsuite dir>/TEST_exception.py:<lineno>] t0
	  =>|     t1()
	-----
	Stack #T|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	  =>|       t2().run()
	-----
	  Local variables:
	    local_1  = 1
	    local_1x = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ... [length:1002]
	-----
	Stack #T|00|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] run
	  =>|       t3()
	-----
	  Class variables (<t2 instance>):
	    self._local_2  = 2
	    self._password = <redacted>
	-----
	Stack #T|00|00|00|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t3
	  =>|     return 1/0
	-----
	  Local variables:
	    local_3 = DummyObj({'a': 1, 'b': 2})
	-----
	Stack #T|00|00|00-02 [<testsuite dir>/TEST_exception.py:<lineno>] t3
	  =>|     raise NestedException('Tier3')
	-----
	  Local variables:
	    local_3 = DummyObj({'a': 1, 'b': 2})
	-----
	Stack #T|00|00-02 [<testsuite dir>/TEST_exception.py:<lineno>] run
	  =>|       raise NestedException('Tier2')
	-----
	  Class variables (<t2 instance>):
	    self._local_2  = 2
	    self._password = <redacted>
	-----
	Stack #T|00-01 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	  =>|       raise NestedException('Tier1_2')
	-----
	  Local variables:
	    local_1  = 1
	    local_1x = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ... [length:1002]
	-----
	Stack #T-02 [<testsuite dir>/TEST_exception.py:<lineno>] t1
	  =>|     raise NestedException('Tier1_1')
	-----
	  Local variables:
	    local_1  = 1
	    local_1x = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ... [length:1002]
	-----

	>>> get_logger().setFormatter(GCFormatter(ex_context=0, ex_vars=-1, ex_fstack=0, ex_tree=0))
	>>> t0()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	-----

	>>> get_logger().setFormatter(GCFormatter(ex_context=0, ex_vars=-1, ex_fstack=1, ex_tree=0))
	>>> t0()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	File stack:
	T|01 <testsuite dir>/TEST_exception.py <lineno> (t0)
	T|00|00|01 <testsuite dir>/TEST_exception.py <lineno> (t1)
	T|00|00|00|01 <testsuite dir>/TEST_exception.py <lineno> (run)
	T|00|00|00|00|01 <testsuite dir>/TEST_exception.py <lineno> (t3)
	T|00|00|00|02 <testsuite dir>/TEST_exception.py <lineno> (t3)
	T|00|00|02 <testsuite dir>/TEST_exception.py <lineno> (run)
	T|00|01 <testsuite dir>/TEST_exception.py <lineno> (t1)
	T|02 <testsuite dir>/TEST_exception.py <lineno> (t1)
	-----

	>>> get_logger().setFormatter(GCFormatter(ex_context=0, ex_vars=-1, ex_fstack=0, ex_tree=1))
	>>> t0()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	NestedException: Tier1_1
	----------
	NestedException: Tier1_2
	----------
	NestedException: Tier2
	----------
	NestedException: Tier3
	----------
	ZeroDivisionError: <details>

	>>> try_catch(lambda: GCLogHandler(fn_candidates=[], mode='w'), 'Exception', 'Unable to find writeable debug log path')
	caught
	>>> try_catch(lambda: GCLogHandler(fn_candidates=[None], mode='w'), 'Exception', 'Unable to find writeable debug log path')
	caught

	>>> handler = GCLogHandler(fn_candidates=['debug.log'], mode='w')
	>>> handler.setFormatter(GCFormatter())
	>>> logging.getLogger().addHandler(handler)
	>>> stream = TestsuiteStream()
	>>> sys.stderr = stream
	>>> tA()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	SyntaxError: invalid syntax <details>
	-----
	In case this is caused by a bug, please send the log file:
	  '.../debug.log'
	to grid-control-dev@googlegroups.com
	>>> stream.write(open('debug.log').read())
	-----
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	SyntaxError: invalid syntax <details>
	             ('invalid syntax <details>

	>>> GCLogHandler.config_instances.append(create_config(config_dict={'test': {'key': 'value'}}))
	>>> GCLogHandler.config_instances.append(None)
	>>> tA()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	SyntaxError: invalid syntax <details>
	-----
	In case this is caused by a bug, please send the log file:
	  '.../debug.log'
	to grid-control-dev@googlegroups.com
	>>> stream.write(open('debug.log').read())
	----------------------------------------------------------------------
	Config instance 0
	======================================================================
	[global!]
	config id ?= unnamed
	plugin paths ?= <testsuite dir>
	workdir ?= <testsuite dir>/work
	workdir base ?= <testsuite dir>
	-----
	[test]
	key = value
	-----
	----------------------------------------------------------------------
	Config instance 1
	======================================================================
	-> unable to display configuration!
	-----
	**********************************************************************
	-----
	List of enums
	  source:2877|matcher:2878
	  executable:64623|command:64624
	-----
	**********************************************************************
	-----
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control...
	-----
	SyntaxError: invalid syntax <details>
	             ('invalid syntax <details>

	>>> import grid_control
	>>> del grid_control.__version__
	>>> tA()
	0000-00-00 00:00:00 - exception:CRITICAL - Exception occured in grid-control [unknown version]
	-----
	SyntaxError: invalid syntax <details>
	-----
	In case this is caused by a bug, please send the log file:
	  '.../debug.log'
	to grid-control-dev@googlegroups.com
	"""

run_test()
