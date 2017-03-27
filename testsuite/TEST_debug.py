#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import inspect, logging
from testfwk import TestsuiteStream, run_test, try_catch
from grid_control.logging_setup import GCStreamHandler
from grid_control_api import handle_debug_interrupt
from hpfwk import DebugInterface
from hpfwk.hpf_debug import _create_trace_fun
from python_compat import StringBuffer


class DummyConsole(object):
	def interact(*args):
		logging.critical('interacting')


def xyz(*args, **kwargs):
	GCStreamHandler.push_std_stream(TestsuiteStream(), TestsuiteStream())
	logging.critical('xyz')
	GCStreamHandler.pop_std_stream()


def interrupt(duration):
	logging.critical('interrupted %s' % duration)


class Test_Debug:
	"""
	>>> buffer = StringBuffer()
	>>> a = 123
	>>> di = DebugInterface(cur_frame=inspect.currentframe(), stream=buffer, interrupt_fun=interrupt)
	>>> denv = di.get_console_env_dict({'TEST': 'VALUE'})
	>>> denv['locals']()['a']
	123
	>>> try_catch(lambda: denv['resume'](1), 'SystemExit')
	0000-00-00 00:00:00 - root:CRITICAL - interrupted 1
	Exit with 0
	>>> console = di.get_console(denv)
	>>> console.push('1+2')
	3
	False
	>>> di.set_trace(fun_name='xyz', start_on_match=True, stop_on_match=True)
	>>> xyz()
	0000-00-00 00:00:00 - root:CRITICAL - interrupted 0
	0000-00-00 00:00:00 - root:CRITICAL - xyz
	>>> tmp = di.get_console_env_dict({})
	>>> ('locals' in tmp, 'resume' in tmp, 'trace' in tmp)
	(True, True, True)
	>>> buffer.flush()
	>>> 'xyz' in buffer.getvalue()
	True
	>>> di.set_trace(start_on_match=True, stop_on_match=True)
	0000-00-00 00:00:00 - root:CRITICAL - interrupted 0
	>>> buffer.flush()

	>>> output_stream = StringBuffer()
	>>> fun = _create_trace_fun(output_stream, interrupt, stop_on_match=True, start_on_match=True)
	>>> fun(inspect.currentframe(), 'call', None)
	0000-00-00 00:00:00 - root:CRITICAL - interrupted 0
	>>> output_stream.getvalue().strip().split()[-2:]
	['call', 'None']

	>>> di.get_console = lambda *args, **kwargs: DummyConsole()
	>>> di.start_console()
	0000-00-00 00:00:00 - root:CRITICAL - interacting

	>>> DebugInterface.start_console = xyz
	>>> handle_debug_interrupt()
	0000-00-00 00:00:00 - root:CRITICAL - xyz
	"""


run_test()
