#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testfwk').setup(__file__)
# - prolog marker
import re
from testfwk import run_test, try_catch
from grid_control.utils.process_base import LocalProcess, Process
from python_compat import sorted


long_timeout = 30

class TestProcess(Process):
	def _start(self):
		print('start')

class Test_Process:
	"""
	>>> try_catch(lambda: Process('sleep'), 'AbstractError', 'start is an abstract function')
	caught
	>>> p = TestProcess('sleep')
	start
	>>> try_catch(lambda: p.terminate(timeout=1), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: p.kill(), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: p.status(timeout=1), 'AbstractError', 'is an abstract function')
	caught
	"""

def should_be_less(value, ref):
	if value < ref:
		return True
	return value

class Test_LocalProcess:
	"""
	>>> try_catch(lambda: LocalProcess(''), 'ProcessError', 'Invalid executable')
	caught
	>>> try_catch(lambda: LocalProcess('abc'), 'ProcessError', 'does not exist')
	caught
	>>> try_catch(lambda: LocalProcess('test.conf'), 'ProcessError', 'is not executable')
	caught
	>>> try_catch(lambda: LocalProcess('sleep', '10').get_output(timeout=0.1, raise_errors=True), 'ProcessTimeout', 'Process is still running')
	caught
	>>> LocalProcess('sleep', '1').stdout.wait(timeout=0.1)
	False
	>>> p0 = LocalProcess('sleep', '1')
	>>> p0.stdout.wait(timeout=long_timeout)
	True
	>>> should_be_less(p0.get_runtime(), 10)
	True
	>>> re.sub('cmd = .*sleep,', 'cmd = <sleep cmd>,', repr(p0))
	"LocalProcess(cmd = <sleep cmd>, args = ['1'], status = 0, stdin log = '', stdout log = '', stderr log = '')"

	>>> sorted(LocalProcess('find', '.', '-name', 'test*.py').get_output(timeout=long_timeout).split())
	['./datasets/testResync.py', './parameters/testINC.py', './testDS.py', './testfwk.py', './testfwk_datasets.py']

	>>> p1 = LocalProcess('find', '.', '-name', 'test*.py')
	>>> sorted(p1.get_output(timeout=long_timeout).splitlines())
	['./datasets/testResync.py', './parameters/testINC.py', './testDS.py', './testfwk.py', './testfwk_datasets.py']

	>>> p2 = LocalProcess('python', '-Ec', 'cs=__import__("code").InteractiveConsole();tmp=cs.raw_input;cs.raw_input=lambda*args:tmp();cs.interact("")')
	>>> p2.stdin.write('1+5\\n')
	>>> p2.stdout.wait(timeout=long_timeout)
	True
	>>> p2.stdin.close()
	>>> p2.status(timeout=long_timeout)
	0
	>>> p2.get_output(timeout=long_timeout)
	'6\\n'
	>>> p2.terminate(timeout=1)
	0
	>>> p2.stdout
	ProcessReadStream(buffer = '6\\n')

	>>> p3 = LocalProcess('python', '-Ec', 'cs=__import__("code").InteractiveConsole();tmp=cs.raw_input;cs.raw_input=lambda*args:tmp();cs.interact("")')
	>>> p3.stdin.write('1+5\\n')
	>>> p3.stdin.write('6*7\\n')
	>>> p3.stdout.wait(timeout=long_timeout)
	True
	>>> p3.stdin.close()
	>>> p3.get_output(timeout=long_timeout)
	'6\\n42\\n'

	>>> p4 = LocalProcess('cat')
	>>> try_catch(lambda: p4.status_raise(timeout=0.5), 'ProcessTimeout', 'Process is still running')
	caught

	>>> p5 = LocalProcess('/usr/bin/find', '.', '-name', 'test*.py', logging=False)
	>>> for x in sorted(p5.stdout.iter(timeout=long_timeout)):
	...   print(x.strip())
	./datasets/testResync.py
	./parameters/testINC.py
	./testDS.py
	./testfwk.py
	./testfwk_datasets.py
	>>> p5.status(timeout=1)
	0
	>>> p5
	LocalProcess(cmd = /usr/bin/find, args = ['.', '-name', 'test*.py'], status = 0)
	>>> p5.stdout.read_log()
	''

	# bin/process: Hello[1s]World<br>[2s]Test[inf]
	>>> p6 = LocalProcess('bin/process')
	>>> try_catch(lambda: p6.stdout.read_cond(timeout=0.1, cond=lambda buffer: 'Hello World' in buffer), 'ProcessTimeout', 'did not fulfill condition')
	caught
	>>> p6.status(timeout=1)
	>>> p6.restart(timeout=10) in ['SIGKILL', 'SIGTERM']
	True
	>>> p6.get_runtime() < 10
	True
	>>> p6.status(timeout=0)

	>>> 'Hello World' in p6.stdout.read_cond(timeout=long_timeout, cond=lambda buffer: 'Hello World' in buffer)
	True
	>>> p6
	LocalProcess(cmd = bin/process, args = [], status = None, stdin log = '', stdout log = 'Hello World\\n', stderr log = '')
	>>> try_catch(lambda: list(p6.stdout.iter(timeout=1)), 'ProcessTimeout', 'did not yield more lines')
	caught
	>>> p6.status(timeout=5, terminate=True) in ['SIGKILL', 'SIGTERM']  # delay kill so 'TEST...' is in the output buffer
	True
	>>> p6.get_runtime() > 1
	True
	>>> list(p6.stdout.iter(timeout=1))
	['Test...']
	>>> p6
	LocalProcess(cmd = bin/process, args = [], status = SIGKILL, stdin log = '', stdout log = 'Hello World\\nTest...', stderr log = '')
	>>> p6.get_runtime() > 1
	True
	"""

run_test()
