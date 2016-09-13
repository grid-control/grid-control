#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), ''))
__import__('testFwk').setup(__file__)
# - prolog marker
import os, time
from testFwk import remove_files_testsuite, run_test, setPath, str_dict_testsuite, try_catch
from grid_control import utils
from grid_control.utils.activity import Activity
from grid_control.utils.cmd_options import Options
from grid_control.utils.data_structures import UniqueList, make_enum
from grid_control.utils.file_objects import SafeFile, ZipFile
from grid_control.utils.parsing import parse_dict, parse_time, remove_unicode, split_advanced, split_brackets, split_quotes
from grid_control.utils.table import ColumnTable, JSONTable, ParseableTable, RowTable
from grid_control.utils.thread_tools import GCLock, GCQueue, GCThreadPool, start_daemon, tchain
from python_compat import ichain, imap, irange, lchain, lrange, next, sorted


def setup_parser():
	parser = Options()
	parser.add_text(None, 'o', 'option', default = 'default')
	parser.add_flag(None, ('a', 'A'), ('alpha', 'no-alpha'), default = True)
	parser.add_flag(None, ('b', 'B'), ('beta', 'no-beta'), default = False)
	parser.add_flag(None, ('g', 'G'), ('gamma', 'no-gamma'), default = False)
	parser.add_fset(None, 'f', 'flags', 'Setup not alpha and beta: %s', '--no-alpha --beta')
	parser.testsuite_default_dict = parser.parse()[2]
	return parser

parser = setup_parser()

def display_opts(parse_result, short = False):
	(opt, args, opt_dict) = parse_result
	if short:
		for key, default in parser.testsuite_default_dict.items():
			if opt_dict.get(key) == default:
				opt_dict.pop(key)
	print('%r %s' % (args, str_dict_testsuite(opt_dict)))

class Test_Options:
	"""
	>>> parser = setup_parser()
	>>> display_opts(parser.parse([]))
	[] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'default'}

	>>> display_opts(parser.parse(['--alpha']))
	[] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['--no-alpha']))
	[] {'alpha': 'False', 'beta': 'False', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['--no-alpha', '--alpha']))
	[] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['-f']))
	[] {'alpha': 'False', 'beta': 'True', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['-f', '--alpha', '--gamma']))
	[] {'alpha': 'True', 'beta': 'True', 'gamma': 'True', 'option': 'default'}

	>>> display_opts(parser.parse(['hello']))
	['hello'] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['hello', 'world']))
	['hello', 'world'] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['hello world']))
	['hello world'] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'default'}
	>>> display_opts(parser.parse(['world', '-o', 'hello']))
	['world'] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'hello'}
	>>> display_opts(parser.parse(['-o', 'hello', 'world']))
	['world'] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'hello'}
	>>> display_opts(parser.parse(['-o', 'hello world']))
	[] {'alpha': 'True', 'beta': 'False', 'gamma': 'False', 'option': 'hello world'}

	>>> display_opts(parser.parse(['arg1', 'arg2', 'arg3']), short = True)
	['arg1', 'arg2', 'arg3'] {}
	>>> display_opts(parser.parse(['arg1', 'arg2', 'arg3'], arg_keys = ['key']), short = True)
	['arg1', 'arg2', 'arg3'] {'key': 'arg1 arg2 arg3'}
	>>> display_opts(parser.parse(['arg1', 'arg2', 'arg3'], arg_keys = ['key1', 'key2']), short = True)
	['arg1', 'arg2', 'arg3'] {'key1': 'arg1', 'key2': 'arg2 arg3'}
	>>> display_opts(parser.parse(['arg1', '-o', 'arg2', 'arg3'], arg_keys = ['key1', 'key2']), short = True)
	['arg1', 'arg3'] {'key1': 'arg1', 'key2': 'arg3', 'option': 'arg2'}
	"""

def display_activity(root):
	for entry in root.get_children():
		print('%s %s' % ('-' * entry.depth, entry.get_message(truncate = 30, last = 10)))

class Test_Activity:
	"""
	>>> try_catch(lambda: Activity(parent = 'nobody'), 'APIError', 'Invalid parent')
	caught

	>>> Activity.root.finish()
	>>> del Activity.root
	>>> Activity.root = Activity('Testing', name = 'root')
	>>> Activity.root
	Activity(name: 'root', msg: 'Testing', lvl: 20, depth: 0, parent: None)

	>>> a = Activity('Working on A', name = 'a')
	>>> a1 = Activity('Working on A1')
	>>> a2 = Activity('Working on A2', parent = 'a')
	>>> a2x = Activity('Working on A2' + 'x'*100)
	>>> display_activity(Activity.root)
	- Working on A...
	-- Working on A1...
	-- Working on A2...
	--- Working on A2xxxx...xxxxxxx...

	>>> a2.finish()
	>>> display_activity(Activity.root)
	- Working on A...
	-- Working on A1...
	"""

class Test_SafeFile:
	"""
	>>> f = SafeFile('test', 'w')
	>>> f.write('hallo')
	>>> f.close()
	>>> f
	SafeFile(fn = 'test', mode = 'w', keep_old = False, handle = None)
	>>> os.path.exists('test')
	True
	>>> remove_files_testsuite(['test'])
	"""

class Test_Tables:
	"""
	>>> head = [('k1', 'Key1'), ('k2', 'Key2')]
	>>> data = [{'k1': 'Hello', 'k2': 'World'}, '-', {'k1': 'Test'}, '=', {'k2': 'Value'}]

	>>> tmp = JSONTable(head, data)
	{"data": [{"k1": "Hello", "k2": "World"}, "-", {"k1": "Test"}, "=", {"k2": "Value"}], "header": [["k1", "Key1"], ["k2", "Key2"]]}

	>>> tmp = ParseableTable(head, data)
	Key1|Key2
	Hello|World
	Test|
	|Value

	>>> tmp = RowTable(head, data)
	-----
	  Key1 | Hello
	  Key2 | World
	=======+===============================
	  Key1 | Test
	  Key2 | 
	=======+===============================
	  Key1 | 
	  Key2 | Value
	-----

	>>> tmp = ColumnTable(head, data)
	-----
	  Key1 |  Key2 
	=======+=======
	 Hello | World 
	-------+-------
	  Test |       
	=======+=======
	       | Value 
	-----
	"""

class Test_DataStructures:
	"""
	>>> try_catch(lambda: make_enum(['hallo', 'Hallo']), 'APIError', 'Invalid enum definition')
	caught

	>>> enum = make_enum(['A', 'B'])
	>>> enum.enum2str(enum.A)
	'A'
	>>> enum.str2enum('A') == enum.A
	True
	>>> enum.str2enum('None') is None
	True

	>>> tmp = UniqueList([1,2,1,2,3])
	>>> tmp
	<1, 2, 3>
	"""

def fail_hard(idx):
	time.sleep(idx)
	raise Exception('FAIL %d!' % idx)

def work_long():
	time.sleep(2)

class Test_Threads:
	"""
	>>> tp = GCThreadPool()
	>>> tp.start_daemon('test1', fail_hard, 1)
	>>> tp.start_daemon('test2', fail_hard, 2)
	>>> tmp = tp.wait_and_drop()
	0000-00-00 00:00:00 - thread_pool:ERROR - Exception in thread 'test1': Exception: FAIL 1!
	0000-00-00 00:00:00 - thread_pool:ERROR - Exception in thread 'test2': Exception: FAIL 2!

	>>> tmp
	True

	>>> tp = GCThreadPool()
	>>> tp.start_daemon('worker1', work_long)
	>>> tp.start_daemon('worker2', work_long)
	>>> tp.wait_and_drop(timeout = 0)
	False

	>>> q = GCQueue()
	>>> q.put(1)
	>>> q.put(2)
	>>> q
	GCQueue([1, 2])
	>>> q.get(timeout = 0)
	1
	>>> q.get(timeout = 0)
	2
	>>> try_catch(lambda: q.get(timeout = 0), 'IndexError', 'Queue is empty!')
	caught

	>>> l = GCLock()
	>>> l.acquire(timeout = None)
	True
	>>> l.acquire(timeout = 0)
	False
	>>> l.release()
	>>> l.acquire(timeout = 0)
	True
	>>> try_catch(lambda: l.acquire(timeout = 0.1), 'TimeoutException')
	caught
	"""

def src_fail():
	yield 1
	yield 2
	raise Exception('FAIL')

def src_slow(values):
	for item in values:
		yield item
		time.sleep(1)

class Test_Chain:
	"""
	>>> list(ichain([lrange(0,5), lrange(5,10)]))
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
	>>> list(lchain([lrange(0,5), lrange(5,10)]))
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
	>>> sorted(tchain([lrange(0,5), lrange(5,10)]))
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
	>>> sorted(tchain([lrange(0,5), lrange(5,10)], timeout = 1))
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
	>>> sorted(tchain([src_slow(lrange(0, 5)), lrange(5,10)], timeout = 1.5))
	[0, 1, 5, 6, 7, 8, 9]
	>>> aborted = next(tchain([src_slow(lrange(0, 5)), lrange(5,10)], timeout = 1.5))
	>>> try_catch(lambda: list(tchain([src_fail(), lrange(5,10)])), 'NestedException', 'Caught exception during threaded chain')
	caught
	"""

class Test_FileObjects:
	"""
	>>> fw = ZipFile('test.gz', 'w')
	>>> fw.write('Hallo Welt')
	>>> fw.close()
	>>> fr = ZipFile('test.gz', 'r')
	>>> fr.readline()
	'Hallo Welt'
	>>> remove_files_testsuite(['test.gz'])
	"""

qs1 = 'a "" "c" \'d b\' \'d "x" b\''
qr1 = ['a', ' ', '""', ' ', '"c"', ' ', "'d b'", ' ', '\'d "x" b\'']
qs2 = 'a b c "d e" f g h \'asd "b"\''
qr2 = ['a', ' ', 'b', ' ', 'c', ' ', '"d e"', ' ', 'f', ' ', 'g', ' ', 'h', ' ', '\'asd "b"\'']
qs3 = '"\'"\'\'"'

class Test_Utils:
	"""
	>>> remove_unicode({'test': [1, 2, ('A', 'B', 3)]})
	{'test': [1, 2, ('A', 'B', 3)]}

	>>> setPath('bin')
	>>> pr = utils.ping_host('8.8.8.8')
	>>> (pr is None) or (pr < 5)
	True

	>>> parse_dict(' 1\\n1 => 5\\n2=>4', int) ==\
	({'1': 5, '2': 4, None: 1}, ['1', '2'])
	True
	>>> parse_dict(' 1\\n1 => 5\\n2=>4', parser_key=int) ==\
	({1: '5', 2: '4', None: '1'}, [1, 2])
	True
	>>> parse_dict(' 1 2 3\\n 4 5 6\\n1 => 5\\n2=>3 4\\n63 1') ==\
	({'1': '5', '2': '3 4\\n63 1', None: '1 2 3\\n4 5 6'}, ['1', '2'])
	True

	>>> parse_time('')
	-1
	>>> parse_time(None)
	-1
	>>> parse_time('1:00')
	3600
	>>> parse_time('1:00:00')
	3600
	>>> try_catch(lambda: parse_time('1:00:00:00'), 'Exception', 'Invalid time format')
	caught

	>>> list(split_quotes(qs1)) == qr1
	True
	>>> list(split_quotes(qs2)) == qr2
	True
	>>> try_catch(lambda: list(split_quotes(qs3)), 'Exception', 'Unclosed quotes')
	caught

	>>> list(split_brackets('a[-1]x'))
	['a', '[-1]', 'x']
	>>> list(split_brackets('a[(-1)+(4)+[2]]x'))
	['a', '[(-1)+(4)+[2]]', 'x']
	>>> list(split_brackets('()[]{}([])[({})]'))
	['()', '[]', '{}', '([])', '[({})]']
	>>> try_catch(lambda: list(split_brackets('a[(])')), 'Exception', 'does not match bracket')
	caught
	>>> try_catch(lambda: list(split_brackets('a][b')), 'Exception', 'is without opening bracket')
	caught
	>>> try_catch(lambda: list(split_brackets('a([()')), 'Exception', 'Unclosed brackets')
	caught

	>>> list(split_advanced('', lambda t: t == ',', lambda t: False))
	[]
	>>> list(split_advanced('a', lambda t: t == ',', lambda t: False))
	['a']
	>>> list(split_advanced(' a ', lambda t: t == ',', lambda t: False))
	[' a ']
	>>> list(split_advanced(' a , b ', lambda t: t == ',', lambda t: False))
	[' a ', ' b ']
	>>> list(split_advanced(' (a) ', lambda t: t == ',', lambda t: False))
	[' (a) ']
	>>> list(split_advanced(' ( a ) ', lambda t: t == ',', lambda t: False))
	[' ( a ) ']
	>>> list(split_advanced(' ( a , b ) ', lambda t: t == ',', lambda t: False))
	[' ( a , b ) ']
	>>> list(split_advanced(' ( a , b ) ', lambda t: t == '|', lambda t: False))
	[' ( a , b ) ']

	>>> list(split_advanced('', lambda t: t == ' ', lambda t: True))
	[]
	>>> list(split_advanced('a', lambda t: t == ' ', lambda t: True))
	['a']
	>>> list(split_advanced('()', lambda t: t == ' ', lambda t: True))
	['()']
	>>> list(split_advanced('(a)', lambda t: t == ' ', lambda t: True))
	['(a)']
	>>> list(split_advanced('(a b (c))', lambda t: t == ' ', lambda t: True))
	['(a b (c))']
	>>> list(split_advanced('(a b (c)) ((d))', lambda t: t == ' ', lambda t: True))
	['(a b (c))', ' ', '((d))']
	>>> list(split_advanced(' (c d )  (f g) ()', lambda t: t == ' ', lambda t: True))
	['', ' ', '(c d )', ' ', '', ' ', '(f g)', ' ', '()']
	>>> list(split_advanced('ac b (c d ) e (f g) h (i) (j k) () h', lambda t: t == ' ', lambda t: True))
	['ac', ' ', 'b', ' ', '(c d )', ' ', 'e', ' ', '(f g)', ' ', 'h', ' ', '(i)', ' ', '(j k)', ' ', '()', ' ', 'h']
	>>> list(split_advanced('ac b (c d ) e (f g) h (i) (j k) () h', lambda t: t == ' ', lambda t: False))
	['ac', 'b', '(c d )', 'e', '(f g)', 'h', '(i)', '(j k)', '()', 'h']

	>>> acc5 = lambda x, buf: len(buf) == 5
	>>> list(utils.accumulate([], [], acc5, add_fun = lambda x, y: x + [y]))
	[]
	>>> list(utils.accumulate(lrange(1), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0]]
	>>> list(utils.accumulate(lrange(4), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0, 1, 2, 3]]
	>>> list(utils.accumulate(lrange(5), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0, 1, 2, 3, 4]]
	>>> list(utils.accumulate(lrange(6), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0, 1, 2, 3, 4], [5]]
	>>> list(utils.accumulate(lrange(24), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 13, 14], [15, 16, 17, 18, 19], [20, 21, 22, 23]]
	>>> list(utils.accumulate(lrange(25), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 13, 14], [15, 16, 17, 18, 19], [20, 21, 22, 23, 24]]
	>>> list(utils.accumulate(lrange(26), [], acc5, add_fun = lambda x, y: x + [y]))
	[[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11, 12, 13, 14], [15, 16, 17, 18, 19], [20, 21, 22, 23, 24], [25]]

	>>> utils.wrap_list(imap(str, irange(20)), 20)
	'0, 1, 2, 3, 4, 5, 6,\\n7, 8, 9, 10, 11, 12,\\n13, 14, 15, 16, 17,\\n18, 19'

	>>> utils.split_opt('abc # def:ghi', ['#', ':'])
	('abc', 'def', 'ghi')
	>>> utils.split_opt('abcghi#def', ['#', ':'])
	('abcghi', 'def', '')
	>>> utils.split_opt('abc: def#test :ghi', [':', '#', ':'])
	('abc', 'def', 'test', 'ghi')
	>>> utils.split_opt('abc', [':', ':'])
	('abc', '', '')

	>>> try_catch(lambda: utils.ensure_dir_exists('/root/.grid-control'), 'PathError', 'Problem creating')
	caught

	>>> utils.Result(key = 'value')
	Result(key = 'value')
	"""

run_test()
