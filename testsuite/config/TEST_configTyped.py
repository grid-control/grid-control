#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import TestsuiteStream, create_config, run_test, str_dict_testsuite, try_catch
from grid_control.gc_plugin import NamedPlugin
from python_compat import lmap, unspecified


def get_dictDisplay(value):
	if None in value[0]:
		return str_dict_testsuite(value[0], value[1] + [None])
	return str_dict_testsuite(value[0], value[1])

def createMiniConfig(value):
	return create_config(config_dict={'TEST': {'key': value}})

class TSBase(NamedPlugin):
	config_tag_name = 'blubb'
	def __init__(self, config, name, msg):
		NamedPlugin.__init__(self, config, name)
		self._msg = msg

	def __repr__(self):
		return '%s(name = %s, msg = %s)' % (self.__class__.__name__, self._name, self._msg)

class TSMulti(TSBase):
	def __init__(self, config, name, cls_list, msg):
		TSBase.__init__(self, config, name, msg)
		self._cls_list = cls_list

	def __repr__(self):
		return '%s(name = %s, msg = %s, %r)' % (self.__class__.__name__, self._name, self._msg, self._cls_list)

class TSChild(TSBase):
	pass

class TSChild1(TSBase):
	pass

class TSChildA(TSChild1):
	pass

class Test_ConfigBase:
	r"""
	>>> config = createMiniConfig('value')
	>>> config.get('key', 'default')
	'value'
	>>> config.get('key_def', 'default')
	'default'
	>>> config.set('key_set', 'eulav') is not None
	True
	>>> config.get('key_set', 'fail')
	'eulav'

	== get_int ==
	>>> createMiniConfig('1').get_int('key', 0)
	1
	>>> createMiniConfig('+1').get_int('key', 0)
	1
	>>> createMiniConfig('-1').get_int('key', 0)
	-1
	>>> try_catch(lambda: createMiniConfig('0').get_int('key_def', None), 'APIError', 'Unable to get string representation of default object: None')
	caught
	>>> try_catch(lambda: createMiniConfig('').get_int('key', 0), 'ConfigError', 'Unable to parse int')
	caught

	== get_bool ==
	>>> createMiniConfig('1').get_bool('key', False)
	True
	>>> createMiniConfig('true').get_bool('key', False)
	True
	>>> createMiniConfig('0').get_bool('key', True)
	False
	>>> createMiniConfig('false').get_bool('key', True)
	False
	>>> createMiniConfig('false').get_bool('key_def1', True)
	True
	>>> createMiniConfig('false').get_bool('key_def2', False)
	False
	>>> try_catch(lambda: createMiniConfig('false').get_bool('key_def3', None), 'APIError', 'Unable to get string representation of default object')
	caught
	>>> try_catch(lambda: createMiniConfig('').get_bool('key', False), 'ConfigError', 'Unable to parse bool')
	caught

	== get_time ==
	>>> createMiniConfig('1').get_time('key', 0)
	3600
	>>> createMiniConfig('1:01').get_time('key', 0)
	3660
	>>> createMiniConfig('1:01:01').get_time('key', 0)
	3661
	>>> createMiniConfig('').get_time('key', 0)
	-1
	>>> createMiniConfig('').get_time('key', -1)
	-1
	>>> createMiniConfig('').get_time('key', -100)
	-1
	>>> createMiniConfig('').get_time('key_def', 0)
	0
	>>> createMiniConfig('').get_time('key_def', -1)
	-1
	>>> createMiniConfig('').get_time('key_def', -100)
	-1
	>>> createMiniConfig('false').get_time('key_def', None)
	-1
	>>> try_catch(lambda: createMiniConfig('abc').get_time('key', False), 'ConfigError', 'Unable to parse time')
	caught

	== get_list ==
	>>> createMiniConfig('').get_list('key', ['X'])
	[]
	>>> createMiniConfig('123').get_list('key', ['X'])
	['123']
	>>> createMiniConfig('1 2 3').get_list('key', ['X'])
	['1', '2', '3']
	>>> createMiniConfig('').get_list('key_def', [])
	[]
	>>> createMiniConfig('').get_list('key_def', ['1'])
	['1']
	>>> createMiniConfig('').get_list('key_def', [1, 2])
	['1', '2']
	>>> try_catch(lambda: createMiniConfig('false').get_list('key_def', None), 'APIError', 'Unable to get string representation of default object')
	caught

	== get_dict ==
	>>> get_dictDisplay(createMiniConfig('default').get_dict('key', {'K': 'V'}))
	"{None: 'default'}"
	>>> get_dictDisplay(createMiniConfig('A => 1').get_dict('key', {'K': 'V'}))
	"{'A': '1'}"
	>>> get_dictDisplay(createMiniConfig('A => 1\nB => 2').get_dict('key', {'K': 'V'}))
	"{'A': '1', 'B': '2'}"
	>>> get_dictDisplay(createMiniConfig('A => 1\nB => 2').get_dict('key', {'K': 5}, parser=int))
	"{'A': 1, 'B': 2}"
	>>> get_dictDisplay(createMiniConfig('=> 1\nB => 2').get_dict('key', {'K': 'V'}))
	"{'B': '2', None: '1'}"
	>>> get_dictDisplay(createMiniConfig('=> 1B => 2').get_dict('key', {'K': 'V'}))
	"{None: '1B => 2'}"
	>>> get_dictDisplay(createMiniConfig('default\nA => 1\nB => 2').get_dict('key', {'K': 'V'}))
	"{'A': '1', 'B': '2', None: 'default'}"
	>>> get_dictDisplay(createMiniConfig('A => 1\nB => 2\ndefault').get_dict('key', {'K': 'V'}))
	"{'A': '1', 'B': '2\\ndefault'}"
	>>> get_dictDisplay(createMiniConfig('').get_dict('key_def', {'K': 'V'}))
	"{'K': 'V'}"
	>>> get_dictDisplay(createMiniConfig('').get_dict('key_def', {}))
	'{}'
	>>> try_catch(lambda: get_dictDisplay(createMiniConfig('false').get_dict('key_def', None)), 'APIError', 'Unable to convert default object: None')
	caught

	== get_choice ==
	>>> try_catch(lambda: createMiniConfig('').get_choice('key', ['A', 'B', 'C'], 'A'), 'ConfigError', 'Invalid choice')
	caught
	>>> try_catch(lambda: createMiniConfig('D').get_choice('key', ['A', 'B', 'C'], 'A'), 'ConfigError', 'Invalid choice')
	caught
	>>> try_catch(lambda: createMiniConfig('A').get_choice('key', ['A', 'B', 'C'], 'D'), 'APIError', 'Invalid default choice')
	caught
	>>> createMiniConfig('A').get_choice('key', ['A', 'B', 'C'], 'A')
	'A'

	== get_path ==
	>>> showPath = lambda x: str.join('/', x.split('/')[-2:])
	>>> showPath(createMiniConfig('test.conf').get_path('key', 'def'))
	'config/test.conf'
	>>> showPath(createMiniConfig('test.conf').get_path('key', 'def', must_exist=False))
	'config/test.conf'
	>>> try_catch(lambda: showPath(createMiniConfig('file').get_path('key', 'def')), 'ConfigError', 'Unable to parse path')
	caught
	>>> showPath(createMiniConfig('file').get_path('key', 'def', must_exist=False))
	'file'
	>>> showPath(createMiniConfig('').get_path('key', 'def'))
	''
	>>> showPath(createMiniConfig('').get_path('key', 'def', must_exist=False))
	''
	>>> showPath(createMiniConfig('').get_path('key_def', ''))
	''
	>>> showPath(createMiniConfig('').get_path('key_def', '', must_exist=False))
	''
	>>> try_catch(lambda: showPath(createMiniConfig('').get_path('key_def', 'def')), 'ConfigError', 'Unable to parse path')
	caught
	>>> showPath(createMiniConfig('').get_path('key_def', 'def', must_exist=False))
	'def'
	>>> showPath(createMiniConfig('').get_path('key_def', 'test.conf'))
	'config/test.conf'
	>>> showPath(createMiniConfig('').get_path('key_def', 'test.conf', must_exist=False))
	'config/test.conf'
	>>> try_catch(lambda: showPath(createMiniConfig('false').get_path('key_def', None)), 'APIError', 'Unable to get string representation of default object: None')
	caught

	== get_path_list ==
	>>> lmap(showPath, createMiniConfig('test.conf\ninc.conf').get_path_list('key', ['def']))
	['config/test.conf', 'config/inc.conf']
	>>> lmap(showPath, createMiniConfig('test.conf\ninc.conf').get_path_list('key', ['def'], must_exist=False))
	['config/test.conf', 'config/inc.conf']
	>>> try_catch(lambda: lmap(showPath, createMiniConfig('test.conf\nincx.conf').get_path_list('key', ['def'])), 'ConfigError', 'Unable to parse paths')
	caught
	>>> lmap(showPath, createMiniConfig('test.conf\nincx.conf').get_path_list('key', ['def'], must_exist=False))
	['config/test.conf', 'incx.conf']
	>>> lmap(showPath, createMiniConfig('').get_path_list('key', ['def']))
	[]
	>>> lmap(showPath, createMiniConfig('').get_path_list('key', ['def'], must_exist=False))
	[]
	>>> try_catch(lambda: lmap(showPath, createMiniConfig('').get_path_list('key_def', ['def'])), 'ConfigError', 'Unable to parse paths')
	caught
	>>> lmap(showPath, createMiniConfig('').get_path_list('key_def', ['def'], must_exist=False))
	['def']

	== get_plugin ==
	>>> try_catch(lambda: createMiniConfig('TSChildA TSChild1').get_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',)), 'ConfigError', 'This option only allows to specify a single plugin!')
	caught
	>>> createMiniConfig('TSChild').get_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChild(name = TSChild, msg = Hello World)
	>>> createMiniConfig('TSChild1').get_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChild1(name = TSChild1, msg = Hello World)
	>>> createMiniConfig('TSChildA').get_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChildA(name = TSChildA, msg = Hello World)
	>>> createMiniConfig('TSChildA:Testobject').get_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChildA(name = Testobject, msg = Hello World)
	>>> createMiniConfig('').get_plugin('key_def', 'TSChild', cls=TSBase, pargs=('Hello World',))
	TSChild(name = TSChild, msg = Hello World)
	>>> createMiniConfig('').get_plugin('key', require_plugin=False, cls=TSBase, pargs=('Hello World',)) is None
	True

	== get_composited_plugin ==
	>>> try_catch(lambda: createMiniConfig('').get_composited_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',)), 'ConfigError', 'This option requires to specify a valid plugin')
	caught
	>>> createMiniConfig('TSChild').get_composited_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChild(name = TSChild, msg = Hello World)
	>>> createMiniConfig('TSChildA').get_composited_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChildA(name = TSChildA, msg = Hello World)
	>>> createMiniConfig('TSChildA:Testobject').get_composited_plugin('key', unspecified, cls=TSBase, pargs=('Hello World',))
	TSChildA(name = Testobject, msg = Hello World)
	>>> createMiniConfig('TSChildA:Testobject TSChild1').get_composited_plugin('key', unspecified, cls=TSBase, default_compositor='TSMulti', pargs=('Hello World',))
	TSMulti(name = TSMulti, msg = Hello World, [TSChildA(name = Testobject, msg = Hello World), TSChild1(name = TSChild1, msg = Hello World)])
	>>> createMiniConfig('').get_composited_plugin('key_def', 'TSChild TSChild1:HEY TSChildA', cls=TSBase, default_compositor='TSMulti', pargs=('Hello World',))
	TSMulti(name = TSMulti, msg = Hello World, [TSChild(name = TSChild, msg = Hello World), TSChild1(name = HEY, msg = Hello World), TSChildA(name = TSChildA, msg = Hello World)])

	== get_event ==
	>>> evt = createMiniConfig('').get_event('init')
	>>> evt.is_set()
	False
	>>> evt.set()
	True
	>>> evt.is_set()
	True
	>>> evt.clear()
	False
	>>> evt.is_set()
	False
	"""

class Test_ConfigSet:
	"""
	>>> config = create_config()
	>>> tmp = config.set_int('key_int', 12)
	>>> tmp.source = '<test>'
	>>> tmp
	ConfigEntry(accessed = False, option = 'key_int', opttype = '=', order = 1, section = 'global!', source = '<test>', used = False, value = '12')
	>>> config.set_time('key_time', 100000) is not None
	True
	>>> config.set_choice('key_choice', 'A') is not None
	True
	>>> config.write(TestsuiteStream(), print_default=False)
	[global!]
	key_choice = A
	key_int = 12
	key_time = 27:46:40
	-----
	"""

run_test()
