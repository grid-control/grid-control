#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, logging
from testfwk import TestsuiteStream, create_config, run_test, try_catch
from python_compat import lmap


default_cfg = os.path.abspath('test_default.conf')
assert(os.path.exists(default_cfg))
os.environ['GC_CONFIG'] = default_cfg

class Test_Config:
	"""
	>>> try_catch(lambda: create_config('FAIL.conf'), 'ConfigError', 'Could not find file')
	caught
	>>> try_catch(lambda: create_config('FAIL1.conf'), 'ConfigError', 'Unable to interpolate')
	caught

	>>> config = create_config()
	>>> config.get_config_name()
	'unnamed'

	>>> config = create_config('test.conf', {'dict': {'test': 'works'}}, use_default_files=True)
	>>> config.get_config_name()
	'test'

	>>> config.get('key', 'default_key')
	'value'
	>>> try_catch(lambda: config.get('key', 'default'), 'APIError', 'Inconsistent default values')
	caught
	>>> config.get('key1', 'default_key1')
	'default_key1'
	>>> try_catch(lambda: config.get('key1', 'default'), 'APIError', 'Inconsistent default values')
	caught

	>>> config.write(TestsuiteStream(), print_default=False, print_unused=False)
	[global]
	plugin paths += <testsuite dir>/config
	-----
	[testuser]
	key = value
	-----

	>>> config.get('test')
	'works'

	>>> config.write(TestsuiteStream(), print_default=True, print_unused=False)
	[dict]
	test = works
	-----
	[global]
	plugin paths += <testsuite dir>/config
	-----
	[global!]
	config id ?= test
	key1 ?= default_key1
	plugin paths ?= <testsuite dir>/config
	workdir ?= <testsuite dir>/config/work.test
	workdir base ?= <testsuite dir>/config
	-----
	[testuser]
	key = value
	-----

	>>> config.get_dict('dict1') == ({'key3': 'value3', 'key2': 'value2\\ndefault2', 'key1': 'value1\\nvalue4', None: 'default1'}, ['key1', 'key2', 'key3'])
	True
	>>> config.get_dict('dict2', {'key1': 'val1'}) == ({'key1': 'val1'}, ['key1'])
	True
	>>> config.get_dict('dict3', {})
	({}, [])

	>>> config.get('key')
	'value'
	>>> config.change_view(set_sections=['TEST', 'TESTnick']).get('key')
	'valueX'
	>>> config.change_view(set_sections=['TEST', 'TESTnicky']).get('key')
	'value'

	>>> config.get('keydef', 'default')
	'valuedef'

	>>> try_catch(lambda: config.get('doesntexist'), 'ConfigError', 'does not exist')
	caught
	>>> try_catch(lambda: config.get('test1'), 'ConfigError', 'does not exist')
	caught
	>>> config.get('test2', 'default')
	'default'

	>>> config.get_int('zahl')
	123
	>>> try_catch(lambda: config.get_int('zahl1', 555), 'ConfigError', 'Unable to parse int')
	caught
	>>> config.get_int('zahl2', 985)
	985

	>>> config.change_view(set_sections=['TEST']).get_list('liste')
	['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

	>>> config.change_view(set_sections=['TEST', 'TESTnick']).get_list('liste')
	['x', 'y', 'z']

	>>> l0 = [1, 2]
	>>> config1 = config.change_view(set_sections=['TEST'])
	>>> config1.get_list('liste')
	['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
	>>> config1.get_list('liste1', l0)
	['1', '2']
	>>> config1.get_list('liste')
	['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
	>>> config1.get_list('liste1', l0)
	['1', '2']

	>>> config.get_list('liste1')
	['1', '2']
	>>> config.get_list('liste2', [1,2,3])
	['1', '2', '3']
	>>> config.get_list('liste3', []) == []
	True

	>>> config.get_bool('boolean')
	False
	>>> config.get_bool('boolean1', True)
	True

	>>> config.get_path('path1')
	'/bin/sh'
	>>> try_catch(lambda: config.get_path('path2'), 'ConfigError', 'Unable to parse path')
	caught
	>>> os.path.basename(config.get_path('path3'))
	'test.conf'
	>>> try_catch(lambda: config.get_path('path4', 'invalid'), 'ConfigError', 'Unable to parse path')
	caught

	>>> os.path.basename(config.get_path('path5', 'test.conf'))
	'test.conf'
	>>> lmap(os.path.basename, config.get_path_list('paths'))
	['test.conf', 'TEST_config.py', 'sh']
	>>> config.get_path_list('paths1', '')
	[]
	>>> config.get_path_list('paths1', [])
	[]
	>>> lmap(os.path.basename, config.get_path_list('paths2', ['test.conf', '/bin/sh']))
	['test.conf', 'sh']
	"""

def verbose_get(config, option, default):
	logging.getLogger('config').setLevel(logging.DEBUG3)
	result = config.get(option, default)
	logging.getLogger('config').setLevel(logging.INFO)
	return result

class Test_ConfigIncludes:
	"""
	>>> config = create_config('inc.conf', use_default_files=True)

	config = Config('inc.conf', {'global': {'workdir': '.'}}, configHostSpecific=False)

	>>> config.get('keyd', 'valued')
	'valued'

	>>> config.get('key')
	'value'
	>>> config.get('keyl1')
	'level1'
	>>> config.get('keyl2')
	'level2'

	Multiple config files can be specified
	>>> config.get('keyl1a')
	'level1a'

	Top level config file overrides settings
	>>> config.get('keyo')
	'value0'

	2nd level is not read ...
	>>> config.get('keyl1o')
	'level1'

	Options can append to lower level values
	>>> config.get('keyl1ap')
	'level2\\nlevel1'
	>>> config.get('keyl0ap')
	'level1\\nlevel0'
	>>> config.get('keydefap1')
	'level0'
	>>> config.get('keydefap2', 'DEFAULT')
	'DEFAULT\\nlevel0'
	>>> config.get_list('keydefap3')
	['a', 'b', 'c', 'd']
	>>> config.get_list('keydefap4', ['1', '2', '3'])
	['1', '2', '3', 'e', 'f', 'g', 'h']

	Last include file overrides previous settings on same level
	>>> config.get('same1', 'same')
	'level1'
	>>> config.get('same2', 'same')
	'level1a'
	>>> config.get('same3')
	'level1a'

	>>> config.get('samex')
	'level0'

	>>> config.get('settest1', 'blubb')
	'blubb'
	>>> config.set('settest1', 'foobar') is None
	False
	>>> config.get('settest1', 'blubb')
	'foobar'

	>>> config.get('settest2', 'blubb')
	'blubb'
	>>> config.set('settest2', 'foobar', '?=') is None
	False
	>>> verbose_get(config, 'settest2', 'blubb')
	0000-00-00 00:00:00 - config.inc:DEBUG2 - Config query from: 'verbose_get'
	0000-00-00 00:00:00 - config.inc:DEBUG1 - Config query for config option 'settest2'
	0000-00-00 00:00:00 - config.inc:DEBUG1 - Used config entries:
	0000-00-00 00:00:00 - config.inc:DEBUG1 -   [global!] settest2 ?= foobar (<string by ?> | ...)
	0000-00-00 00:00:00 - config.inc:DEBUG1 -   [global!] settest2 ?= blubb (<default> | ...)
	0000-00-00 00:00:00 - config.inc:DEBUG1 -   [global] settest2 ?= blubb (<default fallback> | None)
	Using dynamic value [global!] settest2 ?= foobar
	'foobar'

	>>> try_catch(lambda: config.set('settest3', 3), 'APIError', 'Unable to set string')
	caught
	"""


class Test_Interpolation:
	"""
	>>> cfg_global = {'key_g': 'global_g', 'key_h': 'global_h'}
	>>> cfg_test1 = {'key_t1': 'test1_1', 'key_t2': 'test1_2 %(key_g)s'}
	>>> cfg_test2 = {'key_t1': 'test2_1', 'key_t4': 'test2_2 %(key_t1)s'}
	>>> config = create_config(config_dict={'global': cfg_global, 'test1': cfg_test1, 'test2': cfg_test2}, use_default_files=True)
	>>> config.get('key_t2')
	'test1_2 global_g'
	>>> config.get('key_t4')
	'test2_2 test2_1'
	>>> config.write(TestsuiteStream(), print_unused=False)
	[global!]
	config id ?= unnamed
	plugin paths ?= <testsuite dir>/config
	workdir ?= <testsuite dir>/config/work
	workdir base ?= <testsuite dir>/config
	-----
	[test1]
	key_t2 = test1_2 global_g
	-----
	[test2]
	key_t4 = test2_2 test2_1
	-----

	>>> cfg_test2 = {'key_t1': 'test2_1', 'key_t2': 'test2_2 %(key_t1)s', 'key_t3': 'test2_3 %(key_t4)'}
	>>> try_catch(lambda: create_config(config_dict={'global': cfg_global, 'test1': cfg_test1, 'test2': cfg_test2}, use_default_files=True), 'ConfigError', 'Unable to interpolate value')
	caught

	>>> config = create_config('test_interpol.conf', use_default_files=True)
	>>> config.get('testx').split()
	['value_1', 'value_2']
	>>> config.get('testy').split()
	['value_global']
	>>> config.get('testz').split()
	['value_default']
	"""


class Test_ConfigScoped:
	"""
	>>> config = create_config('sections.conf')

	>>> config_nt = config.change_view(set_sections=['section named', 'section test'])
	>>> config_nt.get_option_list()
	['key2', 'key3']
	>>> config_nt.get('key1', 'fail')
	'fail'
	>>> config_nt.get('key2', 'fail')
	'valueX'
	>>> config_nt.get('key3', 'fail')
	'value3'

	>>> config_s = config.change_view(set_sections=['section'])
	>>> config_s.get_option_list()
	['key1', 'key2']
	>>> config_s.get('key1', 'fail')
	'value1'
	>>> config_s.get('key2', 'fail')
	'value2'
	>>> config_s.get('key3', 'fail')
	'fail'

	>>> config_n = config.change_view(set_sections=['section named'])
	>>> config_n.get('key1', 'fail')
	'fail'
	>>> config_n.get('key2', 'fail')
	'valueX'
	>>> config_n.get('key3', 'fail')
	'fail'

	>>> config_t = config.change_view(set_sections=['section test'])
	>>> config_t.get_option_list()
	['key3']
	>>> config_t.get('key1', 'fail')
	'fail'
	>>> config_t.get('key2', 'fail')
	'fail'
	>>> config_t.get('key3', 'fail')
	'value3'

	>>> config_st = config.change_view(set_sections=['section', 'section test'])
	>>> config_st.get_option_list()
	['key1', 'key2', 'key3']

	>>> config_st.get('key1', 'fail')
	'value1'
	>>> config_st.get('key2', 'fail')
	'value2'
	>>> config_st.get('key3', 'fail')
	'value3'

	>>> config_sn = config.change_view(set_sections=['section', 'section named'])
	>>> config_sn.get('key1', 'fail')
	'value1'
	>>> config_sn.get('key2', 'fail')
	'valueX'
	>>> config_sn.get('key3', 'fail')
	'fail'

	>>> config_snt = config.change_view(set_sections=['section', 'section named', 'section test'])
	>>> config_snt.get_option_list()
	['key1', 'key2', 'key3']
	>>> config_snt.get('key1', 'fail')
	'value1'
	>>> config_snt.get('key2', 'fail')
	'valueX'
	>>> config_snt.get('key3', 'fail')
	'value3'

	>>> config_nt = config.change_view(set_sections=['section named', 'section test'])
	>>> config_nt.get_option_list()
	['key1', 'key2', 'key3']
	>>> config_nt.get('key1')
	'fail'
	>>> config_nt.get('key2')
	'valueX'
	>>> config_nt.get('key3')
	'value3'

	>>> config = create_config('sections.conf')
	>>> config.change_view(set_sections=['section']).get('keyX', 'persistent1')
	'persistent1'
	>>> config.change_view(set_sections=['section named']).get('keyX', 'persistent2')
	'persistent2'
	>>> config.change_view(set_sections=['section named', 'section']).get('keyX')
	'persistent1'
	>>> config.change_view(set_sections=['section', 'section named']).get('keyX')
	'persistent2'

	>>> config = create_config(config_dict={'global': {'test': None}})
	>>> try_catch(lambda: create_config(config_dict={'global': {None: None}}), 'ConfigError', 'Unable to register')
	caught
	"""

run_test()
