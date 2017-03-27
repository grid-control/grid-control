#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, testfwk_remove_files, try_catch


class Test_Config:
	"""
	>>> testfwk_remove_files(['work.test_mod/*', 'work.test_mod'])
	>>> config1 = create_config('test_mod.conf')
	>>> print(config1.get('key1', 'k1_def'))
	k1_v1
	k1_v2
	>>> print(config1.get('key2', 'k2_def'))
	k2_def
	k2_v1
	>>> config1_a = config1.change_view(set_sections=['mod', 'section'])
	>>> tmp = config1_a.set('key3', 'k3_v2', '+=')
	>>> print(config1_a.get('key3', 'k3_def'))
	k3_v1
	k3_def
	k3_v2
	>>> config1.factory.freeze()
	>>> try_catch(lambda: config1_a.set('key  fail', 'value'), 'APIError', 'Config container is read-only')
	caught

	>>> config2 = create_config('test_mod.conf')
	>>> print(config2.get('key1', 'k1_def'))
	k1_v1
	k1_v2
	>>> print(config2.get('key2', 'k2_def'))
	k2_def
	k2_v1
	>>> config2_a = config2.change_view(set_sections=['mod', 'section'])
	>>> tmp = config2_a.set('key3', 'k3_v2', '+=')
	>>> print(config2_a.get('key3', 'k3_def'))
	k3_v1
	k3_def
	k3_v2
	>>> config1.factory.freeze()
	>>> #testfwk_remove_files(['work.test_mod/*', 'work.test_mod'])
	"""

run_test()
