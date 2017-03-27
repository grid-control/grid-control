#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test
from grid_control.config.cfiller_base import FileConfigFiller
from grid_control.config.config_entry import ConfigContainer, ConfigEntry


def display(container, key):
	def _select_all(x):
		return True
	entry = ConfigEntry.combine_entries(container.iter_config_entries(key.lower(), _select_all))
	if entry:
		return entry.value.replace('\n', ';')


class Test_ConfigContainer:
	"""
	>>> container = ConfigContainer('test')
	>>> tmp = FileConfigFiller(config_fn_list = ['test_c0.conf'])
	>>> tmp.fill(container)
	>>> display(container, 'key_l0a_norm')
	'1;2'
	>>> display(container, 'key_l0a_add')
	'3;4'
	>>> display(container, 'key_l0a_cond')
	'5;6'
	>>> display(container, 'key_l0a_force')
	'7;8'
	>>> display(container, 'key_l0a_pre')
	'9;0'
	>>> display(container, 'key_l0a_nset')

	>>> display(container, 'key_l0a_def_norm')
	'1;2'
	>>> display(container, 'key_l0a_def_add')
	'3;4'
	>>> display(container, 'key_l0a_def_cond')
	'5;6'
	>>> display(container, 'key_l0a_def_force')
	'7;8'
	>>> display(container, 'key_l0a_def_pre')
	'9;0'
	>>> display(container, 'key_l0a_def_nset')

	>>> display(container, 'key_l1A_norm')
	'11;12'
	>>> display(container, 'key_l1a_add')
	'13;14'
	>>> display(container, 'key_l1a_cond')
	'15;16'
	>>> display(container, 'key_l1a_force')
	'17;18'
	>>> display(container, 'key_l1a_pre')
	'19;10'
	>>> display(container, 'key_l1a_def_norm')
	'11;12'
	>>> display(container, 'key_l1a_def_add')
	'13;14'
	>>> display(container, 'key_l1a_def_cond')
	'15;16'
	>>> display(container, 'key_l1a_def_force')
	'17;18'
	>>> display(container, 'key_l1a_def_pre')
	'19;10'

	>>> display(container, 'key_l0b_norm')
	'1;2'
	>>> display(container, 'key_l0b_add')
	'13;14;3;4'
	>>> display(container, 'key_l0b_cond')
	'15;16'
	>>> display(container, 'key_l0b_force')
	'17;18'
	>>> display(container, 'key_l0b_pre')
	'9;0;19;10'
	>>> display(container, 'key_l0b_def_norm')
	'1;2'
	>>> display(container, 'key_l0b_def_add')
	'13;14;3;4'
	>>> display(container, 'key_l0b_def_cond')
	'15;16'
	>>> display(container, 'key_l0b_def_force')
	'17;18'
	>>> display(container, 'key_l0b_def_pre')
	'9;0;19;10'
	"""

run_test()
