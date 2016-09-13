#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import cmp_obj, create_config, run_test, try_catch
from grid_control.parameters.config_param import ParameterConfig, frange, parse_parameter_option, parse_tuple


class Test_ParameterConfigParser:
	"""
	>>> try_catch(lambda: frange(5), 'ConfigError', 'No exit condition')
	caught
	>>> try_catch(lambda: frange(1, 5, 5, 1), 'ConfigError', 'Overdetermined parameters')
	caught
	>>> frange(1, end = 5)
	['1', '2', '3', '4', '5']
	>>> frange(1.5, end = 5)
	['1.5', '2.5', '3.5', '4.5']
	>>> frange(1.5, num = 5)
	['1.5', '2.5', '3.5', '4.5', '5.5']
	>>> frange(5, num = 5)
	['5', '6', '7', '8', '9']
	>>> frange(0.5, num = 5, steps = 1.5)
	['0.5', '2', '3.5', '5', '6.5']
	>>> frange(0, end = 5, num = 3)
	['0', '2.5', '5']
	>>> frange(0, end = 5, num = 11)
	['0', '0.5', '1', '1.5', '2', '2.5', '3', '3.5', '4', '4.5', '5']
	>>> frange(0, end = 5, steps = 1.5)
	['0', '1.5', '3', '4.5']

	>>> parse_tuple('()', ',')
	()
	>>> parse_tuple('(,)', ',')
	('', '')
	>>> parse_tuple('(, )', ',')
	('', '')
	>>> parse_tuple('( ,)', ',')
	('', '')
	>>> parse_tuple('( , )', ',')
	('', '')
	>>> parse_tuple('(,,)', ',')
	('', '', '')

	>>> parse_parameter_option('a')
	('a', None)
	>>> parse_parameter_option('a1')
	('a1', None)
	>>> parse_parameter_option('a b')
	('a', 'b')
	>>> parse_parameter_option('a1 b1')
	('a1', 'b1')
	>>> parse_parameter_option('a b c')
	('a', 'b c')
	>>> parse_parameter_option('a1 b1 c1')
	('a1', 'b1 c1')
	>>> parse_parameter_option('(a) b c')
	(('a',), 'b c')
	>>> parse_parameter_option('(a1) b1 c1')
	(('a1',), 'b1 c1')
	>>> parse_parameter_option('(a_b) b_c c_d')
	(('a_b',), 'b_c c_d')
	>>> parse_parameter_option('(a_b b_c) c_d')
	(('a_b', 'b_c'), 'c_d')
	>>> parse_parameter_option('(a b)')
	(('a', 'b'), None)
	>>> parse_parameter_option('(a1 b1)')
	(('a1', 'b1'), None)
	>>> parse_parameter_option('(a b) c')
	(('a', 'b'), 'c')
	>>> parse_parameter_option('(a1 b1) c1')
	(('a1', 'b1'), 'c1')

	>>> parse_parameter_option('a b c')
	('a', 'b c')
	>>> parse_parameter_option('(a) b c')
	(('a',), 'b c')
	>>> parse_parameter_option('(a b)')
	(('a', 'b'), None)
	>>> parse_parameter_option('(a b) c')
	(('a', 'b'), 'c')
	"""

class Test_ParameterConfig:
	"""
	>>> pc = ParameterConfig(create_config(config_file='test.conf'))

	>>> pc.get_parameter('a')
	['1', '2', '3', '4', '5 6', '7', '8', '9 0']
	>>> pc.get_parameter('b')
	['a b c d', 'e f g h']
	>>> pc.get_parameter('e')
	[2, 4, 6, 8]

	>>> pc.get_parameter('ss')
	['a b c', 'd\\ne f', 'g h']

	>>> pc.get_parameter('c') == ({('Y',): [987], None: [123], ('X',): [511]}, [('X',), ('Y',)])
	True
	>>> pc.get_parameter('d') == ({('A',): ['511', '456'], ('B',): ['987', '823', '177']}, [('A',), ('B',)])
	True
	>>> pc.get_parameter('g') == ({('A',): ['511', '456'], ('B',): ['987 823', '177'], None: ['124', '634']}, [('A',), ('B',)])
	True
	>>> pc.get_parameter('h') == ({('A',): ['511', '456'], ('B',): ['987', '823', '177'], None: ['124', '634']}, [('A',), ('B',)])
	True

	>>> pc.get_parameter('dd')
	['A => 511 456', 'B => 987 823 177']

	>>> pc.get_parameter('x')
	[0, 3, 6, 11]
	>>> pc.get_parameter('y')
	['2', '3', '4 1', '2']

	>>> pc.get_parameter('x1')
	['1', '3']
	>>> pc.get_parameter('y2')
	['2', '4']

	>>> pc.get_parameter('t1')
	[1, 1, '1,']
	>>> pc.get_parameter('t2')
	['"2"', '"2,3"', '","']
	>>> pc.get_parameter('t3')
	[3, 4, '']

	>>> pc.get_parameter('w')
	['1', '2', '3', '4', '5']
	>>> pc.get_parameter('z')
	['2', '3', '4', '5', '6']

	>>> pc.get_parameter('s')
	['1', '8']
	>>> pc.get_parameter('j')
	['2,3', '2']
	>>> pc.get_parameter('f')
	[4, 1]

	>>> pc.get_parameter('t4')
	['', '', '1', '1', '', '', '1', '1']
	>>> pc.get_parameter('t5')
	['', '1', '', '1', '', '1', '', '1']

	>>> pc.get_parameter('TEST_1')
	[1, 8]
	>>> pc.get_parameter('1_TEST')
	['2,3', '2']
	>>> pc.get_parameter('TEST_TEST')
	['4', '1']

	>>> pm1 = pc.get_parameter('m1')
	>>> cmp_obj(pm1, ({('J', 'K'): ['6', '7'], ('K', 'L'): ['1', '2', '3']}, [('K', 'L'), ('J', 'K')]))
	>>> pn1 = pc.get_parameter('n1')
	>>> cmp_obj(pn1, ({('J', 'K'): ['7', '8'], ('K', 'L'): ['2', '3', '4']}, [('K', 'L'), ('J', 'K')]))

	>>> pm = pc.get_parameter('m')
	>>> cmp_obj(pm, ({('J', 'K'): ['6 7'], ('K', 'L'): ['1 2 3', '6 7 1']}, [('K', 'L'), ('J', 'K')]))
	>>> pn = pc.get_parameter('n')
	>>> cmp_obj(pn, ({('J', 'K'): ["'Y'"], ('K', 'L'): ["'X'", "'Z'"]}, [('K', 'L'), ('J', 'K')]))
	>>> try_catch(lambda: pc.get_parameter('o'), 'ConfigError', 'expands to multiple variable entries')
	caught
	>>> try_catch(lambda: pc.get_parameter('p'), 'ConfigError', 'Variable p is undefined')
	caught
	"""

run_test()
