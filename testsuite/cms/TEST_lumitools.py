#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, try_catch
from grid_control_cms.lumi_tools import filter_lumi_filter, format_lumi, merge_lumi_list, parse_lumi_filter, parse_lumi_from_str, select_lumi, select_run
from python_compat import imap, lmap


class Test_Lumi:
	"""
	>>> parse_lumi_filter('123:123')
	[([123, 123], [123, 123])]
	>>> try_catch(lambda: parse_lumi_filter('123:;123'), 'ConfigError', 'Could not process lumi filter expression')
	caught

	>>> merge_lumi_list([([1, 11], [1, 20]), ([1, 1], [1, 10]), ([1, 22], [1, 30])])
	[([1, 1], [1, 20]), ([1, 22], [1, 30])]
	>>> merge_lumi_list([([1, 1], [2, 2]), ([2, 3], [2, 10]), ([2, 11], [4, 30])])
	[([1, 1], [4, 30])]

	>>> lmap(parse_lumi_from_str, ['1', '1-', '-1', '1-2'])
	[([1, None], [1, None]), ([1, None], [None, None]), ([None, None], [1, None]), ([1, None], [2, None])]
	>>> lmap(parse_lumi_from_str, ['1:5', '1:5-', '-1:5', '1:5-2:6'])
	[([1, 5], [1, 5]), ([1, 5], [None, None]), ([None, None], [1, 5]), ([1, 5], [2, 6])]
	>>> lmap(parse_lumi_from_str, ['1-:5', ':5-1', ':5-:6'])
	[([1, None], [None, 5]), ([None, 5], [1, None]), ([None, 5], [None, 6])]
	>>> lmap(parse_lumi_from_str, ['1:5-2', '1-2:5'])
	[([1, 5], [2, None]), ([1, None], [2, 5])]

	>>> format_lumi(filter_lumi_filter([2,3,6], [([1, None], [2, None]), ([4, 1], [4, None]), ([5, 1], [None,3])]))
	['1:MIN-2:MAX', '5:1-9999999:3']
	>>> format_lumi(filter_lumi_filter([2,3,6], [([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7,3])]))
	['1:1-2:2', '3:1-5:2', '5:2-7:3']

	>>> select_run(1, [([1, None], [2, None])])
	True
	>>> select_run(2, [([1, 3], [5, 12])])
	True
	>>> select_run(6, [([1, 3], [5, 12])])
	False
	>>> select_run(9, [([3, 23], [None, None])])
	True

	>>> select_lumi((1,2), [([1, None], [2, None])])
	True
	>>> select_lumi((1,2), [([1, 3], [5, 12])])
	False
	>>> select_lumi((2,1), [([1, 3], [5, 12])])
	True
	>>> select_lumi((9,2), [([3, 23], [None, None])])
	True

	>>> format_lumi(imap(parse_lumi_from_str, ['1', '1-', '-1', '1-2']))
	['1:MIN-1:MAX', '1:MIN-9999999:MAX', '1:MIN-1:MAX', '1:MIN-2:MAX']
	>>> format_lumi(imap(parse_lumi_from_str, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5-1:5', '1:5-9999999:MAX', '1:MIN-1:5', '1:5-2:6']
	>>> format_lumi(imap(parse_lumi_from_str, ['1-:5', ':5-1', ':5-:6']))
	['1:MIN-9999999:5', '1:5-1:MAX', '1:5-9999999:6']
	>>> format_lumi(imap(parse_lumi_from_str, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:MIN-2:5']
	"""

run_test()
