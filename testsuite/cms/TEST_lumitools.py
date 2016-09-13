#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import run_test, try_catch
from grid_control_cms.lumi_tools import filterLumiFilter, formatLumi, mergeLumi, parseLumiFilter, parseLumiFromString, selectLumi, selectRun
from python_compat import imap, lmap


class Test_Lumi:
	"""
	>>> parseLumiFilter('123:123')
	[([123, 123], [123, 123])]
	>>> try_catch(lambda: parseLumiFilter('123:;123'), 'ConfigError', 'Could not process lumi filter expression')
	caught

	>>> mergeLumi([([1, 11], [1, 20]), ([1, 1], [1, 10]), ([1, 22], [1, 30])])
	[([1, 1], [1, 20]), ([1, 22], [1, 30])]
	>>> mergeLumi([([1, 1], [2, 2]), ([2, 3], [2, 10]), ([2, 11], [4, 30])])
	[([1, 1], [4, 30])]

	>>> lmap(parseLumiFromString, ['1', '1-', '-1', '1-2'])
	[([1, None], [1, None]), ([1, None], [None, None]), ([None, None], [1, None]), ([1, None], [2, None])]
	>>> lmap(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6'])
	[([1, 5], [1, 5]), ([1, 5], [None, None]), ([None, None], [1, 5]), ([1, 5], [2, 6])]
	>>> lmap(parseLumiFromString, ['1-:5', ':5-1', ':5-:6'])
	[([1, None], [None, 5]), ([None, 5], [1, None]), ([None, 5], [None, 6])]
	>>> lmap(parseLumiFromString, ['1:5-2', '1-2:5'])
	[([1, 5], [2, None]), ([1, None], [2, 5])]

	>>> formatLumi(filterLumiFilter([2,3,6], [([1, None], [2, None]), ([4, 1], [4, None]), ([5, 1], [None,3])]))
	['1:MIN-2:MAX', '5:1-9999999:3']
	>>> formatLumi(filterLumiFilter([2,3,6], [([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7,3])]))
	['1:1-2:2', '3:1-5:2', '5:2-7:3']

	>>> selectRun(1, [([1, None], [2, None])])
	True
	>>> selectRun(2, [([1, 3], [5, 12])])
	True
	>>> selectRun(6, [([1, 3], [5, 12])])
	False
	>>> selectRun(9, [([3, 23], [None, None])])
	True

	>>> selectLumi((1,2), [([1, None], [2, None])])
	True
	>>> selectLumi((1,2), [([1, 3], [5, 12])])
	False
	>>> selectLumi((2,1), [([1, 3], [5, 12])])
	True
	>>> selectLumi((9,2), [([3, 23], [None, None])])
	True

	>>> formatLumi(imap(parseLumiFromString, ['1', '1-', '-1', '1-2']))
	['1:MIN-1:MAX', '1:MIN-9999999:MAX', '1:MIN-1:MAX', '1:MIN-2:MAX']
	>>> formatLumi(imap(parseLumiFromString, ['1:5', '1:5-', '-1:5', '1:5-2:6']))
	['1:5-1:5', '1:5-9999999:MAX', '1:MIN-1:5', '1:5-2:6']
	>>> formatLumi(imap(parseLumiFromString, ['1-:5', ':5-1', ':5-:6']))
	['1:MIN-9999999:5', '1:5-1:MAX', '1:5-9999999:6']
	>>> formatLumi(imap(parseLumiFromString, ['1:5-2', '1-2:5']))
	['1:5-2:MAX', '1:MIN-2:5']
	"""

run_test()
