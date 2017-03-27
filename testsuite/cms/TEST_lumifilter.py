#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, try_catch
from grid_control_cms.lumi_tools import filter_lumi_filter, parse_lumi_filter


class Test_LumiFilter:
	"""
	>>> a = parse_lumi_filter('')
	>>> a
	>>> x = parse_lumi_filter('1:23-2:45')
	>>> x
	[([1, 23], [2, 45])]
	>>> y = parse_lumi_filter('test1.json')
	>>> y
	[([10, 1], [10, 329]), ([10, 332], [10, 658]), ([11, 1], [11, 95])]
	>>> z = parse_lumi_filter('test2.json|133450-')
	>>> z
	[([133450, 1], [133450, 329]), ([133450, 332], [133450, 658]), ([133474, 1], [133474, 95])]
	>>> w = parse_lumi_filter('3-5')
	>>> w
	[([3, None], [5, None])]
	>>> try_catch(lambda: parse_lumi_filter('test3.json'), 'ConfigError', 'Could not process lumi filter file')
	caught
	>>> try_catch(lambda: parse_lumi_filter('xxx.json'), 'ConfigError', 'Could not process lumi filter file')
	caught
	>>> list(filter_lumi_filter([2,3,6], [([1, None], [2, None]), ([4, 1], [4, None]), ([5, 1], [None, 3])]))
	[([1, None], [2, None]), ([5, 1], [None, 3])]
	>>> list(filter_lumi_filter([2,3,6], [([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7,3]), ([7,5], [10,1])]))
	[([1, 1], [2, 2]), ([3, 1], [5, 2]), ([5, 2], [7, 3])]
	>>> list(filter_lumi_filter([2,3,6], [([2, 1], [3, 2])]))
	[([2, 1], [3, 2])]
	>>> list(filter_lumi_filter([2,2,2], [([2, 1], [3, 2])]))
	[([2, 1], [3, 2])]
	>>> list(filter_lumi_filter([2,2,2], [([3, 1], [3, 2])]))
	[]
	>>> list(filter_lumi_filter([2,2,2], [([3, 1], [3, 2])]))
	[]
	"""

run_test()
