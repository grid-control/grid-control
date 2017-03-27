#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test
from grid_control.config.matcher_base import DictLookup, ListFilter, ListOrder, Matcher


def testMatch(m, selector, value):
	r1 = m.matcher(value, selector)
	r2 = m.create_matcher(selector).match(value)
	assert(r1 == r2)
	print(r1)

def testListFilter(name, m, selector, value, order=ListOrder.source, key=None, negate=False):
	lf = ListFilter.create_instance(name, selector, m, order, negate)
	return lf.filter_list(value, key)

def createMatcher(name, settings=None):
	config = create_config(config_dict={'section': settings or {}})
	return Matcher.create_instance(name, config, 'prefix')

def testMatcher(name, selector, value, settings=None):
	matcher = createMatcher(name, settings)
	return testMatch(matcher, selector, value)

class Test_Matcher:
	"""
	>>> createMatcher('StartMatcher').create_matcher('A')
	StartMatcher_FixedSelector('A')
	>>> createMatcher('StartMatcher', {'prefix case sensitive': False}).create_matcher('A')
	StartMatcher_FixedSelector_ci('a')

	>>> testMatcher('AlwaysMatcher', '', '')
	1
	>>> testMatcher('AlwaysMatcher', 'A', 'ABC')
	1
	>>> testMatcher('AlwaysMatcher', 'X', 'ABC')
	1

	>>> testMatcher('StartMatcher', 'A', 'ABC')
	1
	>>> testMatcher('StartMatcher', 'C', 'ABC')
	-1
	>>> testMatcher('StartMatcher', 'ABC', 'ABC')
	1

	>>> testMatcher('StartMatcher', 'A', 'aBC')
	-1
	>>> testMatcher('StartMatcher', 'C', 'aBC')
	-1
	>>> testMatcher('StartMatcher', 'ABC', 'aBC')
	-1
	>>> testMatcher('StartMatcher', 'a', 'aBC')
	1
	>>> testMatcher('StartMatcher', 'c', 'aBC')
	-1
	>>> testMatcher('StartMatcher', 'abc', 'aBC')
	-1
	>>> testMatcher('StartMatcher', 'A', 'aBC', {'prefix case sensitive': False})
	1
	>>> testMatcher('StartMatcher', 'C', 'aBC', {'prefix case sensitive': False})
	-1
	>>> testMatcher('StartMatcher', 'ABC', 'aBC', {'prefix case sensitive': False})
	1
	>>> testMatcher('StartMatcher', 'a', 'aBC', {'prefix case sensitive': False})
	1
	>>> testMatcher('StartMatcher', 'c', 'aBC', {'prefix case sensitive': False})
	-1
	>>> testMatcher('StartMatcher', 'abc', 'aBC', {'prefix case sensitive': False})
	1

	>>> testMatcher('EndMatcher', 'A', 'ABC')
	-1
	>>> testMatcher('EndMatcher', 'C', 'ABC')
	1
	>>> testMatcher('EndMatcher', 'ABC', 'ABC')
	1

	>>> m3 = createMatcher('EqualMatcher')
	>>> testMatch(m3, 'A', 'ABC')
	-1
	>>> testMatch(m3, 'C', 'ABC')
	-1
	>>> testMatch(m3, 'ABC', 'ABC')
	1

	>>> m4 = createMatcher('ExprMatcher')
	>>> m4.get_positive_selector('"A" in value') is None
	True
	>>> testMatch(m4, '"A" in value', 'ABC')
	1
	>>> testMatch(m4, '"X" in value', 'ABC')
	-1
	>>> testMatch(m4, '"ABC" in value', 'ABC')
	1

	>>> m5_c = createMatcher('RegexMatcher', {'prefix case sensitive': False})
	>>> m5_c.get_positive_selector('A..') is None
	True
	>>> testMatch(m5_c, 'A..', 'ABC')
	1
	>>> testMatch(m5_c, '.BA', 'ABC')
	-1
	>>> testMatch(m5_c, '..C', 'ABC')
	1
	>>> m5 = createMatcher('RegexMatcher')
	>>> m5.get_positive_selector('A..') is None
	True
	>>> testMatch(m5, 'A..', 'ABC')
	1
	>>> testMatch(m5, '.BA', 'ABC')
	-1
	>>> testMatch(m5, '..C', 'ABC')
	1

	>>> m6_c = createMatcher('blackwhite', {'prefix case sensitive': False})
	>>> m6_c.get_positive_selector('-A AB')
	['AB']
	>>> testMatch(m6_c, '-a ab', 'ABC')
	2
	>>> testMatch(m6_c, '-A AB', 'abc')
	2

	>>> m6 = createMatcher('blackwhite')
	>>> m6.get_positive_selector('-A AB')
	['AB']
	>>> testMatch(m6, '-A AB', 'ABC')
	2
	>>> testMatch(m6, '-A AC', 'ABC')
	-1
	>>> testMatch(m6, 'AB -ABC', 'A')
	0
	>>> testMatch(m6, 'ABC', 'ABC')
	1

	>>> m7_c = createMatcher('ShellStyleMatcher', {'prefix case sensitive': False})
	>>> m7_c.get_positive_selector('A*')
	['A*']
	>>> testMatch(m7_c, 'a*', 'ABC')
	1
	>>> testMatch(m7_c, '*ba', 'ABC')
	-1
	>>> testMatch(m7_c, '*c', 'ABC')
	1
	>>> m7 = createMatcher('ShellStyleMatcher')
	>>> m7.get_positive_selector('A*')
	['A*']
	>>> testMatch(m7, '.*', '.git')
	1
	>>> testMatch(m7, '.*', 'git')
	-1
	>>> testMatch(m7, 'A*', 'ABC')
	1
	>>> testMatch(m7, '*BA', 'ABC')
	-1
	>>> testMatch(m7, '*C', 'ABC')
	1
	"""

class Test_MatchList:
	"""
	>>> config = create_config()
	>>> m1 = Matcher.create_instance('StartMatcher', config, 'prefix')
	>>> m2 = Matcher.create_instance('blackwhite', config, 'prefix')

	>>> testListFilter('strict', m1, 'AB', ['A', 'AB', 'ABC', 'ABCD', 'XAB'])
	['AB', 'ABC', 'ABCD']
	>>> testListFilter('strict', m2, 'AB -ABC', ['A', 'AB', 'ABC', 'ABCD', 'XAB', 'ABX'])
	['AB', 'ABX']
	>>> testListFilter('strict', m2, 'AB -ABC', ['X', 'Y', 'Z'])
	[]

	>>> testListFilter('try_strict', m1, 'AB', ['A', 'AB', 'ABC', 'ABCD', 'XAB'])
	['AB', 'ABC', 'ABCD']
	>>> testListFilter('try_strict', m2, 'AB -ABC', ['A', 'AB', 'ABC', 'ABCD', 'XAB', 'ABX'])
	['AB', 'ABX']
	>>> testListFilter('try_strict', m2, 'AB -ABC', ['X', 'Y', 'Z'])
	['X', 'Y', 'Z']

	>>> testListFilter('weak', m1, 'AB', ['A', 'AB', 'ABC', 'ABCD', 'XAB'])
	['AB', 'ABC', 'ABCD']
	>>> testListFilter('weak', m2, 'AB -ABC', ['A', 'AB', 'ABC', 'ABCD', 'XAB', 'ABX'])
	['A', 'AB', 'XAB', 'ABX']
	>>> testListFilter('weak', m2, 'AB -ABC', ['X', 'Y', 'Z'])
	['X', 'Y', 'Z']

	>>> testListFilter('strict', m2, 'A -AB ABCD', ['AB', 'ABC', 'B', 'ABCD', 'A'])
	['ABCD', 'A']
	>>> testListFilter('strict', m2, 'A -AB ABCD', ['AB', 'ABC', 'B', 'ABCD', 'A'], ListOrder.matcher)
	['A', 'ABCD']
	>>> testListFilter('strict', m2, 'A -AB ABCD', ['AB', 'ABC', 'B', 'ABCD', 'A'], negate=True)
	['AB', 'ABC']

	>>> testListFilter('strict', m2, 'A -AB ABCD', [{'data': 'AB'}, {'data': 'ABC'}, {'data': 'B'}, {'data': 'ABCD'}, {'data': 'A'}], key=lambda x: x['data'])
	[{'data': 'ABCD'}, {'data': 'A'}]
	>>> testListFilter('strict', m2, 'A -AB ABCD', [{'data': 'AB'}, {'data': 'ABC'}, {'data': 'B'}, {'data': 'ABCD'}, {'data': 'A'}], key=lambda x: x['data'], order=ListOrder.matcher)
	[{'data': 'A'}, {'data': 'ABCD'}]
	>>> testListFilter('strict', m2, 'A -AB ABCD', [{'data': 'AB'}, {'data': 'ABC'}, {'data': 'B'}, {'data': 'ABCD'}, {'data': 'A'}], key=lambda x: x['data'], negate=True)
	[{'data': 'AB'}, {'data': 'ABC'}]
	"""

class Test_Lookup:
	"""
	>>> config = create_config()
	>>> m1 = Matcher.create_instance('StartMatcher', config, 'prefix')
	>>> d1 = {'A': 1, 'B': 2, 'AB': 3, 'ABC': 4, None: 5}
	>>> o1 = ['A', 'ABC', 'AB', 'B']
	>>> repr(DictLookup(d1, o1, m1, only_first=True, always_default=False))
	'DictLookup(values = {A = 1, ABC = 4, AB = 3, B = 2, None = 5}, matcher = StartMatcher(case sensitive = True), only_first = True, always_default = False)'
	>>> DictLookup(d1, o1, m1, only_first=True, always_default=False).lookup('A')
	1
	>>> DictLookup(d1, o1, m1, only_first=True, always_default=False).lookup('A', is_selector=False)
	1

	>>> DictLookup(d1, o1, m1, only_first=False, always_default=False).lookup('A')
	[1, 4, 3]
	>>> DictLookup(d1, o1, m1, only_first=False, always_default=False).lookup('A', is_selector=False)
	[1]

	>>> DictLookup(d1, o1, m1, only_first=False, always_default=True).lookup('A')
	[1, 4, 3, 5]
	>>> DictLookup(d1, o1, m1, only_first=False, always_default=True).lookup('A', is_selector=False)
	[1, 5]

	>>> DictLookup(d1, o1, m1, only_first=True, always_default=True).lookup('A')
	1
	>>> DictLookup(d1, o1, m1, only_first=True, always_default=True).lookup('A', is_selector=False)
	1

	>>> DictLookup(d1, o1, m1, only_first=True, always_default=False).lookup('AB')
	4
	>>> DictLookup(d1, o1, m1, only_first=True, always_default=False).lookup('AB', is_selector=False)
	1

	>>> DictLookup(d1, o1, m1, only_first=False, always_default=False).lookup('AB')
	[4, 3]
	>>> DictLookup(d1, o1, m1, only_first=False, always_default=False).lookup('AB', is_selector=False)
	[1, 3]

	>>> DictLookup(d1, o1, m1, only_first=False, always_default=True).lookup('AB')
	[4, 3, 5]
	>>> DictLookup(d1, o1, m1, only_first=False, always_default=True).lookup('AB', is_selector=False)
	[1, 3, 5]

	>>> DictLookup(d1, o1, m1, only_first=True, always_default=True).lookup('AB')
	4
	>>> DictLookup(d1, o1, m1, only_first=True, always_default=True).lookup('AB', is_selector=False)
	1

	"""

class Test_ConfigWrapper:
	"""
	>>> config = create_config()
	>>> f = config.get_filter('foo filter', 'A B -C', default_matcher='blackwhite', default_filter='weak')
	>>> repr(f)
	"WeakListFilter(matcher = BlackWhiteMatcher(base matcher = StartMatcher(case sensitive = True)), positive = ['A', 'B'], order = 'source', negate = False)"
	>>> f.filter_list(None)
	['A', 'B']
	>>> f.filter_list(['A', 'C'])
	['A']
	>>> f.filter_list(['X', 'Y'])
	['X', 'Y']
	>>> l = config.get_lookup('bar lookup', {'A': 1, 'BC': 4, 'B': 2, 'AB': 3})
	>>> l.lookup('A')
	'1'
	>>> l.lookup('AB')
	'3'
	>>> l.lookup('B') # dict is sorted by keys
	'2'
	>>> l = config.get_lookup('bar lookup', {'A': 1, 'BC': 4, 'B': 2, 'AB': 3}, single=False)
	>>> l.lookup('A')
	['1', '3']
	>>> l.lookup('AB')
	['3']
	>>> l.lookup('B') # dict is sorted by keys
	['2', '4']
	>>> l = config.get_lookup('bar lookup', {'A': 1, 'BC': 4, 'B': 2, 'AB': 3}, single=False)
	>>> l.lookup('A')
	['1', '3']
	>>> l.lookup('AB')
	['3']
	>>> l.lookup('B') # dict is sorted by keys
	['2', '4']
	>>> l = config.get_lookup('foobar lookup', {None: 0, 'A': 1, 'BC': 4, 'B': 2, 'AB': 3}, single=False)
	>>> l.lookup(None)
	['0']
	>>> l.lookup(None, is_selector=False)
	['0']
	>>> l.lookup('A')
	['1', '3']
	>>> l.lookup('A', is_selector=False)
	['1']
	>>> l.lookup('B')
	['2', '4']
	>>> l.lookup('B', is_selector=False)
	['2']
	"""

run_test()
