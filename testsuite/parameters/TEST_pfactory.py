#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import random, logging
from testFwk import create_config, remove_files_testsuite, run_test, try_catch
from grid_control.parameters import ParameterAdapter
from hpfwk import Plugin
from testINC import DataSplitProcessorTest, display_ps_str2ps_str, norm_ps_display, ps_str2display_ps_str, testPA
from python_compat import imap, irange, izip


ParameterFactory = Plugin.get_class('ParameterFactory')
global_repo = {}
random.randint = lambda *args: 42 # 100% randomly choosen

def DS_setup():
	config = create_config(config_dict = {'dataset': {'files per job': 1}})
	provider = Plugin.create_instance('ListProvider', config, 'dataset', 'test.dbs')
	splitter = Plugin.create_instance('FileBoundarySplitter', config, 'dataset')
	pproc = DataSplitProcessorTest(config, 'dataset')
	dps = Plugin.create_instance('DataParameterSource', '.', 'data', provider, splitter, pproc, global_repo, keep_old = False)

def DS_clear():
	global_repo.pop('dataset:data', None)
	remove_files_testsuite(['datamap.tar', 'datacache.dat'])

def getPF(name, pstr, var_dict, **kwargs):
	pdict = {}
	for key in var_dict:
		pdict[key.lower()] = display_ps_str2ps_str(var_dict[key])
	for key in kwargs:
		pdict[key.replace('_', ' ').lower()] = kwargs[key]
	pdict['parameters'] = display_ps_str2ps_str(pstr)
	config = create_config(config_dict={'parameters': pdict})
	pf = ParameterFactory.create_instance(name, config)
	return pf.get_psrc(global_repo)

def testPF(source, details = False, **kwargs):
	every = 1
	if not details:
		every = 3
	pa = ParameterAdapter.create_instance('BasicParameterAdapter', create_config(), source)
	testPA(pa, showJob = details, showPNum = details,
		showMetadata = details, showIV = details, showKeys = details,
		showUntracked = True, showJobPrefix = details, newlineEvery = every)

def getMPF(pstr, **kwargs):
	mpf_config = {
		'A': '1 2 3',
		'B': 'x y',
		'C': 'a b',
	}
	return getPF('ModularParameterFactory', display_ps_str2ps_str(pstr), mpf_config, **kwargs)

def testMPF(pstr, **kwargs):
	show = kwargs.pop('show_source', False)
	ps = getMPF(pstr, **kwargs)
	if show:
		norm_ps_display(ps)
	return testPF(ps, **kwargs)

class Test_ParameterFactory:
	"""
	>>> try_catch(lambda: getPF('ParameterFactory', 'test', {}), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: getPF('UserParameterFactory', 'test', {}), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_ModularParameterFactory:
	"""
	>>> getMPF('')
	null()
	>>> getMPF('null()')
	null()
	>>> getMPF("var('A')")
	var('A')
	>>> DS_setup()
	>>> getMPF('data()')
	data()
	>>> testMPF('data()')
	2
	{'!EVT': 10, '!FN': 'file1', 'SID': 0, '!SKIP': 0} {'!EVT': 10, '!FN': 'file2', 'SID': 1, '!SKIP': 0}
	>>> DS_clear()

	>>> testMPF('')
	None
	{}
	{}
	>>> testMPF("counter('X', 100)")
	None
	{'!X': 101}
	{'!X': 111}

	>>> testMPF("var('A')", A = '1 2 3')
	3
	{'A': '1'} {'A': '2'} {'A': '3'}
	>>> testMPF("const('A')", A = '1 2 3')
	None
	{'A': '1 2 3'}
	{'A': '1 2 3'}
	>>> testMPF("var('a')", A = '1 2 3')
	3
	{'a': '1'} {'a': '2'} {'a': '3'}
	>>> testMPF("repeat(var('a'), 5)", A = '1 2 3')
	15
	{'a': '1'} {'a': '2'} {'a': '3'}
	{'a': '1'} {'a': '2'} {'a': '3'}
	{'a': '1'} {'a': '2'} {'a': '3'}
	{'a': '1'} {'a': '2'} {'a': '3'}
	{'a': '1'} {'a': '2'} {'a': '3'}
	>>> testMPF("ZIP(var('a'), var('b'))", A = '1 2 3', B = 'x y')
	3
	{'a': '1', 'b': 'x'} {'a': '2', 'b': 'y'} {'a': '3'}
	>>> testMPF("cross(var('a'), var('b'))", A = '1 2 3', B = 'x y')
	6
	{'a': '1', 'b': 'x'} {'a': '2', 'b': 'x'} {'a': '3', 'b': 'x'}
	{'a': '1', 'b': 'y'} {'a': '2', 'b': 'y'} {'a': '3', 'b': 'y'}
	>>> testMPF("cross(var('a'), var('b'), var('C'))", A = '1 2 3', B = 'x y', C = 'a b')
	12
	{'C': 'a', 'a': '1', 'b': 'x'} {'C': 'a', 'a': '2', 'b': 'x'} {'C': 'a', 'a': '3', 'b': 'x'}
	{'C': 'a', 'a': '1', 'b': 'y'} {'C': 'a', 'a': '2', 'b': 'y'} {'C': 'a', 'a': '3', 'b': 'y'}
	{'C': 'b', 'a': '1', 'b': 'x'} {'C': 'b', 'a': '2', 'b': 'x'} {'C': 'b', 'a': '3', 'b': 'x'}
	{'C': 'b', 'a': '1', 'b': 'y'} {'C': 'b', 'a': '2', 'b': 'y'} {'C': 'b', 'a': '3', 'b': 'y'}
	>>> testMPF("ZIP(var('a'), var('b'), var('C'))", A = '1 2 3', B = 'x y', C = 'a b')
	3
	{'C': 'a', 'a': '1', 'b': 'x'} {'C': 'b', 'a': '2', 'b': 'y'} {'a': '3'}
	>>> testMPF("chain(var('a'), var('b'), var('C'))", A = '1 2 3', B = 'x y', C = 'a b')
	7
	{'a': '1'} {'a': '2'} {'a': '3'}
	{'b': 'x'} {'b': 'y'} {'C': 'a'}
	{'C': 'b'}

	>>> testMPF("cross(rng(), var('a'), counter('b', 90), const('c', 'TEST'))",
	... A = '1 2 3', b = None, c = None)
	3
	{'!JOB_RANDOM': 42, 'a': '1', '!b': 90, 'c': 'TEST'} {'!JOB_RANDOM': 42, 'a': '2', '!b': 91, 'c': 'TEST'} {'!JOB_RANDOM': 42, 'a': '3', '!b': 92, 'c': 'TEST'}

	>>> testMPF("cross(var('a'), transform('b', 'a + 100'), req())", a = '1 2 3', show_source = True)
	ZIP(var('a'), transform('b', 'a + 100', ''), req())
	3
	{'a': '1', '!b': 101} {'a': '2', '!b': 102} {'a': '3', '!b': 103}
	>>> testMPF("cross(var('a'), var('b'))",
	... A = '1 2 3', b = 'x y z', show_source = True)
	cross(var('a'), var('b'))
	9
	{'a': '1', 'b': 'x'} {'a': '2', 'b': 'x'} {'a': '3', 'b': 'x'}
	{'a': '1', 'b': 'y'} {'a': '2', 'b': 'y'} {'a': '3', 'b': 'y'}
	{'a': '1', 'b': 'z'} {'a': '2', 'b': 'z'} {'a': '3', 'b': 'z'}
	>>> testMPF("cross(cross(var('a'), var('b')), format('c', '%02d', 'a', 0))",
	... A = '1 2 3', b = 'x y z', c = None, show_source = True)
	ZIP(cross(var('a'), var('b')), format('c', '%02d', 'a', 0))
	9
	{'a': '1', 'b': 'x', '!c': '01'} {'a': '2', 'b': 'x', '!c': '02'} {'a': '3', 'b': 'x', '!c': '03'}
	{'a': '1', 'b': 'y', '!c': '01'} {'a': '2', 'b': 'y', '!c': '02'} {'a': '3', 'b': 'y', '!c': '03'}
	{'a': '1', 'b': 'z', '!c': '01'} {'a': '2', 'b': 'z', '!c': '02'} {'a': '3', 'b': 'z', '!c': '03'}
	>>> testMPF("ZIP(chain(var('va'), var('vb')), collect('t', 'v...'))",
	... VA = '1 2 3', Vb = 'x y z', t = None)
	6
	{'t': '1', 'va': '1'} {'t': '2', 'va': '2'} {'t': '3', 'va': '3'}
	{'t': 'x', 'vb': 'x'} {'t': 'y', 'vb': 'y'} {'t': 'z', 'vb': 'z'}
	>>> testMPF("RANGE(var('a'), 5)", A = '1 2 3 4 5 6 7 8 9 10')
	5
	{'a': '6'} {'a': '7'} {'a': '8'}
	{'a': '9'} {'a': '10'}
	>>> testMPF("RANGE(var('a'), 0, 2)", A = '1 2 3 4 5 6 7 8 9 10')
	3
	{'a': '1'} {'a': '2'} {'a': '3'}
	>>> testMPF("RANGE(var('a'), None, 1)", A = '1 2 3 4 5 6 7 8 9 10')
	2
	{'a': '1'} {'a': '2'}
	>>> testMPF("RANGE(var('a'), 3, 6)", A = '1 2 3 4 5 6 7 8 9 10')
	4
	{'a': '4'} {'a': '5'} {'a': '6'}
	{'a': '7'}
	>>> testMPF("variation(var('a'), var('b'), var('c'))", A = 'a0 a-1 a+1', B = 'b0 b-1 b-2 b+1', C = 'c0 c+1 c+2')
	8
	{'a': 'a0', 'b': 'b0', 'c': 'c0'} {'a': 'a-1', 'b': 'b0', 'c': 'c0'} {'a': 'a+1', 'b': 'b0', 'c': 'c0'}
	{'a': 'a0', 'b': 'b-1', 'c': 'c0'} {'a': 'a0', 'b': 'b-2', 'c': 'c0'} {'a': 'a0', 'b': 'b+1', 'c': 'c0'}
	{'a': 'a0', 'b': 'b0', 'c': 'c+1'} {'a': 'a0', 'b': 'b0', 'c': 'c+2'}

	>>> getMPF("chain(var('a'), rng('b'))", a = '1 2')
	chain(var('a'), truncate(rng('!b'), 1))

	>>> logging.getLogger().setLevel(logging.CRITICAL)
	>>> try_catch(lambda: getMPF('dummy()'), 'ParameterError', 'Unable to parse parameter expression')
	caught
	>>> try_catch(lambda: testMPF("cross(var('a'), lookup(key('d'), null()))", A = 'a1 a2 a3 a b1', D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2'), 'ConfigError', 'Lookup parameter not defined')
	caught
	>>> try_catch(lambda: testMPF("switch(null(), key('d'), key('a'))", D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2'), 'ParameterError', 'infinite parameter space')
	caught
	>>> try_catch(lambda: testMPF("switch(counter('c', 1), key('d'), key('c'))", D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2'), 'ParameterError', 'with an infinite parameter space')
	caught
	>>> logging.getLogger().setLevel(logging.DEFAULT)

	>>> testMPF("cross(var('a'), lookup('d', 'a'))", A = 'a1 a2 a3 a b1', D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2')
	5
	{'a': 'a1', 'd': '3'} {'a': 'a2', 'd': '1'} {'a': 'a3', 'd': '1'}
	{'a': 'a', 'd': '2'} {'a': 'b1', 'd': 'x'}

	>>> testMPF("cross(var('a'), lookup(key('d'), key('a')))", A = 'a1 a2 a3 a b1', D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2')
	5
	{'a': 'a1', 'd': '3'} {'a': 'a2', 'd': '1'} {'a': 'a3', 'd': '1'}
	{'a': 'a', 'd': '2'} {'a': 'b1', 'd': 'x'}

	>>> testMPF("cross(var('a'), lookup(key('d'), key('a')))", D_matcher = 'start', A = 'a1 a2 a3 a b1', D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2')
	5
	{'a': 'a1', 'd': '3'} {'a': 'a2', 'd': '2'} {'a': 'a3', 'd': '2'}
	{'a': 'a', 'd': '2'} {'a': 'b1', 'd': 'x'}

	>>> testMPF("cross(var('a'), var('b'), var('c'))", A = 'a1 a2 a3', B = 'b1 b2', C = 'c1 c2 c3')
	18
	{'a': 'a1', 'b': 'b1', 'c': 'c1'} {'a': 'a2', 'b': 'b1', 'c': 'c1'} {'a': 'a3', 'b': 'b1', 'c': 'c1'}
	{'a': 'a1', 'b': 'b2', 'c': 'c1'} {'a': 'a2', 'b': 'b2', 'c': 'c1'} {'a': 'a3', 'b': 'b2', 'c': 'c1'}
	{'a': 'a1', 'b': 'b1', 'c': 'c2'} {'a': 'a2', 'b': 'b1', 'c': 'c2'} {'a': 'a3', 'b': 'b1', 'c': 'c2'}
	{'a': 'a1', 'b': 'b2', 'c': 'c2'} {'a': 'a2', 'b': 'b2', 'c': 'c2'} {'a': 'a3', 'b': 'b2', 'c': 'c2'}
	{'a': 'a1', 'b': 'b1', 'c': 'c3'} {'a': 'a2', 'b': 'b1', 'c': 'c3'} {'a': 'a3', 'b': 'b1', 'c': 'c3'}
	{'a': 'a1', 'b': 'b2', 'c': 'c3'} {'a': 'a2', 'b': 'b2', 'c': 'c3'} {'a': 'a3', 'b': 'b2', 'c': 'c3'}

	>>> testMPF("cross(var('a'), var('b'), var('c'), lookup(key('d'), key('a')))", A = 'a1 a2 a3', B = 'b1 b2', C = 'c1 c2 c3', D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2')
	18
	{'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '3'} {'a': 'a2', 'b': 'b1', 'c': 'c1', 'd': '1'} {'a': 'a3', 'b': 'b1', 'c': 'c1', 'd': '1'}
	{'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '3'} {'a': 'a2', 'b': 'b2', 'c': 'c1', 'd': '1'} {'a': 'a3', 'b': 'b2', 'c': 'c1', 'd': '1'}
	{'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '3'} {'a': 'a2', 'b': 'b1', 'c': 'c2', 'd': '1'} {'a': 'a3', 'b': 'b1', 'c': 'c2', 'd': '1'}
	{'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '3'} {'a': 'a2', 'b': 'b2', 'c': 'c2', 'd': '1'} {'a': 'a3', 'b': 'b2', 'c': 'c2', 'd': '1'}
	{'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '3'} {'a': 'a2', 'b': 'b1', 'c': 'c3', 'd': '1'} {'a': 'a3', 'b': 'b1', 'c': 'c3', 'd': '1'}
	{'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '3'} {'a': 'a2', 'b': 'b2', 'c': 'c3', 'd': '1'} {'a': 'a3', 'b': 'b2', 'c': 'c3', 'd': '1'}

	>>> testMPF("cross(var('a'), var('b'), var('c'), lookup(key('d'), key('a')))", D_matcher = 'start', A = 'a1 a2 a3', B = 'b1 b2', C = 'c1 c2 c3', D = '1 \\n a1 => 3 \\n b1 => x \\n a => 2')
	18
	{'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '3'} {'a': 'a2', 'b': 'b1', 'c': 'c1', 'd': '2'} {'a': 'a3', 'b': 'b1', 'c': 'c1', 'd': '2'}
	{'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '3'} {'a': 'a2', 'b': 'b2', 'c': 'c1', 'd': '2'} {'a': 'a3', 'b': 'b2', 'c': 'c1', 'd': '2'}
	{'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '3'} {'a': 'a2', 'b': 'b1', 'c': 'c2', 'd': '2'} {'a': 'a3', 'b': 'b1', 'c': 'c2', 'd': '2'}
	{'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '3'} {'a': 'a2', 'b': 'b2', 'c': 'c2', 'd': '2'} {'a': 'a3', 'b': 'b2', 'c': 'c2', 'd': '2'}
	{'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '3'} {'a': 'a2', 'b': 'b1', 'c': 'c3', 'd': '2'} {'a': 'a3', 'b': 'b1', 'c': 'c3', 'd': '2'}
	{'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '3'} {'a': 'a2', 'b': 'b2', 'c': 'c3', 'd': '2'} {'a': 'a3', 'b': 'b2', 'c': 'c3', 'd': '2'}

	>>> testMPF("switch(cross(var('a'), var('b'), var('c')), key('d'), key('a'))", A = 'a1 a2 a3 a4', B = 'b1 b2', C = 'c1 c2 c3', D = '\\n a1 => 31 32 33 \\n b1 => x y\\n a => 21')
	18
	{'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '33'}
	{'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '33'}
	{'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '33'}
	{'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '33'}
	{'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '33'}
	{'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '33'}

	>>> testMPF("switch(cross(var('a'), var('b'), var('c')), key('d'), key('a'))", A = 'a1 a2 a3 a4', B = 'b1 b2', C = 'c1 c2 c3', D = 'xx \\n a1 => 31 32 33 \\n b1 => x y\\n a => 21')
	36
	{'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '33'}
	{'a': 'a2', 'b': 'b1', 'c': 'c1', 'd': 'xx'} {'a': 'a3', 'b': 'b1', 'c': 'c1', 'd': 'xx'} {'a': 'a4', 'b': 'b1', 'c': 'c1', 'd': 'xx'}
	{'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '33'}
	{'a': 'a2', 'b': 'b2', 'c': 'c1', 'd': 'xx'} {'a': 'a3', 'b': 'b2', 'c': 'c1', 'd': 'xx'} {'a': 'a4', 'b': 'b2', 'c': 'c1', 'd': 'xx'}
	{'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '33'}
	{'a': 'a2', 'b': 'b1', 'c': 'c2', 'd': 'xx'} {'a': 'a3', 'b': 'b1', 'c': 'c2', 'd': 'xx'} {'a': 'a4', 'b': 'b1', 'c': 'c2', 'd': 'xx'}
	{'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '33'}
	{'a': 'a2', 'b': 'b2', 'c': 'c2', 'd': 'xx'} {'a': 'a3', 'b': 'b2', 'c': 'c2', 'd': 'xx'} {'a': 'a4', 'b': 'b2', 'c': 'c2', 'd': 'xx'}
	{'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '33'}
	{'a': 'a2', 'b': 'b1', 'c': 'c3', 'd': 'xx'} {'a': 'a3', 'b': 'b1', 'c': 'c3', 'd': 'xx'} {'a': 'a4', 'b': 'b1', 'c': 'c3', 'd': 'xx'}
	{'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '33'}
	{'a': 'a2', 'b': 'b2', 'c': 'c3', 'd': 'xx'} {'a': 'a3', 'b': 'b2', 'c': 'c3', 'd': 'xx'} {'a': 'a4', 'b': 'b2', 'c': 'c3', 'd': 'xx'}

	>>> testMPF("switch(cross(var('a'), var('b'), var('c')), key('d'), key('a'))", D_matcher = 'start', A = 'a1 a2 a3 a4', B = 'b1 b2', C = 'c1 c2 c3', D = '\\n a1 => 31 32 33 \\n b1 => x y\\n a => 21')
	36
	{'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c1', 'd': '33'}
	{'a': 'a2', 'b': 'b1', 'c': 'c1', 'd': '21'} {'a': 'a3', 'b': 'b1', 'c': 'c1', 'd': '21'} {'a': 'a4', 'b': 'b1', 'c': 'c1', 'd': '21'}
	{'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c1', 'd': '33'}
	{'a': 'a2', 'b': 'b2', 'c': 'c1', 'd': '21'} {'a': 'a3', 'b': 'b2', 'c': 'c1', 'd': '21'} {'a': 'a4', 'b': 'b2', 'c': 'c1', 'd': '21'}
	{'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c2', 'd': '33'}
	{'a': 'a2', 'b': 'b1', 'c': 'c2', 'd': '21'} {'a': 'a3', 'b': 'b1', 'c': 'c2', 'd': '21'} {'a': 'a4', 'b': 'b1', 'c': 'c2', 'd': '21'}
	{'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': '33'}
	{'a': 'a2', 'b': 'b2', 'c': 'c2', 'd': '21'} {'a': 'a3', 'b': 'b2', 'c': 'c2', 'd': '21'} {'a': 'a4', 'b': 'b2', 'c': 'c2', 'd': '21'}
	{'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '31'} {'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '32'} {'a': 'a1', 'b': 'b1', 'c': 'c3', 'd': '33'}
	{'a': 'a2', 'b': 'b1', 'c': 'c3', 'd': '21'} {'a': 'a3', 'b': 'b1', 'c': 'c3', 'd': '21'} {'a': 'a4', 'b': 'b1', 'c': 'c3', 'd': '21'}
	{'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '31'} {'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '32'} {'a': 'a1', 'b': 'b2', 'c': 'c3', 'd': '33'}
	{'a': 'a2', 'b': 'b2', 'c': 'c3', 'd': '21'} {'a': 'a3', 'b': 'b2', 'c': 'c3', 'd': '21'} {'a': 'a4', 'b': 'b2', 'c': 'c3', 'd': '21'}

	>>> testMPF("cross(var('a'), lookup('d', key('a')))", A = 'a1 a2 a3 ba b1', D = '1 \\n a1 => \\n b1 => x \\n a => 2', D_matcher = 'start')
	5
	{'a': 'a1'} {'a': 'a2', 'd': '2'} {'a': 'a3', 'd': '2'}
	{'a': 'ba', 'd': '1'} {'a': 'b1', 'd': 'x'}

	>>> testMPF("switch(var('a'), key('d'), 'a')", A = 'a1 a2 a3 ba b1', D = '1 \\n a1 => \\n b1 => x \\n a => 2', D_matcher = 'start', D_empty_set = True)
	4
	{'a': 'a2', 'd': '2'} {'a': 'a3', 'd': '2'} {'a': 'ba', 'd': '1'}
	{'a': 'b1', 'd': 'x'}
	"""

def getSPF(pstr, **kwargs):
	spf_config = {
		'A': 'test a b',
		'B': 'test a b',
		'BX': 'test a b',
		'C': 'test a b',
		'C1': 'a => test\n b => xxx',
		'C2': 'a => test\n b => xxx yyy',
		'X': 'a => test\n b => xxx',
		'D': 'test a b',
		'E': 'test a b',
		'F': 'test a b',
		'I': 'test a b',
		'csv source': 'test.csv',
		'data source': 'test.dbs',
		'(G,H)': 'a => (test, test1)\n b => (test2, test3)',
		'(G1,H1)': '(test, test1) (test2, test3)',
		'default lookup': 'A',
	}
	return getPF('SimpleParameterFactory', pstr, spf_config, **kwargs)

def testSPF(pstr, **kwargs):
	return testPF(getSPF(pstr, **kwargs), **kwargs)

class Test_SimpleParameterFactory:
	"""
	>>> getSPF('')
	null()
	>>> getSPF('A')
	var('A')
	>>> getSPF('<csv>')
	csv('test.csv')
	>>> DS_setup()
	>>> getSPF('<data>')
	data()
	>>> DS_clear()

	>>> norm_ps_display(getSPF('A, B, C'))
	ZIP(var('A'), var('B'), var('C'))
	>>> getSPF('A B C')
	cross(var('A'), var('B'), var('C'))
	>>> getSPF('A B * C')
	cross(var('A'), var('B'), var('C'))
	>>> norm_ps_display(getSPF('A B, 2*C'))
	ZIP(cross(var('A'), var('B')), repeat(var('C'), 2))
	>>> norm_ps_display(getSPF('2 * (A B), C'))
	ZIP(repeat(cross(var('A'), var('B')), 2), var('C'))
	>>> norm_ps_display(getSPF('A, (B+(C))*(((((D)))*E)+F), G1 (H1) * I'))
	ZIP(var('A'), cross(chain(var('B'), var('C')), chain(cross(var('D'), var('E')), var('F'))), cross(var('G1'), var('H1'), var('I')))

	>>> norm_ps_display(getSPF('A B, C1[D]'))
	ZIP(cross(var('A'), var('B')), lookup('C1', 'D'))
	>>> getSPF('A B, C2[D]')
	switch(cross(var('A'), var('B')), 'C2', 'D')
	>>> norm_ps_display(getSPF('A B, C1[(D,E)]'))
	ZIP(cross(var('A'), var('B')), lookup('C1', key('D', 'E')))
	>>> norm_ps_display(getSPF('A B, (C1,X)[D]'))
	ZIP(cross(var('A'), var('B')), lookup('C1', 'D'), lookup('X', 'D'))
	>>> norm_ps_display(getSPF('A B, (C,X)[(D,E)]'))
	ZIP(cross(var('A'), var('B')), var('C'), lookup('X', key('D', 'E')))
	>>> norm_ps_display(getSPF('A B, (C1,X)[(D,E)]'))
	ZIP(cross(var('A'), var('B')), lookup('C1', key('D', 'E')), lookup('X', key('D', 'E')))
	>>> norm_ps_display(getSPF('A, 2 B+C D*E+F, (G,H)[A]'))
	ZIP(var('A'), chain(repeat(var('B'), 2), cross(var('C'), var('D'), var('E')), var('F')), lookup('G', 'A'), lookup('H', 'A'))

	>>> testSPF('<csv>')
	3
	{'KEY1': '1', 'KEY2': '2', 'KEY3': '3'} {'KEY1': 'A', 'KEY2': 'B', 'KEY3': 'C'} {'KEY1': 'a', 'KEY2': 'b', 'KEY3': 'c'}

	>>> DS_setup()
	>>> testSPF('<data>')
	2
	{'!EVT': 10, '!FN': 'file1', 'SID': 0, '!SKIP': 0} {'!EVT': 10, '!FN': 'file2', 'SID': 1, '!SKIP': 0}
	>>> DS_clear()

	>>> testSPF('VAR', VAR = '1 2 3', VAR_repeat_idx_1 = 4)
	6
	{'VAR': '1'} {'VAR': '2'} {'VAR': '3'}
	{'VAR': '2'} {'VAR': '2'} {'VAR': '2'}

	>>> testSPF('VAR, FMT', VAR = '1 2 3', FMT = '%02d', FMT_type = 'format', FMT_source = 'VAR')
	3
	{'!FMT': '01', 'VAR': '1'} {'!FMT': '02', 'VAR': '2'} {'!FMT': '03', 'VAR': '3'}

	>>> testSPF('VAR, FMT', VAR = 'A B C', FMT = '%-2s', FMT_type = 'format', FMT_source = 'VAR')
	3
	{'!FMT': 'A ', 'VAR': 'A'} {'!FMT': 'B ', 'VAR': 'B'} {'!FMT': 'C ', 'VAR': 'C'}
	"""

def getBPF(config_dict = None):
	class X:
		config_section_list = ['task']
	for section_key in (config_dict or {}):
		for opt_key in config_dict[section_key]:
			config_dict[section_key][opt_key] = display_ps_str2ps_str(str(config_dict[section_key][opt_key]))
	config = create_config(config_dict = config_dict or {}).change_view(view_class = 'TaggedConfigView', setClasses = [X()])
	pf = config.get_plugin('internal parameter factory', 'BasicParameterFactory', cls = ParameterFactory)
	return pf.get_psrc(global_repo)

def testBPF(config_dict = None):
	return testPF(getBPF(config_dict), details = True)

class Test_BasicParameterFactory:
	"""
	>>> testBPF()
	None
	Keys = JOB_RANDOM, SEED_0, SEED_1, SEED_2, SEED_3, SEED_4, SEED_5, SEED_6, SEED_7, SEED_8, SEED_9, GC_JOB_ID, GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!JOB_RANDOM': 42, '!SEED_0': 43, '!SEED_1': 43, '!SEED_2': 43, '!SEED_3': 43, '!SEED_4': 43, '!SEED_5': 43, '!SEED_6': 43, '!SEED_7': 43, '!SEED_8': 43, '!SEED_9': 43, '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!JOB_RANDOM': 42, '!SEED_0': 53, '!SEED_1': 53, '!SEED_2': 53, '!SEED_3': 53, '!SEED_4': 53, '!SEED_5': 53, '!SEED_6': 53, '!SEED_7': 53, '!SEED_8': 53, '!SEED_9': 53, '!GC_JOB_ID': 11, '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> testBPF({'jobs': {'seeds': '611'}})
	None
	Keys = JOB_RANDOM, SEED_0, GC_JOB_ID, GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!JOB_RANDOM': 42, '!SEED_0': 612, '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!JOB_RANDOM': 42, '!SEED_0': 622, '!GC_JOB_ID': 11, '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> DS_setup()
	>>> testBPF({'jobs': {'seeds': '611'}})
	2
	Keys = EVT, FN, JOB_RANDOM, SEED_0, SID [trk], SKIP, GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 10, '!FN': 'file1', '!JOB_RANDOM': 42, '!SEED_0': 611, 'SID': 0, '!SKIP': 0, '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 10, '!FN': 'file2', '!JOB_RANDOM': 42, '!SEED_0': 612, 'SID': 1, '!SKIP': 0, '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	redo: [] disable: [] size: False
	>>> DS_clear()
	"""

def testSubSpace(pexpr, factory, **kwargs):
	config_dict = {
		'parameters': {'parameters': pexpr, 'A': '1 2'},
		'ps1': {'parameters': "X Y", 'X': '4 5', 'Y': '8 9'},
		'ps2': {'parameters': "cross(var('M'), var('N'))", 'M': 'a b', 'N': 'x y'},
		'ps3': {'parameters': "X Y", 'X': '0', 'Y': '8'},
		'ps4': {'parameters': "X Y", 'X': '1', 'Y': '9'},
	}
	for entry in kwargs:
		config_dict[entry].update(kwargs[entry])
	config = create_config(config_dict=config_dict).change_view(
		view_class = 'SimpleConfigView', setSections = ['parameters'])
	pf = ParameterFactory.create_instance(factory, config)
	return pf.get_psrc(global_repo)

class Test_Subspace:
	"""
	>>> ps = testSubSpace("cross(var('A'), pspace('ps1'), pspace('ps2', 'modular'))", 'ModularParameterFactory')
	>>> ps
	cross(var('A'), pspace('ps1'), pspace('ps2', 'ModularParameterFactory'))

	>>> ps = testSubSpace("A {ps1} {modular:ps2}", 'SimpleParameterFactory')
	>>> ps
	cross(var('A'), pspace('ps1'), pspace('ps2', 'ModularParameterFactory'))

	>>> testPF(ps, details = True)
	32
	Keys = A [trk], M [trk], N [trk], X [trk], Y [trk], GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'x', 'X': '4', 'Y': '8', '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'x', 'X': '4', 'Y': '8', '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'x', 'X': '5', 'Y': '8', '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'x', 'X': '5', 'Y': '8', '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'x', 'X': '4', 'Y': '9', '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'x', 'X': '4', 'Y': '9', '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'x', 'X': '5', 'Y': '9', '!GC_JOB_ID': 6, '!GC_PARAM': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'x', 'X': '5', 'Y': '9', '!GC_JOB_ID': 7, '!GC_PARAM': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'x', 'X': '4', 'Y': '8', '!GC_JOB_ID': 8, '!GC_PARAM': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'x', 'X': '4', 'Y': '8', '!GC_JOB_ID': 9, '!GC_PARAM': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'x', 'X': '5', 'Y': '8', '!GC_JOB_ID': 10, '!GC_PARAM': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'x', 'X': '5', 'Y': '8', '!GC_JOB_ID': 11, '!GC_PARAM': 11}
	12 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'x', 'X': '4', 'Y': '9', '!GC_JOB_ID': 12, '!GC_PARAM': 12}
	13 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'x', 'X': '4', 'Y': '9', '!GC_JOB_ID': 13, '!GC_PARAM': 13}
	14 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'x', 'X': '5', 'Y': '9', '!GC_JOB_ID': 14, '!GC_PARAM': 14}
	15 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'x', 'X': '5', 'Y': '9', '!GC_JOB_ID': 15, '!GC_PARAM': 15}
	16 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'y', 'X': '4', 'Y': '8', '!GC_JOB_ID': 16, '!GC_PARAM': 16}
	17 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'y', 'X': '4', 'Y': '8', '!GC_JOB_ID': 17, '!GC_PARAM': 17}
	18 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'y', 'X': '5', 'Y': '8', '!GC_JOB_ID': 18, '!GC_PARAM': 18}
	19 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'y', 'X': '5', 'Y': '8', '!GC_JOB_ID': 19, '!GC_PARAM': 19}
	20 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'y', 'X': '4', 'Y': '9', '!GC_JOB_ID': 20, '!GC_PARAM': 20}
	21 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'y', 'X': '4', 'Y': '9', '!GC_JOB_ID': 21, '!GC_PARAM': 21}
	22 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'a', 'N': 'y', 'X': '5', 'Y': '9', '!GC_JOB_ID': 22, '!GC_PARAM': 22}
	23 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'a', 'N': 'y', 'X': '5', 'Y': '9', '!GC_JOB_ID': 23, '!GC_PARAM': 23}
	24 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'y', 'X': '4', 'Y': '8', '!GC_JOB_ID': 24, '!GC_PARAM': 24}
	25 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'y', 'X': '4', 'Y': '8', '!GC_JOB_ID': 25, '!GC_PARAM': 25}
	26 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'y', 'X': '5', 'Y': '8', '!GC_JOB_ID': 26, '!GC_PARAM': 26}
	27 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'y', 'X': '5', 'Y': '8', '!GC_JOB_ID': 27, '!GC_PARAM': 27}
	28 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'y', 'X': '4', 'Y': '9', '!GC_JOB_ID': 28, '!GC_PARAM': 28}
	29 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'y', 'X': '4', 'Y': '9', '!GC_JOB_ID': 29, '!GC_PARAM': 29}
	30 {'<ACTIVE>': True, '<REQS>': [], 'A': '1', 'M': 'b', 'N': 'y', 'X': '5', 'Y': '9', '!GC_JOB_ID': 30, '!GC_PARAM': 30}
	31 {'<ACTIVE>': True, '<REQS>': [], 'A': '2', 'M': 'b', 'N': 'y', 'X': '5', 'Y': '9', '!GC_JOB_ID': 31, '!GC_PARAM': 31}
	redo: [] disable: [] size: False

	>>> ps = testSubSpace("A ({ps1} + 1*{ps3}) {modular:ps2}", 'SimpleParameterFactory')
	>>> (ps.get_parameter_len(), ps)
	(40, cross(var('A'), chain(pspace('ps1'), truncate(pspace('ps3'), 1)), pspace('ps2', 'ModularParameterFactory')))

	>>> ps = testSubSpace("{ps1} + {ps3} + {ps4}", 'SimpleParameterFactory')
	>>> (ps.get_parameter_len(), ps)
	(6, chain(pspace('ps1'), truncate(pspace('ps3'), 1), truncate(pspace('ps4'), 1)))
	"""


def testLookup(spf_expr, mpf_expr, bpf_info):
	lookup_config = {
		'A': 'a11 a12 a21 a22 a31 A00',
		'B': 'b11 b21 b22 b31 b41 B00',
		'LA': 'A! \n a1 => A1 \n a2 => A2 \n a3 => A3',
		'LA matcher': 'regex',
		'LB': 'B! \n b1 => B1 \n b2 => B2 \n b3 => B3',
		'LB matcher': 'regex',
		'LC': 'C! \n (a1, b1) => C1 \n (a2, b2) => C2 \n (a3, b3) => C3',
		'LC matcher': 'regex',
	}
	spf = getSPF(spf_expr, **lookup_config)
	mpf = getMPF(mpf_expr, **lookup_config)
	assert(repr(spf) == repr(mpf))
	(bpf_spf_expr, bpf_mpf_expr, bpf_const, bpf_other) = bpf_info
	def getLocalBPF(pexpr, pf):
		tconf = {'constants': bpf_const, 'parameter factory': pf, 'nseeds': 0, 'random variables': '',
			'translate requirements': False}
		pconf = {'parameters': pexpr}
		pconf.update(lookup_config)
		pconf.update(bpf_other)
		config_dict = {'task': tconf, 'parameters': pconf}
		return getBPF(config_dict)
	bpf_spf = getLocalBPF(bpf_spf_expr, 'SimpleParameterFactory')
	bpf_mpf = getLocalBPF(display_ps_str2ps_str(bpf_mpf_expr), 'ModularParameterFactory')
	assert(repr(mpf) == repr(spf))
	assert(repr(mpf) == repr(bpf_mpf))
	assert(repr(mpf) == repr(bpf_spf))
	def getVars(ps):
		pa = ParameterAdapter.create_instance('BasicParameterAdapter', create_config(), ps)
		for jobnum in irange(pa.get_job_len()):
			yield pa.get_job_content(jobnum)
	for (v_mpf, v_spf, v_bpf_mpf, v_bpf_spf) in izip(*imap(getVars, [mpf, spf, bpf_mpf, bpf_spf])):
		assert(v_mpf == v_spf)
		assert(v_mpf == v_bpf_mpf)
		assert(v_mpf == v_bpf_spf)
	norm_ps_display(spf)
	testPF(mpf, details = True)

class Test_Lookups:
	"""
	>>> testLookup(
	... 'A * LA[A]',
	... "ZIP(var('A'), lookup('LA', 'A'))",
	... ('A', "var('A')", 'LA', {'LA lookup': 'A'}))
	ZIP(var('A'), lookup('LA', 'A'))
	6
	Keys = A [trk], LA [trk], GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a11', 'LA': 'A1', '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a12', 'LA': 'A1', '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a21', 'LA': 'A2', '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a22', 'LA': 'A2', '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a31', 'LA': 'A3', '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A00', 'LA': 'A!', '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [] disable: [] size: False

	>>> testLookup(
	... '(A, B) LA[A] LB[B] LC[(A, B)]',
	... "ZIP(var('A'), var('B'), lookup('LA', 'A'), lookup(key('LB'), key('B')), lookup(key('LC'), key('A', 'B')))",
	... ('(A, B)', "ZIP(var('A'), var('B'))", 'LA LB LC', {'LA lookup': 'A', 'LB lookup': 'B', 'LC lookup': '(A,B)'}))
	ZIP(var('A'), var('B'), lookup('LA', 'A'), lookup('LB', 'B'), lookup('LC', key('A', 'B')))
	6
	Keys = A [trk], B [trk], LA [trk], LB [trk], LC [trk], GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a11', 'B': 'b11', 'LA': 'A1', 'LB': 'B1', 'LC': 'C1', '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a12', 'B': 'b21', 'LA': 'A1', 'LB': 'B2', 'LC': 'C!', '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a21', 'B': 'b22', 'LA': 'A2', 'LB': 'B2', 'LC': 'C2', '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a22', 'B': 'b31', 'LA': 'A2', 'LB': 'B3', 'LC': 'C!', '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a31', 'B': 'b41', 'LA': 'A3', 'LB': 'B!', 'LC': 'C!', '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A00', 'B': 'B00', 'LA': 'A!', 'LB': 'B!', 'LC': 'C!', '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [] disable: [] size: False

	>>> testLookup(
	... '(A, B) LA[A] LB[B] LC[(A, B)]',
	... "ZIP(var('A'), var('B'), lookup('LA', 'A'), lookup('LB', 'B'), lookup('LC', 'A B'))",
	... ('(A, B)', "ZIP(var('A'), var('B'))", 'LA LB LC', {'LA lookup': 'A', 'LB lookup': 'B', 'LC lookup': '(A,B)'}))
	ZIP(var('A'), var('B'), lookup('LA', 'A'), lookup('LB', 'B'), lookup('LC', key('A', 'B')))
	6
	Keys = A [trk], B [trk], LA [trk], LB [trk], LC [trk], GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a11', 'B': 'b11', 'LA': 'A1', 'LB': 'B1', 'LC': 'C1', '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a12', 'B': 'b21', 'LA': 'A1', 'LB': 'B2', 'LC': 'C!', '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a21', 'B': 'b22', 'LA': 'A2', 'LB': 'B2', 'LC': 'C2', '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a22', 'B': 'b31', 'LA': 'A2', 'LB': 'B3', 'LC': 'C!', '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 'a31', 'B': 'b41', 'LA': 'A3', 'LB': 'B!', 'LC': 'C!', '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A00', 'B': 'B00', 'LA': 'A!', 'LB': 'B!', 'LC': 'C!', '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [] disable: [] size: False
	"""

def testScope(config_dict, use = False, show_hash = False):
	config_dict.setdefault('task', {})
	config_dict['task'].setdefault('nseeds', 0)
	config_dict['task'].setdefault('translate requirements', 'False')
	config_dict['task'].setdefault('random variables', '')
	source = getBPF(config_dict)
	if show_hash:
		print(source.get_psrc_hash())
	print(ps_str2display_ps_str(str.join('\n', source.show_psrc()).replace('\t', '  ')))
	if use:
		testPF(source)

class Test_Scope:
	"""
	>>> testScope({'parameters': {'parameters': 'A'}, 'task': {'A': '1 2 3'}})
	SimpleParameterSource: var = A, len = 3
	>>> testScope({'parameters': {'parameters': 'A', 'A': '1 2'}, 'task': {'A': '1 2 3'}})
	SimpleParameterSource: var = A, len = 2
	>>> testScope({'parameters': {'parameters': 'A', 'A': '1 2'}})
	SimpleParameterSource: var = A, len = 2
	>>> testScope({'parameters': {'parameters': 'A B[A]', 'A': '1 2', 'B': 'X\\n 1 => Y'}})
	ZipLongParameterSource:
	  SimpleParameterSource: var = A, len = 2
	  SimpleLookupParameterSource: var = B, lookup = 'A'
	>>> try_catch(lambda: testScope({'parameters': {'parameters': 'A {p1}'}, 'p1': {'parameters': '!'}, 'task': {'A': '1 2 3'}}), 'ParameterError', 'Unable to create subspace')
	caught
	>>> testScope({'parameters': {'parameters': '{p1}', 'A': '1 2'}, 'p1': {'parameters': 'A', 'A': '1 2 3 4'}, 'task': {'A': '1 2 3'}})
	SubSpaceParameterSource: name = p1, factory = SimpleParameterFactory
	  SimpleParameterSource: var = A, len = 4
	>>> testScope({'parameters': {'parameters': 'A+{p1}+{p2}', 'A': '1 2'}, 'p1': {'parameters': 'A', 'A': '1 2 3 4'}, 'p2': {'parameters': 'A', 'A': '1 2 3 4 5'}, 'task': {'A': '1 2 3'}})
	ChainParameterSource:
	  SimpleParameterSource: var = A, len = 2
	  SubSpaceParameterSource: name = p1, factory = SimpleParameterFactory
	    SimpleParameterSource: var = A, len = 4
	  SubSpaceParameterSource: name = p2, factory = SimpleParameterFactory
	    SimpleParameterSource: var = A, len = 5
	>>> testScope({'parameters': {'parameters': '2*A+{p1}+{p2}', 'A': '1 2'}, 'p1': {'parameters': 'A', 'A': '1 2 3 4'}, 'p2': {'parameters': 'A'}, 'task': {'A': '1 2 3'}})
	ChainParameterSource:
	  RepeatParameterSource: count = 2
	    SimpleParameterSource: var = A, len = 2
	  SubSpaceParameterSource: name = p1, factory = SimpleParameterFactory
	    SimpleParameterSource: var = A, len = 4
	  SubSpaceParameterSource: name = p2, factory = SimpleParameterFactory
	    SimpleParameterSource: var = A, len = 2
	>>> testScope({'parameters': {'parameters': 'A+{p1}+{p2}'}, 'p1': {'parameters': 'A'}, 'p2': {'parameters': 'A'}, 'task': {'A': '1 2 3'}})
	ChainParameterSource:
	  SimpleParameterSource: var = A, len = 3
	  SubSpaceParameterSource: name = p1, factory = SimpleParameterFactory
	    SimpleParameterSource: var = A, len = 3
	  SubSpaceParameterSource: name = p2, factory = SimpleParameterFactory
	    SimpleParameterSource: var = A, len = 3

	>>> testScope({'parameters': {'parameters': '{modular p1}', 'A': '1 2'}, 'p1': {'parameters': 'ZIP(var("A"), transform("B", "10 - A"), format("C", "%02d", "B", 0))', 'A': '1 2 3 4'}, 'task': {'A': '1 2 3'}}, show_hash = True, use = True)
	a2f7f68f8cade27905b5de556fe7fba9
	SubSpaceParameterSource: name = p1, factory = ModularParameterFactory
	  ZipLongParameterSource:
	    SimpleParameterSource: var = A, len = 4
	    TransformParameterSource: var = B, expr = '10 - A', default = ''
	    FormatterParameterSource: var = C, fmt = '%02d', source = B, default = 0
	4
	{'A': '1', '!B': 9, '!C': '09'} {'A': '2', '!B': 8, '!C': '08'} {'A': '3', '!B': 7, '!C': '07'}
	{'A': '4', '!B': 6, '!C': '06'}

	>>> testScope({'parameters': {'parameters': '{modular p1}', 'A': '1 2'}, 'p1': {'parameters': 'RANGE(var("A"), 1)', 'A': '1 2 3 4'}, 'task': {'A': '1 2 3'}}, use = True)
	SubSpaceParameterSource: name = p1, factory = ModularParameterFactory
	  RangeParameterSource: RANGE = (1, 3)
	    SimpleParameterSource: var = A, len = 4
	3
	{'A': '2'} {'A': '3'} {'A': '4'}
	"""

run_test()
