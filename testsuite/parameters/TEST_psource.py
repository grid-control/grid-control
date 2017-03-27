#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import random
from testfwk import create_config, run_test, try_catch
from grid_control import utils
from grid_control.config import Matcher
from grid_control.parameters import ParameterMetadata, ParameterSource
from grid_control.parameters.padapter import ParameterAdapter
from grid_control.parameters.psource_file import GCDumpParameterSource
from testDS import ss2bl
from testINC import TestParameterSource, norm_ps_display, testPS, updateDS
from python_compat import lmap, lrange, set


def display_ps(ps):
	norm_ps_display(ps)
	print(ps.get_psrc_hash() or '<no hash>')

def create_matcher(name):
	return Matcher.create_instance(name, create_config(), 'test')

random.randint = lambda *args: 42 # 100% randomly choosen

class Test_ParameterSource:
	"""
	>>> try_catch(lambda: ParameterSource().fill_parameter_metadata([]), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: ParameterSource().fill_parameter_content(0, {}), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(ParameterSource().get_psrc_hash, 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_BasicParameterSource:
	"""
	>>> ps = ParameterSource.create_instance('InternalParameterSource', [{'KEY': 1}, {'KEY': 2}, {'KEY': 3}], lmap(ParameterMetadata, ['KEY']))
	>>> tmp = []
	>>> ps.fill_parameter_metadata(tmp)
	>>> tmp
	[KEY]

	>>> testPS(ps)
	<internal:KEY=847b5e94811319b47889a38148027369>
	847b5e94811319b47889a38148027369
	3
	Keys = KEY [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 3, '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('SimpleParameterSource', 'KEY', [1, 2, 3]))
	var('KEY')
	c6f4f19eaaf7b0f08d86271a324af317
	3
	Keys = KEY [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 3, '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('SimpleParameterSource', '!KEY', [1, 2, 3]))
	var('!KEY')
	a922d58cb7574b45a50aa1aaffe116d4
	3
	Keys = KEY, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!KEY': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], '!KEY': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], '!KEY': 3, '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('ConstParameterSource', 'KEY', 'VALUE'))
	const('KEY', 'VALUE')
	46a0333cfded1743aa4d9b817f31a780
	None
	Keys = KEY [trk], GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 'VALUE', '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], 'KEY': 'VALUE', '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('ConstParameterSource', '!KEY', 'VALUE'))
	const('!KEY', 'VALUE')
	acf2f6ac25c6b742bace44eace293f91
	None
	Keys = KEY, GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!KEY': 'VALUE', '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!KEY': 'VALUE', '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> ps = ParameterSource.create_instance('SimpleLookupParameterSource', 'KEY', ('SRC',), None, {}, [])
	>>> testPS(ps)
	lookup('KEY', 'SRC')
	3242a24ef00d71aecdad0cbe72545a7b
	None
	Keys = KEY [trk], GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> random.seed(0)
	>>> ps = ParameterSource.create_instance('RNGParameterSource', 'RNG', 1, 9)
	>>> testPS(ps)
	rng('!RNG')
	dbfaa786e44d2dc9576c221a8647d557
	None
	Keys = RNG, GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!RNG': 42, '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!RNG': 42, '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> ps = ParameterSource.create_instance('CounterParameterSource', 'C', 1111)
	>>> testPS(ps)
	counter('!C', 1111)
	18d0b5245c0400f0158f6132342779dc
	None
	Keys = C, GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!C': 1112, '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!C': 1122, '!GC_PARAM': 11}
	redo: [] disable: [] size: False

	>>> p1 = ParameterSource.create_instance('InternalParameterSource',
	... [{'CPUTIME': '10:00'}, {'MEMORY': '2000'}, {'WALLTIME': '0:00:10'}], [])
	>>> pr = ParameterSource.create_instance('RequirementParameterSource')
	>>> testPS(ParameterSource.create_instance('ZipLongParameterSource', p1, pr))
	ZIP(<internal:=8696dee8347feb49bad12e44d9e18d0f>, req())
	0decad426c0e4575aa538d3b4b0f6229
	3
	Keys = GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [('CPUTIME', 36000)], '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [('MEMORY', 2000)], '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [('WALLTIME', 10)], '!GC_PARAM': 2}
	redo: [] disable: [] size: False
	"""

class Test_LookupParameterSources:
	"""
	>>> p1 = TestParameterSource('KEY1', ['AX', 'AY', 'AZ', 'BX', 'BY'], (set([1, 4]), set([2, 3]), False))
	>>> p2 = ParameterSource.create_instance('SimpleParameterSource', 'KEY2', ['10', '15', '20', '25', '30'])

	>>> pl = ParameterSource.create_instance('SimpleLookupParameterSource', 'LOOKUP', ('KEY1',),
	... [create_matcher('equal')],
	... {('AX',): ['511'], ('A',): ['811'], ('B',): ['987']}, [('AX',), ('A',), ('B',)])
	>>> testPS(ParameterSource.create_instance('ZipLongParameterSource', p1, pl), showHash = False)
	ZIP(var('KEY1'), lookup('LOOKUP', 'KEY1'))
	5
	Keys = KEY1 [trk], LOOKUP [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'LOOKUP': '511', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', '!GC_PARAM': 4}
	redo: [1, 4] disable: [2, 3] size: False

	>>> pl = ParameterSource.create_instance('SimpleLookupParameterSource', 'LOOKUP', ('KEY1',),
	... [create_matcher('start')],
	... {('AX',): ['511'], ('A',): ['811'], ('B',): ['987']}, [('AX',), ('A',), ('B',)])
	>>> testPS(ParameterSource.create_instance('ZipLongParameterSource', p1, pl), showHash = False)
	ZIP(var('KEY1'), lookup('LOOKUP', 'KEY1'))
	5
	Keys = KEY1 [trk], LOOKUP [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'LOOKUP': '511', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'LOOKUP': '811', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'LOOKUP': '811', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'LOOKUP': '987', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'LOOKUP': '987', '!GC_PARAM': 4}
	redo: [1, 4] disable: [2, 3] size: False

	>>> pl = ParameterSource.create_instance('SimpleLookupParameterSource', 'LOOKUP', ('KEY1', 'KEY2'),
	... [create_matcher('start'), create_matcher('equal')],
	... {('AX',): ['511'], ('A', '15'): ['811'], ('B', '25'): ['987']}, [('AX',), ('A', '15'), ('B', '25')])
	>>> testPS(ParameterSource.create_instance('ZipLongParameterSource', p1, p2, pl), showHash = False)
	ZIP(var('KEY1'), var('KEY2'), lookup('LOOKUP', key('KEY1', 'KEY2')))
	5
	Keys = KEY1 [trk], KEY2 [trk], LOOKUP [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'KEY2': '10', 'LOOKUP': '511', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'KEY2': '15', 'LOOKUP': '811', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'KEY2': '20', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'KEY2': '25', 'LOOKUP': '987', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'KEY2': '30', '!GC_PARAM': 4}
	redo: [1, 4] disable: [2, 3] size: False

	>>> p1a = TestParameterSource('KEY1', ['AX', 'AY', 'AZ', 'BX', 'BY'], (set([1, 3]), set([2]), False))
	>>> p2a = TestParameterSource('KEY2', ['10', '15', '20', '25', '30'], (set([0]), set([1, 2]), False))
	>>> testPS(ParameterSource.create_instance('CrossParameterSource', p1a, p2a, pl), showHash = False)
	cross(var('KEY1'), var('KEY2'), lookup('LOOKUP', key('KEY1', 'KEY2')))
	25
	Keys = KEY1 [trk], KEY2 [trk], LOOKUP [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'KEY2': '10', 'LOOKUP': '511', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'KEY2': '10', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'KEY2': '10', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'KEY2': '10', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'KEY2': '10', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'KEY2': '15', 'LOOKUP': '511', '!GC_PARAM': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'KEY2': '15', 'LOOKUP': '811', '!GC_PARAM': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'KEY2': '15', 'LOOKUP': '811', '!GC_PARAM': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'KEY2': '15', '!GC_PARAM': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'KEY2': '15', '!GC_PARAM': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'KEY2': '20', 'LOOKUP': '511', '!GC_PARAM': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'KEY2': '20', '!GC_PARAM': 11}
	12 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'KEY2': '20', '!GC_PARAM': 12}
	13 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'KEY2': '20', '!GC_PARAM': 13}
	14 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'KEY2': '20', '!GC_PARAM': 14}
	15 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'KEY2': '25', 'LOOKUP': '511', '!GC_PARAM': 15}
	16 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'KEY2': '25', '!GC_PARAM': 16}
	17 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'KEY2': '25', '!GC_PARAM': 17}
	18 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'KEY2': '25', 'LOOKUP': '987', '!GC_PARAM': 18}
	19 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'KEY2': '25', 'LOOKUP': '987', '!GC_PARAM': 19}
	20 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'KEY2': '30', 'LOOKUP': '511', '!GC_PARAM': 20}
	21 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'KEY2': '30', '!GC_PARAM': 21}
	22 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'KEY2': '30', '!GC_PARAM': 22}
	23 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BX', 'KEY2': '30', '!GC_PARAM': 23}
	24 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'KEY2': '30', '!GC_PARAM': 24}
	redo: [0, 1, 2, 3, 4, 6, 8, 11, 13, 16, 18, 21, 23] disable: [2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 22] size: False

	>>> pl = ParameterSource.create_instance('SwitchingLookupParameterSource', p1a, 'LOOKUP', ('KEY1', 'KEY2'),
	... [create_matcher('start')],
	... {('AX',): ['511', '235'], ('A',): ['811'], ('BY',): ['987', '634', '374']}, [('AX',), ('A',), ('BY',)])
	>>> testPS(pl, showHash = False)
	switch(var('KEY1'), 'LOOKUP', key('KEY1', 'KEY2'))
	7
	Keys = KEY1 [trk], LOOKUP [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'LOOKUP': '511', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AX', 'LOOKUP': '235', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AY', 'LOOKUP': '811', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'AZ', 'LOOKUP': '811', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'LOOKUP': '987', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'LOOKUP': '634', '!GC_PARAM': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'BY', 'LOOKUP': '374', '!GC_PARAM': 6}
	redo: [2] disable: [3] size: False
	"""

class Test_MultiParameterSource:
	"""
	>>> p0 = ParameterSource.create_instance('NullParameterSource')
	>>> p0.get_psrc_hash()
	''
	>>> try_catch(lambda: ParameterSource.create_instance('MultiParameterSource', p0, p0, p0), 'AbstractError')

	>>> p1 = TestParameterSource('A', [1, 2, 3], (set([1]), set([2]), False))
	>>> p2 = TestParameterSource('B', ['M', 'N'], (set([0]), set([1]), False))
	>>> p1s = ParameterSource.create_instance('SimpleParameterSource', 'A', [1, 2, 3])
	>>> p2s = ParameterSource.create_instance('SimpleParameterSource', 'B', ['M', 'N'])
	>>> p3 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['x', 'y', 'z'])
	>>> p4 = ParameterSource.create_instance('CounterParameterSource', 'X', 100)
	>>> p5 = ParameterSource.create_instance('CounterParameterSource', 'Y', 900)
	>>> p6 = ParameterSource.create_instance('CounterParameterSource', 'Z', 400)
	>>> p7 = ParameterSource.create_instance('SimpleParameterSource', 'M', [1])
	>>> p8 = ParameterSource.create_instance('ConstParameterSource', 'N', 9)
	>>> p9 = TestParameterSource('KEY1', lrange(10), (set([1, 3]), set([2]), False))

	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p0, 0))
	null()
	<no hash>
	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p2s, 0))
	null()
	<no hash>
	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p4, 0))
	null()
	<no hash>

	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p0, 1))
	truncate(null(), 1)
	c4ca4238a0b923820dcc509a6f75849b
	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p2s, 1))
	var('B')
	64f5d53e025b3362da3ecbf9e9bfec29
	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p4, 1))
	truncate(counter('!X', 100), 1)
	6e12670768fffadb02edd573cc86d166

	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p0, 3))
	truncate(null(), 3)
	eccbc87e4b5ce2fe28308fd9f2a7baf3
	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p2s, 3))
	repeat(var('B'), 3)
	d1c46e7f5cbdd1e15e30ffca602c9d0f
	>>> display_ps(ParameterSource.create_instance('RepeatParameterSource', p4, 3))
	truncate(counter('!X', 100), 3)
	6b59f943464ccd388684be992a6db84e

	>>> display_ps(ParameterSource.create_instance('ZipLongParameterSource', p0, p1s, p0))
	var('A')
	45ae8703f1816bb53ff2758e5508c8a5
	>>> display_ps(ParameterSource.create_instance('ZipLongParameterSource', p0, p1s, p2s, p0))
	ZIP(var('A'), var('B'))
	004120bba7c2ab39774cdeeb51bb9519
	>>> display_ps(ParameterSource.create_instance('ZipLongParameterSource', p0, p1s, p0, p2s, p3, p0))
	ZIP(var('A'), var('B'), var('C'))
	3fbd1afb894d4cb2c160f9b80f55da0b

	>>> pz1 = ParameterSource.create_instance('ZipLongParameterSource', p0, p1s, p0)
	>>> pz2 = ParameterSource.create_instance('ZipLongParameterSource', p2s, p3)
	>>> display_ps(ParameterSource.create_instance('ZipLongParameterSource', pz1, pz2))
	ZIP(var('A'), var('B'), var('C'))
	3fbd1afb894d4cb2c160f9b80f55da0b

	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', p1s, p2s, p3))
	cross(var('A'), var('B'), var('C'))
	d1c56034fcacb8308155e2ac7597f866
	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', p0, p1s, p0, p2s, p3, p0))
	cross(var('A'), var('B'), var('C'))
	d1c56034fcacb8308155e2ac7597f866

	>>> pc1 = ParameterSource.create_instance('CrossParameterSource', p0, p1s, p0)
	>>> pc2 = ParameterSource.create_instance('CrossParameterSource', p2s, p3)
	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', pc1, pc2))
	cross(var('A'), var('B'), var('C'))
	d1c56034fcacb8308155e2ac7597f866

	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', p4, p0))
	counter('!X', 100)
	ad0d2ee480f9f53f2aa556749be998eb
	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', p1s, p4, p5, p0))
	ZIP(var('A'), counter('!X', 100), counter('!Y', 900))
	72029ba9f84ae0064f6bb7bd2221e1c4
	>>> display_ps(ParameterSource.create_instance('ZipLongParameterSource', p1s, p4, p0, p5))
	ZIP(var('A'), counter('!X', 100), counter('!Y', 900))
	72029ba9f84ae0064f6bb7bd2221e1c4

	>>> pcs1 = ParameterSource.create_instance('CrossParameterSource', p1s, p4, p0)
	>>> pcs2 = ParameterSource.create_instance('CrossParameterSource', p5, p0, p6)
	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', pcs1, pcs2))
	ZIP(var('A'), counter('!X', 100), counter('!Y', 900), counter('!Z', 400))
	c1d300c496c89c99a5af5e4ffc0725a8

	>>> pcsA = ParameterSource.create_instance('CrossParameterSource', p1s, p2s, p4)
	>>> pcsB = ParameterSource.create_instance('CrossParameterSource', p5, p6)
	>>> display_ps(ParameterSource.create_instance('CrossParameterSource', pcsA, pcsB))
	ZIP(cross(var('A'), var('B'), counter('!X', 100)), counter('!Y', 900), counter('!Z', 400))
	c58e053657b5729005f40584bf01fd58

	>>> testPS(ParameterSource.create_instance('ZipShortParameterSource', p1, p2, p3), showHash = False)
	sZIP(var('A'), var('B'), var('C'))
	2
	Keys = A [trk], B [trk], C [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'x', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'y', '!GC_PARAM': 1}
	redo: [0, 1] disable: [1, 2] size: False

	>>> testPS(ParameterSource.create_instance('ZipShortParameterSource', p1, p2, p3, p4, p5), showHash = False)
	sZIP(var('A'), var('B'), var('C'), counter('!X', 100), counter('!Y', 900))
	2
	Keys = A [trk], B [trk], C [trk], X, Y, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'x', '!X': 100, '!Y': 900, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'y', '!X': 101, '!Y': 901, '!GC_PARAM': 1}
	redo: [0, 1] disable: [1, 2] size: False

	>>> testPS(ParameterSource.create_instance('ZipLongParameterSource', p1, p2, p3), showHash = False)
	ZIP(var('A'), var('B'), var('C'))
	3
	Keys = A [trk], B [trk], C [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'x', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'y', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'C': 'z', '!GC_PARAM': 2}
	redo: [0, 1] disable: [1, 2] size: False

	>>> testPS(ParameterSource.create_instance('ChainParameterSource', p1, p2, p3), showHash = False)
	chain(var('A'), var('B'), var('C'))
	8
	Keys = A [trk], B [trk], C [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'B': 'M', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'B': 'N', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'C': 'x', '!GC_PARAM': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'C': 'y', '!GC_PARAM': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'C': 'z', '!GC_PARAM': 7}
	redo: [1, 3] disable: [2, 4] size: False

	>>> testPS(ParameterSource.create_instance('RepeatParameterSource', p2, 3), showHash = False)
	repeat(var('B'), 3)
	6
	Keys = B [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'B': 'M', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'B': 'N', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'B': 'M', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'B': 'N', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'B': 'M', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'B': 'N', '!GC_PARAM': 5}
	redo: [0, 2, 4] disable: [1, 3, 5] size: False

	>>> testPS(ParameterSource.create_instance('CrossParameterSource', p1, p2), showHash = False)
	cross(var('A'), var('B'))
	6
	Keys = A [trk], B [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!GC_PARAM': 5}
	redo: [0, 1, 2, 4] disable: [2, 3, 4, 5] size: False

	>>> testPS(ParameterSource.create_instance('CrossParameterSource', p1, p2, p3), showHash = False)
	cross(var('A'), var('B'), var('C'))
	18
	Keys = A [trk], B [trk], C [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'x', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'x', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'x', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'x', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'x', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'x', '!GC_PARAM': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'y', '!GC_PARAM': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'y', '!GC_PARAM': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'y', '!GC_PARAM': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'y', '!GC_PARAM': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'y', '!GC_PARAM': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'y', '!GC_PARAM': 11}
	12 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'z', '!GC_PARAM': 12}
	13 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'z', '!GC_PARAM': 13}
	14 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'z', '!GC_PARAM': 14}
	15 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'z', '!GC_PARAM': 15}
	16 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'z', '!GC_PARAM': 16}
	17 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'z', '!GC_PARAM': 17}
	redo: [0, 1, 2, 4, 6, 7, 8, 10, 12, 13, 14, 16] disable: [2, 3, 4, 5, 8, 9, 10, 11, 14, 15, 16, 17] size: False

	>>> testPS(ParameterSource.create_instance('CrossParameterSource', p1, p5, p2), showHash = False)
	cross(var('A'), counter('!Y', 900), var('B'))
	6
	Keys = A [trk], B [trk], Y, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!Y': 900, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!Y': 901, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!Y': 902, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!Y': 903, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!Y': 904, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!Y': 905, '!GC_PARAM': 5}
	redo: [0, 1, 2, 4] disable: [2, 3, 4, 5] size: False

	>>> testPS(ParameterSource.create_instance('CrossParameterSource', p5, p1, p2), showHash = False)
	cross(counter('!Y', 900), var('A'), var('B'))
	6
	Keys = A [trk], B [trk], Y, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!Y': 900, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!Y': 901, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!Y': 902, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!Y': 903, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!Y': 904, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!Y': 905, '!GC_PARAM': 5}
	redo: [0, 1, 2, 4] disable: [2, 3, 4, 5] size: False

	>>> testPS(ParameterSource.create_instance('RangeParameterSource', p9, 5), showHash = False)
	RANGE(var('KEY1'), 5)
	5
	Keys = KEY1 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 5, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 6, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 7, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 8, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 9, '!GC_PARAM': 4}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('RangeParameterSource', p9, None, 2), showHash = False)
	RANGE(var('KEY1'), 0, 2)
	3
	Keys = KEY1 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 2, '!GC_PARAM': 2}
	redo: [1] disable: [2] size: False

	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p9, 1), showHash = False)
	truncate(var('KEY1'), 1)
	1
	Keys = KEY1 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 0, '!GC_PARAM': 0}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p9, 3), showHash = False)
	truncate(var('KEY1'), 3)
	3
	Keys = KEY1 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 2, '!GC_PARAM': 2}
	redo: [1] disable: [2] size: False

	>>> testPS(ParameterSource.create_instance('RangeParameterSource', p9, 1, 3), showHash = False)
	RANGE(var('KEY1'), 1, 3)
	3
	Keys = KEY1 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 3, '!GC_PARAM': 2}
	redo: [0, 2] disable: [1] size: False

	>>> px = TestParameterSource('A', [1, 2, 3], (set([1]), set([2]), False))
	>>> py = TestParameterSource('B', ['M', 'N'], (set([]), set([1]), False))
	>>> pz = TestParameterSource('C', ['x', 'y', 'z'], (set([2]), set([1]), False))
	>>> eps = ParameterSource.create_instance('ErrorParameterSource', px, py, pz)
	>>> testPS(eps, showHash = False)
	variation(var('A'), var('B'), var('C'))
	6
	Keys = A [trk], B [trk], C [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'x', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'x', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'x', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'x', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'y', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'z', '!GC_PARAM': 5}
	redo: [1, 5] disable: [2, 3, 4] size: False

	>>> pAt = ParameterSource.create_instance('SimpleParameterSource', 'A', [1, 2])
	>>> pAu = ParameterSource.create_instance('SimpleParameterSource', '!A', [1, 2])
	>>> pBt = ParameterSource.create_instance('SimpleParameterSource', 'B', [3])
	>>> testPS(ParameterSource.create_instance('ChainParameterSource', pAt, pBt, pAt))
	chain(var('A'), var('B'), var('A'))
	5db79e7fadb60f6b5d432f8a20da5738
	5
	Keys = A [trk], B [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'B': 3, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, '!GC_PARAM': 4}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p0, 0))
	null()
	<no hash>
	None
	Keys = GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 11}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p0, 1))
	truncate(null(), 1)
	c4ca4238a0b923820dcc509a6f75849b
	1
	Keys = GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 0}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p0, 5))
	truncate(null(), 5)
	e4da3b7fbbce2345d7772b0674a318d5
	5
	Keys = GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 4}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p1s, 0))
	null()
	<no hash>
	None
	Keys = GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 11}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p1s, 1))
	truncate(var('A'), 1)
	e3b0d98766f0ab986fc1e44592489d1e
	1
	Keys = A [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, '!GC_PARAM': 0}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p1s, 5))
	truncate(var('A'), 5)
	271bd40d99d083ef84051fe5c50a4620
	5
	Keys = A [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 4}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p4, 0))
	null()
	<no hash>
	None
	Keys = GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 11}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p4, 1))
	truncate(counter('!X', 100), 1)
	6e12670768fffadb02edd573cc86d166
	1
	Keys = X, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!X': 100, '!GC_PARAM': 0}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p4, 5))
	truncate(counter('!X', 100), 5)
	4a53ed033864e8b0f7c09aa125ec41a4
	5
	Keys = X, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!X': 100, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], '!X': 101, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], '!X': 102, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!X': 103, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!X': 104, '!GC_PARAM': 4}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p8, 0))
	null()
	<no hash>
	None
	Keys = GC_PARAM
	1 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 1}
	11 {'<ACTIVE>': True, '<REQS>': [], '!GC_PARAM': 11}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p8, 1))
	truncate(const('N', 9), 1)
	5e4c0cca19a55d506d80bc8b6aac824d
	1
	Keys = N [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'N': 9, '!GC_PARAM': 0}
	redo: [] disable: [] size: False
	>>> testPS(ParameterSource.create_instance('TruncateParameterSource', p8, 5))
	truncate(const('N', 9), 5)
	6c4d48f105d1638901fc0c3811e8f6d4
	5
	Keys = N [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'N': 9, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'N': 9, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'N': 9, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'N': 9, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'N': 9, '!GC_PARAM': 4}
	redo: [] disable: [] size: False

	>>> pLA1 = TestParameterSource('AIDX', [10, 11, 12, 13, 14], (set([]), set([]), False))
	>>> pLA2 = TestParameterSource('A', ['A', 'B', 'C', 'D', 'E'], (set([1]), set([3]), False))
	>>> pLA = ParameterSource.create_instance('ZipShortParameterSource', pLA1, pLA2)
	>>> testPS(pLA)
	sZIP(var('AIDX'), var('A'))
	a7059939e050e1f5a0eaa6b6db9ada35
	5
	Keys = A [trk], AIDX [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A', 'AIDX': 10, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 'B', 'AIDX': 11, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 'C', 'AIDX': 12, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 'D', 'AIDX': 13, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 'E', 'AIDX': 14, '!GC_PARAM': 4}
	redo: [1] disable: [3] size: False

	>>> pLB1 = TestParameterSource('BIDX', [20, 21, 22, 23, 24], (set([]), set([]), False))
	>>> pLB2 = TestParameterSource('B', ['M', 'N', 'A', 'E', 'A'], (set([0]), set([2]), False))
	>>> pLB = ParameterSource.create_instance('ZipShortParameterSource', pLB1, pLB2)
	>>> testPS(pLB)
	sZIP(var('BIDX'), var('B'))
	fa28e7a3a2badc8b241cd2d760fe182c
	5
	Keys = B [trk], BIDX [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'B': 'M', 'BIDX': 20, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'B': 'N', 'BIDX': 21, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'B': 'A', 'BIDX': 22, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'B': 'E', 'BIDX': 23, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'B': 'A', 'BIDX': 24, '!GC_PARAM': 4}
	redo: [0] disable: [2] size: False

	>>> testPS(ParameterSource.create_instance('CombineParameterSource', pLA, pLB, 'A', 'B'))
	combine(sZIP(var('AIDX'), var('A')), sZIP(var('BIDX'), var('B')))
	89f5eb1ee02891a9aa2441e2e3dd9a98
	3
	Keys = A [trk], AIDX [trk], B [trk], BIDX [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A', 'AIDX': 10, 'B': 'A', 'BIDX': 22, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A', 'AIDX': 10, 'B': 'A', 'BIDX': 24, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 'E', 'AIDX': 14, 'B': 'E', 'BIDX': 23, '!GC_PARAM': 2}
	redo: [] disable: [0] size: False

	>>> testPS(ParameterSource.create_instance('LinkParameterSource', pLA, pLB, 'A', 'B'))
	link(sZIP(var('AIDX'), var('A')), sZIP(var('BIDX'), var('B')))
	ce94b93b75c9773c6150c85a21757474
	2
	Keys = A [trk], AIDX [trk], B [trk], BIDX [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'A', 'AIDX': 10, 'B': 'A', 'BIDX': 22, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 'E', 'AIDX': 14, 'B': 'E', 'BIDX': 23, '!GC_PARAM': 1}
	redo: [] disable: [0] size: False

	>>> testPS(ParameterSource.create_instance('CombineParameterSource', p1, p2, 'A', 'B'))
	combine(var('A'), var('B'))
	19d48e81d495eda641b25f23e055e44c
	0
	Keys = A [trk], B [trk], GC_PARAM
	<no parameter space points>
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('CombineParameterSource', pLA, ParameterSource.create_instance('ConstParameterSource', 'B', 'C'), 'A', 'B'))
	combine(sZIP(var('AIDX'), var('A')), const('B', 'C'))
	3028bf3c7357728cb4983e268439703b
	1
	Keys = A [trk], AIDX [trk], B [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'C', 'AIDX': 12, 'B': 'C', '!GC_PARAM': 0}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('CombineParameterSource', ParameterSource.create_instance('ConstParameterSource', 'A', 'C'), ParameterSource.create_instance('ConstParameterSource', 'B', 'C'), 'A', 'B'))
	combine(const('A', 'C'), const('B', 'C'))
	90085596d2fc0b688d1882cec0aa0f45
	1
	Keys = A [trk], B [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 'C', 'B': 'C', '!GC_PARAM': 0}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('CombineParameterSource', ParameterSource.create_instance('ConstParameterSource', 'A', 'C'), ParameterSource.create_instance('ConstParameterSource', 'B', 'X'), 'A', 'B'))
	combine(const('A', 'C'), const('B', 'X'))
	24adfe86ff1bb38c9a3cb8866e6ead71
	0
	Keys = A [trk], B [trk], GC_PARAM
	<no parameter space points>
	redo: [] disable: [] size: False

	>>> try_catch(lambda: testPS(ParameterSource.create_instance('MultiParameterSource', pAt, pAu)), 'AbstractError', 'is an abstract function')
	caught

	>>> try_catch(lambda: testPS(ParameterSource.create_instance('CrossParameterSource', pAt, pAu)), 'ParameterError', 'Collisions of parameter')
	cross(var('A'), var('!A'))
	fd0fc43c2cc6ad40eb547dc732592646
	4
	caught
	>>> try_catch(lambda: testPS(ParameterSource.create_instance('ChainParameterSource', pAt, pBt, pAu)), 'ParameterError', 'Collisions of tracking status for parameter')
	chain(var('A'), var('B'), var('!A'))
	dc6c9553478efe652139751de610edb6
	5
	caught
	"""

class Test_FileParameterSources:
	"""
	>>> utils.remove_files(['param.saved'])

	>>> p1 = TestParameterSource('A', [1, 2, 3], (set([1]), set([2]), False))
	>>> p2 = TestParameterSource('B', ['M', 'N'], (set([0]), set([1]), False))
	>>> p3 = ParameterSource.create_instance('CounterParameterSource', 'X', 100)

	>>> ps = ParameterSource.create_instance('CrossParameterSource', p1, p2, p3)
	>>> testPS(ps, showHash = False)
	cross(var('A'), var('B'), counter('!X', 100))
	6
	Keys = A [trk], B [trk], X, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_PARAM': 5}
	redo: [0, 1, 2, 4] disable: [2, 3, 4, 5] size: False
	>>> tmp_pa = ParameterAdapter(create_config(), ps)
	>>> GCDumpParameterSource.write('param.saved', tmp_pa.get_job_len(), tmp_pa.get_job_metadata(), tmp_pa.iter_jobs())

	>>> pl = ParameterSource.create_instance('GCDumpParameterSource', 'param.saved')
	>>> testPS(pl, showHash = False, showPS = False)
	6
	Keys = A [trk], B [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!GC_PARAM': 5}
	redo: [] disable: [] size: False

	>>> utils.remove_files(['param.saved'])
	"""

class Test_DataParameterSourceReal:
	"""
	>>> utils.remove_files(['datacache.dat', 'datamap.tar'])
	>>> config = create_config(config_dict = {'global': {'workdir': '.', 'events per job': 10, 'dataset': '../datasets/dataE.dbs', 'dataset splitter': 'EventBoundarySplitter'}})
	>>> psrc = ParameterSource.create_instance('DataParameterSource', config, 'dataset', {}, keep_old=False)
	Block /MY/DATASET#easy3 is not available at any site!
	 * Dataset '../datasets/dataE.dbs':
	  contains 3 blocks with 9 files with 85 entries
	>>> testPS(psrc)
	data()
	4379d052246854b00b80807bca9a2b4b
	9
	Keys = DATASETBLOCK, DATASETNICK, DATASETPATH, DATASETSPLIT [trk], FILE_NAMES, MAX_EVENTS, SKIP_EVENTS, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [('STORAGE', ['SE4'])], '!DATASETBLOCK': 'easy1', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 0, '!FILE_NAMES': '/path/file0', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [('STORAGE', ['SE4'])], '!DATASETBLOCK': 'easy1', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 1, '!FILE_NAMES': '/path/file1 /path/file2', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [('STORAGE', ['SE4'])], '!DATASETBLOCK': 'easy1', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 2, '!FILE_NAMES': '/path/file2', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 5, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!DATASETBLOCK': 'easy2', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 3, '!FILE_NAMES': '/path/file3', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!DATASETBLOCK': 'easy2', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 4, '!FILE_NAMES': '/path/file5', '!MAX_EVENTS': 5, '!SKIP_EVENTS': 0, '!GC_PARAM': 4}
	5 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 5, '!FILE_NAMES': '/path/file6', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 5}
	6 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 6, '!FILE_NAMES': '/path/file7 /path/file8', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 6}
	7 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 7, '!FILE_NAMES': '/path/file8 /path/file9', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 5, '!GC_PARAM': 7}
	8 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 8, '!FILE_NAMES': '/path/file9', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 5, '!GC_PARAM': 8}
	redo: [] disable: [] size: False

	>>> psrc = ParameterSource.create_instance('DataParameterSource', config, 'dataset', {}, keep_old=False)
	>>> testPS(psrc)
	data()
	4379d052246854b00b80807bca9a2b4b
	9
	Keys = DATASETBLOCK, DATASETNICK, DATASETPATH, DATASETSPLIT [trk], FILE_NAMES, MAX_EVENTS, SKIP_EVENTS, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [('STORAGE', ['SE4'])], '!DATASETBLOCK': 'easy1', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 0, '!FILE_NAMES': '/path/file0', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [('STORAGE', ['SE4'])], '!DATASETBLOCK': 'easy1', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 1, '!FILE_NAMES': '/path/file1 /path/file2', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [('STORAGE', ['SE4'])], '!DATASETBLOCK': 'easy1', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 2, '!FILE_NAMES': '/path/file2', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 5, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!DATASETBLOCK': 'easy2', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 3, '!FILE_NAMES': '/path/file3', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!DATASETBLOCK': 'easy2', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 4, '!FILE_NAMES': '/path/file5', '!MAX_EVENTS': 5, '!SKIP_EVENTS': 0, '!GC_PARAM': 4}
	5 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 5, '!FILE_NAMES': '/path/file6', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 5}
	6 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 6, '!FILE_NAMES': '/path/file7 /path/file8', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 0, '!GC_PARAM': 6}
	7 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 7, '!FILE_NAMES': '/path/file8 /path/file9', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 5, '!GC_PARAM': 7}
	8 {'<ACTIVE>': False, '<REQS>': [('STORAGE', [])], '!DATASETBLOCK': 'easy3', '!DATASETNICK': 'MY_DATASET', '!DATASETPATH': '/MY/DATASET', 'DATASETSPLIT': 8, '!FILE_NAMES': '/path/file9', '!MAX_EVENTS': 10, '!SKIP_EVENTS': 5, '!GC_PARAM': 8}
	redo: [] disable: [] size: False

	>>> utils.remove_files(['datacache.dat', 'datamap.tar'])
	"""

class Test_DataParameterSource:
	"""
	>>> utils.remove_files(['dummycache.dat', 'dummymap.tar', 'dataset.tmp'])
	>>> data_bl = ss2bl('AABBCCCD')
	>>> updateDS(data_bl, '')
	>>> repository = {}
	>>> config = create_config(config_dict = {'global': {'workdir': '.', 'events per job': 3, 'dummy': 'dataset.tmp', 'dummy splitter': 'EventBoundarySplitter', 'partition processor': 'TestsuitePartitionProcessor'}})
	>>> psrc = ParameterSource.create_instance('DataParameterSource', config, 'dummy', repository, keep_old=False)
	 * Dataset 'dataset.tmp':
	  contains 1 block with 4 files with 8 entries
	>>> psrc.setup_resync(interval = 0)

	>>> DPS = ParameterSource.get_class('DataParameterSource')
	>>> DPS.create_psrc(None, repository, 'dummy')
	data(dummy)
	>>> try_catch(lambda: DPS.create_psrc(None, repository, 'data'), 'UserError', 'not setup')
	caught

	>>> testPS(psrc, showHash = False) # AAB BCC CD
	data(dummy)
	3
	Keys = EVT, FN, SID [trk], SKIP, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> updateDS(data_bl, 'D:2')
	>>> psrc.resync_psrc() == (set([]), set([]), True)
	True
	>>> testPS(psrc, showHash = False) # AAB BCC CD D
	data(dummy)
	4
	Keys = EVT, FN, SID [trk], SKIP, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, '!GC_PARAM': 3}
	redo: [] disable: [] size: False

	>>> updateDS(data_bl, 'D:2 C:1')
	>>> psrc.resync_psrc() == (set([]), set([1, 2]), True)
	True
	>>> testPS(psrc, showHash = False) # AAB BCc cD D BC D
	data(dummy)
	6
	Keys = EVT, FN, SID [trk], SKIP, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, '!GC_PARAM': 5}
	redo: [] disable: [] size: False

	>>> updateDS(data_bl, 'C:1')
	>>> psrc.resync_psrc() == (set([]), set([3]), False)
	True
	>>> testPS(psrc, showHash = False) # AAB BCc cD d BC D
	data(dummy)
	6
	Keys = EVT, FN, SID [trk], SKIP, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, '!GC_PARAM': 5}
	redo: [] disable: [] size: False

	>>> updateDS(data_bl, '')
	>>> psrc.resync_psrc() == (set([]), set([]), True)
	True
	>>> testPS(psrc, showHash = False) # AAB BCc cD d BC D CC
	data(dummy)
	7
	Keys = EVT, FN, SID [trk], SKIP, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, '!GC_PARAM': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, '!GC_PARAM': 6}
	redo: [] disable: [] size: False

	>>> utils.remove_files(['dummycache.dat', 'dummymap.tar', 'dataset.tmp'])
	"""

class Test_CSV:
	"""
	>>> testPS(ParameterSource.create_instance('CSVParameterSource', 'test.csv'))
	csv('test.csv')
	773ca76ec29b866084e7aa1b06d36c90
	3
	Keys = KEY1 [trk], KEY2 [trk], KEY3 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': '1', 'KEY2': '2', 'KEY3': '3', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'A', 'KEY2': 'B', 'KEY3': 'C', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'a', 'KEY2': 'b', 'KEY3': 'c', '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('CSVParameterSource', 'test2.csv'))
	csv('test2.csv')
	773ca76ec29b866084e7aa1b06d36c90
	3
	Keys = KEY1 [trk], KEY2 [trk], KEY3 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': '1', 'KEY2': '2', 'KEY3': '3', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'A', 'KEY2': 'B', 'KEY3': 'C', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'a', 'KEY2': 'b', 'KEY3': 'c', '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> testPS(ParameterSource.create_instance('CSVParameterSource', 'test2.csv', 'excel'))
	csv('test2.csv', 'excel')
	773ca76ec29b866084e7aa1b06d36c90
	3
	Keys = KEY1 [trk], KEY2 [trk], KEY3 [trk], GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': '1', 'KEY2': '2', 'KEY3': '3', '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'A', 'KEY2': 'B', 'KEY3': 'C', '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'KEY1': 'a', 'KEY2': 'b', 'KEY3': 'c', '!GC_PARAM': 2}
	redo: [] disable: [] size: False

	>>> try_catch(lambda: ParameterSource.create_instance('CSVParameterSource', 'test1.csv'), 'ParameterError', 'Malformed entry in csv file')
	caught
	"""

run_test()
