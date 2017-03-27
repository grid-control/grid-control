#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, testfwk_remove_files, try_catch
from grid_control.parameters import ParameterSource
from grid_control.parameters.padapter import BasicParameterAdapter, ParameterAdapter, TrackedParameterAdapter
from grid_control.parameters.psource_meta import CrossParameterSource
from testDS import ss2bl
from testINC import TestParameterSource, testPA, testPS, updateDS
from python_compat import set


p1 = TestParameterSource('A', [1, 2, 3], (set([1]), set([2]), False)) # redo: 2, disable: 3
p2 = TestParameterSource('B', ['M', 'N'], (set([0]), set([1]), False)) # redo: M, disable: N
p3 = ParameterSource.create_instance('CounterParameterSource', 'X', 100)

class Test_ParameterAdapter:
	"""
	>>> testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz'])
	>>> p1.resync_psrc() == (set([1]), set([2]), False)
	True
	>>> p2.resync_psrc() == (set([0]), set([1]), False)
	True
	>>> p1.resync_psrc() == (set([1]), set([2]), False)
	True
	>>> p2.resync_psrc() == (set([0]), set([1]), False)
	True

	>>> ps1 = ParameterSource.create_instance('CrossParameterSource', p1, p2, p3)
	>>> testPS(ps1, showJob = True, showHash = False)
	cross(var('A'), var('B'), counter('!X', 100))
	6
	Keys = A [trk], B [trk], X, GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [0, 1, 2, 4] disable: [2, 3, 4, 5] size: False

	>>> pa1 = ParameterAdapter(create_config(), ps1)
	>>> pa1.can_submit(2)
	True
	>>> try_catch(lambda: pa1.get_job_content(None), 'APIError', 'Unable to process job number None!')
	caught

	>>> pa2 = BasicParameterAdapter(create_config(config_dict={'global': {'workdir': '.'}}), ps1)
	>>> pa2.can_submit(2)
	True

	>>> ps2 = ParameterSource.create_instance('CrossParameterSource', p2, p3, p1)
	>>> testPS(ps2, showJob = True, showHash = False)
	cross(var('B'), counter('!X', 100), var('A'))
	6
	Keys = A [trk], B [trk], X, GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 101, '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 104, '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [0, 2, 3, 4] disable: [1, 3, 4, 5] size: False

	>>> testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz'])
	"""

p0 = ParameterSource.create_instance('SimpleParameterSource', 'VAR', ['A', 'B'])
p4 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['', 'X'])
p5 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['', 'Y', 'X'])
p6 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['Y', ''])
p7 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['', '', 'Y'])
p8 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['X', ''])
p9 = ParameterSource.create_instance('SimpleParameterSource', 'C', ['', 'X', ''])

class Test_TrackedParameterAdapterRepeat:
	"""
	>>> testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz', 'dummycache*.dat', 'dummymap*.tar', 'dataset.tmp'])

	>>> data_bl = ss2bl('AABBCCCD')
	>>> updateDS(data_bl, '')

	>>> config = create_config(config_dict={'global': {'workdir': '.', 'events per job': 3, 'dummy': 'dataset.tmp', 'dummy splitter': 'EventBoundarySplitter', 'partition processor': 'TestsuitePartitionProcessor'}})
	>>> pd = ParameterSource.create_instance('DataParameterSource', config, 'dummy', {}, keep_old=False)
	 * Dataset 'dataset.tmp':
	  contains 1 block with 4 files with 8 entries
	>>> pd.setup_resync(interval = 0)
	>>> ps = CrossParameterSource(pd, p0, p3)
	>>> pa = TrackedParameterAdapter(config, ps)
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	6
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	redo: [] disable: [] size: False

	>>> updateDS(data_bl, 'D:2') # AAB BCC CD D
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	8
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	redo: [] disable: [] size: True

	>>> updateDS(data_bl, 'D:2') # AAB BCC CD D
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	8
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	redo: [] disable: [] size: False

	>>> updateDS(data_bl, 'D:2 C:1') # AAB BCc cD D BC D
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	12
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	8 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'A', '!X': 108}
	9 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'A', '!X': 109}
	10 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'B', '!X': 110}
	11 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'B', '!X': 111}
	redo: [] disable: [1, 2, 4, 5] size: True

	>>> updateDS(data_bl, 'C:1')
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	12
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	8 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'A', '!X': 108}
	9 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'A', '!X': 109}
	10 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'B', '!X': 110}
	11 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'B', '!X': 111}
	redo: [] disable: [6, 7] size: False

	>>> updateDS(data_bl, '')
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	14
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	8 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'A', '!X': 108}
	9 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'A', '!X': 109}
	10 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'B', '!X': 110}
	11 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'B', '!X': 111}
	12 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'A', '!X': 112}
	13 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'B', '!X': 113}
	redo: [] disable: [] size: True

	>>> updateDS(data_bl, 'A:3')
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	16
	Keys = EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	8 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'A', '!X': 108}
	9 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'A', '!X': 109}
	10 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'B', '!X': 110}
	11 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'B', '!X': 111}
	12 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'A', '!X': 112}
	13 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'B', '!X': 113}
	14 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 4, '!FN': 'A B', 'SID': 7, '!SKIP': 0, 'VAR': 'A', '!X': 114}
	15 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 4, '!FN': 'A B', 'SID': 7, '!SKIP': 0, 'VAR': 'B', '!X': 115}
	redo: [] disable: [0, 3] size: True

	>>> ps = CrossParameterSource(pd, p0, p3, p8)
	>>> pa = TrackedParameterAdapter(config, ps)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	>>> testPA(pa, showPNum = False, showJob = False)
	Finished resync of parameter source (XX:XX:XX)
	32
	Keys = C [trk], EVT, FN, SID [trk], SKIP, VAR [trk], X
	0 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 100}
	1 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 101}
	2 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 102}
	3 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 103}
	4 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 104}
	5 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 105}
	6 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 106}
	7 {'<ACTIVE>': False, '<REQS>': [], '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 107}
	8 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'A', '!X': 108}
	9 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'A', '!X': 109}
	10 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'B', '!X': 110}
	11 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'B', '!X': 111}
	12 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'A', '!X': 112}
	13 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'B', '!X': 113}
	14 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 4, '!FN': 'A B', 'SID': 7, '!SKIP': 0, 'VAR': 'A', '!X': 114}
	15 {'<ACTIVE>': True, '<REQS>': [], '!EVT': 4, '!FN': 'A B', 'SID': 7, '!SKIP': 0, 'VAR': 'B', '!X': 115}
	16 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'A', '!X': 116}
	17 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'A', '!X': 117}
	18 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'A', '!X': 118}
	19 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'A', '!X': 119}
	20 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'A', '!X': 120}
	21 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'A', '!X': 121}
	22 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'A', '!X': 122}
	23 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 4, '!FN': 'A B', 'SID': 7, '!SKIP': 0, 'VAR': 'A', '!X': 123}
	24 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 3, '!FN': 'A B', 'SID': 0, '!SKIP': 0, 'VAR': 'B', '!X': 124}
	25 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 3, '!FN': 'B C', 'SID': 1, '!SKIP': 1, 'VAR': 'B', '!X': 125}
	26 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 2, '!FN': 'C D', 'SID': 2, '!SKIP': 2, 'VAR': 'B', '!X': 126}
	27 {'<ACTIVE>': False, '<REQS>': [], 'C': 'X', '!EVT': 0, '!FN': 'D', 'SID': 3, '!SKIP': 1, 'VAR': 'B', '!X': 127}
	28 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 2, '!FN': 'B C', 'SID': 4, '!SKIP': 1, 'VAR': 'B', '!X': 128}
	29 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 1, '!FN': 'D', 'SID': 5, '!SKIP': 0, 'VAR': 'B', '!X': 129}
	30 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 2, '!FN': 'C', 'SID': 6, '!SKIP': 1, 'VAR': 'B', '!X': 130}
	31 {'<ACTIVE>': True, '<REQS>': [], 'C': 'X', '!EVT': 4, '!FN': 'A B', 'SID': 7, '!SKIP': 0, 'VAR': 'B', '!X': 131}
	redo: [] disable: [] size: False

	>>> testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz', 'dummycache*.dat', 'dummymap*.tar', 'dataset.tmp'])
	"""

class Test_TrackedParameterAdapterSequential:
	"""
	>>> testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz'])
	>>> config = create_config(config_dict={'global': {'workdir': '.'}})
	>>> p1.resync_psrc() == (set([1]), set([2]), False)
	True
	>>> p2.resync_psrc() == (set([0]), set([1]), False)
	True
	>>> p3.resync_psrc() == (set([]), set([]), False)
	True
	>>> CrossParameterSource(p1, p2, p3).resync_psrc() == (set([0, 1, 2, 4]), set([2, 3, 4, 5]), False)
	True
	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p1, p2, p3)))
	Finished resync of parameter source (XX:XX:XX)
	6
	Keys = A [trk], B [trk], X, GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1, '!GC_PARAM': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2, '!GC_PARAM': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3, '!GC_PARAM': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4, '!GC_PARAM': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [0, 1] disable: [2, 3, 4, 5] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p2, p3, p1)))
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	6
	Keys = A [trk], B [trk], X, GC_JOB_ID, GC_PARAM
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0, '!GC_PARAM': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1, '!GC_PARAM': 2}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2, '!GC_PARAM': 4}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3, '!GC_PARAM': 1}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4, '!GC_PARAM': 3}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5, '!GC_PARAM': 5}
	redo: [0, 1] disable: [2, 3, 4, 5] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p3, p4, p1, p2)), showPNum = False)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	12
	Keys = A [trk], B [trk], C [trk], X, GC_JOB_ID
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'X', '!X': 106, '!GC_JOB_ID': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'X', '!X': 107, '!GC_JOB_ID': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'X', '!X': 108, '!GC_JOB_ID': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'X', '!X': 109, '!GC_JOB_ID': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'X', '!X': 110, '!GC_JOB_ID': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'X', '!X': 111, '!GC_JOB_ID': 11}
	redo: [0, 1, 6, 7] disable: [2, 3, 4, 5, 8, 9, 10, 11] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p3, p5, p1, p2)), showPNum = False)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	18
	Keys = A [trk], B [trk], C [trk], X, GC_JOB_ID
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'X', '!X': 106, '!GC_JOB_ID': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'X', '!X': 107, '!GC_JOB_ID': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'X', '!X': 108, '!GC_JOB_ID': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'X', '!X': 109, '!GC_JOB_ID': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'X', '!X': 110, '!GC_JOB_ID': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'X', '!X': 111, '!GC_JOB_ID': 11}
	12 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'Y', '!X': 112, '!GC_JOB_ID': 12}
	13 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'Y', '!X': 113, '!GC_JOB_ID': 13}
	14 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'Y', '!X': 114, '!GC_JOB_ID': 14}
	15 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'Y', '!X': 115, '!GC_JOB_ID': 15}
	16 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'Y', '!X': 116, '!GC_JOB_ID': 16}
	17 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'Y', '!X': 117, '!GC_JOB_ID': 17}
	redo: [0, 1, 6, 7, 12, 13] disable: [2, 3, 4, 5, 8, 9, 10, 11, 14, 15, 16, 17] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p3, p6, p1, p2)), showPNum = False)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	18
	Keys = A [trk], B [trk], C [trk], X, GC_JOB_ID
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5}
	6 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'X', '!GC_JOB_ID': 6}
	7 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'X', '!GC_JOB_ID': 7}
	8 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'X', '!GC_JOB_ID': 8}
	9 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'X', '!GC_JOB_ID': 9}
	10 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'X', '!GC_JOB_ID': 10}
	11 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'X', '!GC_JOB_ID': 11}
	12 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'Y', '!X': 112, '!GC_JOB_ID': 12}
	13 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'Y', '!X': 113, '!GC_JOB_ID': 13}
	14 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'Y', '!X': 114, '!GC_JOB_ID': 14}
	15 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'Y', '!X': 115, '!GC_JOB_ID': 15}
	16 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'Y', '!X': 116, '!GC_JOB_ID': 16}
	17 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'Y', '!X': 117, '!GC_JOB_ID': 17}
	redo: [0, 1, 12, 13] disable: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p3, p7, p1, p2)), showPNum = False)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	24
	Keys = A [trk], B [trk], C [trk], X, GC_JOB_ID
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5}
	6 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'X', '!GC_JOB_ID': 6}
	7 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'X', '!GC_JOB_ID': 7}
	8 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'X', '!GC_JOB_ID': 8}
	9 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'X', '!GC_JOB_ID': 9}
	10 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'X', '!GC_JOB_ID': 10}
	11 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'X', '!GC_JOB_ID': 11}
	12 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'Y', '!X': 112, '!GC_JOB_ID': 12}
	13 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'Y', '!X': 113, '!GC_JOB_ID': 13}
	14 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'Y', '!X': 114, '!GC_JOB_ID': 14}
	15 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'Y', '!X': 115, '!GC_JOB_ID': 15}
	16 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'Y', '!X': 116, '!GC_JOB_ID': 16}
	17 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'Y', '!X': 117, '!GC_JOB_ID': 17}
	18 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 118, '!GC_JOB_ID': 18}
	19 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 119, '!GC_JOB_ID': 19}
	20 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 120, '!GC_JOB_ID': 20}
	21 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 121, '!GC_JOB_ID': 21}
	22 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 122, '!GC_JOB_ID': 22}
	23 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 123, '!GC_JOB_ID': 23}
	redo: [0, 1, 12, 13, 18, 19] disable: [2, 3, 4, 5, 14, 15, 16, 17, 20, 21, 22, 23] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p3, p8, p1, p2)), showPNum = False)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	24
	Keys = A [trk], B [trk], C [trk], X, GC_JOB_ID
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'X', '!X': 106, '!GC_JOB_ID': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'X', '!X': 107, '!GC_JOB_ID': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'X', '!X': 108, '!GC_JOB_ID': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'X', '!X': 109, '!GC_JOB_ID': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'X', '!X': 110, '!GC_JOB_ID': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'X', '!X': 111, '!GC_JOB_ID': 11}
	12 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'Y', '!GC_JOB_ID': 12}
	13 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'Y', '!GC_JOB_ID': 13}
	14 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'Y', '!GC_JOB_ID': 14}
	15 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'Y', '!GC_JOB_ID': 15}
	16 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'Y', '!GC_JOB_ID': 16}
	17 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'Y', '!GC_JOB_ID': 17}
	18 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'M', '!GC_JOB_ID': 18}
	19 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'M', '!GC_JOB_ID': 19}
	20 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'M', '!GC_JOB_ID': 20}
	21 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'N', '!GC_JOB_ID': 21}
	22 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'N', '!GC_JOB_ID': 22}
	23 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'N', '!GC_JOB_ID': 23}
	redo: [0, 1, 6, 7] disable: [2, 3, 4, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23] size: False

	>>> testPA(TrackedParameterAdapter(config, CrossParameterSource(p3, p9, p1, p2)), showPNum = False)
	Parameter hash has changed
	Finished resync of parameter source (XX:XX:XX)
	24
	Keys = A [trk], B [trk], C [trk], X, GC_JOB_ID
	0 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 100, '!GC_JOB_ID': 0}
	1 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 101, '!GC_JOB_ID': 1}
	2 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 102, '!GC_JOB_ID': 2}
	3 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 103, '!GC_JOB_ID': 3}
	4 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 104, '!GC_JOB_ID': 4}
	5 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 105, '!GC_JOB_ID': 5}
	6 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'X', '!X': 106, '!GC_JOB_ID': 6}
	7 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'X', '!X': 107, '!GC_JOB_ID': 7}
	8 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'X', '!X': 108, '!GC_JOB_ID': 8}
	9 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'X', '!X': 109, '!GC_JOB_ID': 9}
	10 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'X', '!X': 110, '!GC_JOB_ID': 10}
	11 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'X', '!X': 111, '!GC_JOB_ID': 11}
	12 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'M', 'C': 'Y', '!GC_JOB_ID': 12}
	13 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'M', 'C': 'Y', '!GC_JOB_ID': 13}
	14 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'M', 'C': 'Y', '!GC_JOB_ID': 14}
	15 {'<ACTIVE>': False, '<REQS>': [], 'A': 1, 'B': 'N', 'C': 'Y', '!GC_JOB_ID': 15}
	16 {'<ACTIVE>': False, '<REQS>': [], 'A': 2, 'B': 'N', 'C': 'Y', '!GC_JOB_ID': 16}
	17 {'<ACTIVE>': False, '<REQS>': [], 'A': 3, 'B': 'N', 'C': 'Y', '!GC_JOB_ID': 17}
	18 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'M', '!X': 118, '!GC_JOB_ID': 18}
	19 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'M', '!X': 119, '!GC_JOB_ID': 19}
	20 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'M', '!X': 120, '!GC_JOB_ID': 20}
	21 {'<ACTIVE>': True, '<REQS>': [], 'A': 1, 'B': 'N', '!X': 121, '!GC_JOB_ID': 21}
	22 {'<ACTIVE>': True, '<REQS>': [], 'A': 2, 'B': 'N', '!X': 122, '!GC_JOB_ID': 22}
	23 {'<ACTIVE>': True, '<REQS>': [], 'A': 3, 'B': 'N', '!X': 123, '!GC_JOB_ID': 23}
	redo: [0, 1, 6, 7, 18, 19] disable: [2, 3, 4, 5, 8, 9, 10, 11, 20, 21, 22, 23] size: False

	>>> testfwk_remove_files(['task.dat', 'params.dat.gz', 'params.map.gz'])
	"""

run_test()
