#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, try_catch
from testfwk_datasets import createfun_get_list_provider, get_data_psrc
from grid_control.backends import WMS
from grid_control.datasets import PartitionProcessor
from grid_control.parameters import ParameterInfo
from grid_control.parameters.padapter import ParameterAdapter
from python_compat import imap, irange, lmap


get_data_provider = createfun_get_list_provider()

def testPP(pproc_names, config_dict=None, keys=[], dataset='dataE.dbs', w=23):
	data_src = get_data_provider(fn='../datasets/' + dataset)
	ps = get_data_psrc(data_src, splitter_name='BlockBoundarySplitter',
		config_dict=config_dict, pproc_name_list=pproc_names.split())
	pa = ParameterAdapter(create_config(), ps)
	for jobnum in irange(pa.get_job_len()):
		jobInfo = pa.get_job_content(jobnum)
		reqs = lmap(lambda kv: (WMS.enum2str(kv[0], kv[0]), kv[1]), jobInfo[ParameterInfo.REQS])
		msg = [str(jobnum), str(jobInfo[ParameterInfo.ACTIVE]).rjust(5),
			str(reqs).ljust(w), jobInfo.get('FILE_NAMES', '<missing>')]
		if keys:
			msg.append('|')
			msg.extend(imap(lambda k: repr(jobInfo.get(k)), keys))
		print(str.join(' ', imap(str, msg)).rstrip())


def testPPLoc(config_dict=None):
	config_dict = config_dict or {}
	config_dict['partition location filter matcher mode'] = 'end'
	return testPP('LocationPartitionProcessor', config_dict=config_dict, dataset='dataH.dbs', w=55)


class Test_LocationProcessor:
	"""
	>>> testPPLoc()
	Block /MY/DATASET#storage3 is not available at any site!
	0  True [('STORAGE', ['SE1.siteA', 'SE2.siteA'])]               <missing>
	1  True []                                                      <missing>
	2 False [('STORAGE', [])]                                       <missing>
	3  True [('STORAGE', ['SE2.siteA', 'SE3.siteB'])]               <missing>
	4  True [('STORAGE', ['SE4.siteB'])]                            <missing>
	5  True [('STORAGE', ['SE1.siteA', 'SE3.siteB', 'SE5.siteA'])]  <missing>

	>>> testPPLoc({'partition location filter': '-something'})
	Block /MY/DATASET#storage3 is not available at any site!
	0  True [('STORAGE', ['SE1.siteA', 'SE2.siteA'])]               <missing>
	1  True []                                                      <missing>
	2 False [('STORAGE', [])]                                       <missing>
	3  True [('STORAGE', ['SE2.siteA', 'SE3.siteB'])]               <missing>
	4  True [('STORAGE', ['SE4.siteB'])]                            <missing>
	5  True [('STORAGE', ['SE1.siteA', 'SE3.siteB', 'SE5.siteA'])]  <missing>

	>>> testPPLoc({'partition location filter': 'siteA'})
	Block /MY/DATASET#storage3 is not available at any site!
	0  True [('STORAGE', ['SE1.siteA', 'SE2.siteA'])]               <missing>
	1  True [('STORAGE', ['siteA'])]                                <missing>
	2 False [('STORAGE', [])]                                       <missing>
	3  True [('STORAGE', ['SE2.siteA', 'SE3.siteB'])]               <missing>
	4  True [('STORAGE', ['SE4.siteB'])]                            <missing>
	5  True [('STORAGE', ['SE1.siteA', 'SE3.siteB', 'SE5.siteA'])]  <missing>

	>>> testPPLoc({'partition location filter': '-siteB'})
	Block /MY/DATASET#storage3 is not available at any site!
	0  True [('STORAGE', ['SE1.siteA', 'SE2.siteA'])]               <missing>
	1  True []                                                      <missing>
	2 False [('STORAGE', [])]                                       <missing>
	3  True [('STORAGE', ['SE2.siteA'])]                            <missing>
	4 False [('STORAGE', [])]                                       <missing>
	5  True [('STORAGE', ['SE1.siteA', 'SE5.siteA'])]               <missing>

	>>> testPPLoc({'partition location filter': 'siteA', 'partition location filter plugin': 'strict'})
	Block /MY/DATASET#storage3 is not available at any site!
	0  True [('STORAGE', ['SE1.siteA', 'SE2.siteA'])]               <missing>
	1  True [('STORAGE', ['siteA'])]                                <missing>
	2 False [('STORAGE', [])]                                       <missing>
	3  True [('STORAGE', ['SE2.siteA'])]                            <missing>
	4 False [('STORAGE', [])]                                       <missing>
	5  True [('STORAGE', ['SE1.siteA', 'SE5.siteA'])]               <missing>

	>>> testPPLoc({'partition location preference': 'SE1.siteA'})
	Block /MY/DATASET#storage3 is not available at any site!
	0  True [('STORAGE', ['SE1.siteA'])]                            <missing>
	1  True [('STORAGE', ['SE1.siteA'])]                            <missing>
	2  True [('STORAGE', ['SE1.siteA'])]                            <missing>
	3  True [('STORAGE', ['SE2.siteA', 'SE3.siteB'])]               <missing>
	4  True [('STORAGE', ['SE4.siteB'])]                            <missing>
	5  True [('STORAGE', ['SE1.siteA'])]                            <missing>
	"""

class Test_LFNPartitionProcessor:
	"""
	>>> testPP('LFNPartitionProcessor BasicPartitionProcessor', {'partition lfn modifier': '/'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      /store//path/file0 /store//path/file1 /store//path/file2
	1  True []                      /store//path/file3 /store//path/file5
	2  True []                      /store//path/file6 /store//path/file7 /store//path/file8 /store//path/file9

	>>> testPP('LFNPartitionProcessor BasicPartitionProcessor', {'partition lfn modifier': 'TEST'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      TEST/store//path/file0 TEST/store//path/file1 TEST/store//path/file2
	1  True []                      TEST/store//path/file3 TEST/store//path/file5
	2  True []                      TEST/store//path/file6 TEST/store//path/file7 TEST/store//path/file8 TEST/store//path/file9

	>>> testPP('LFNPartitionProcessor BasicPartitionProcessor', {'partition lfn modifier': 'TEST', 'partition lfn modifier dict': 'test => 123'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      123/store//path/file0 123/store//path/file1 123/store//path/file2
	1  True []                      123/store//path/file3 123/store//path/file5
	2  True []                      123/store//path/file6 123/store//path/file7 123/store//path/file8 123/store//path/file9

	>>> testPP('LFNPartitionProcessor BasicPartitionProcessor', {'partition lfn modifier': '<xrootd>'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      root://cms-xrd-global.cern.ch//store//path/file0 root://cms-xrd-global.cern.ch//store//path/file1 root://cms-xrd-global.cern.ch//store//path/file2
	1  True []                      root://cms-xrd-global.cern.ch//store//path/file3 root://cms-xrd-global.cern.ch//store//path/file5
	2  True []                      root://cms-xrd-global.cern.ch//store//path/file6 root://cms-xrd-global.cern.ch//store//path/file7 root://cms-xrd-global.cern.ch//store//path/file8 root://cms-xrd-global.cern.ch//store//path/file9

	>>> testPP('LFNPartitionProcessor BasicPartitionProcessor', {'partition lfn modifier': 'srm://test.de/store'}, keys=['DATASET_SRM_FILES'])
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      file://file0 file://file1 file://file2 | 'srm://test.de/store/store//path/file0 srm://test.de/store/store//path/file1 srm://test.de/store/store//path/file2'
	1  True []                      file://file3 file://file5 | 'srm://test.de/store/store//path/file3 srm://test.de/store/store//path/file5'
	2  True []                      file://file6 file://file7 file://file8 file://file9 | 'srm://test.de/store/store//path/file6 srm://test.de/store/store//path/file7 srm://test.de/store/store//path/file8 srm://test.de/store/store//path/file9'
	"""

class Test_MetaPartitionProcessor:
	"""
	>>> testPP('MetaPartitionProcessor BasicPartitionProcessor', keys=['KEY1', 'KEY2', 'KEY3'], dataset='dataK.dbs')
	0  True []                      file1 file2 file3 filex | None None None

	>>> testPP('MetaPartitionProcessor BasicPartitionProcessor', keys=['KEY1', 'KEY2', 'KEY3'], dataset='dataK.dbs',
	... config_dict={'partition metadata': 'KEY1'})
	0  True []                      file1 file2 file3 filex | 'Value1' None None

	>>> testPP('MetaPartitionProcessor BasicPartitionProcessor', keys=['KEY1', 'KEY2', 'KEY3'], dataset='dataK.dbs',
	... config_dict={'partition metadata': 'KEY1 KEY2 KEY3'})
	0  True []                      file1 file2 file3 filex | 'Value1' None None
	"""

class Test_TFCPartitionProcessor:
	"""
	>>> testPP('TFCPartitionProcessor BasicPartitionProcessor LocationPartitionProcessor')
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  /path/file0 /path/file1 /path/file2
	1  True []                      /path/file3 /path/file5
	2 False [('STORAGE', [])]       /path/file6 /path/file7 /path/file8 /path/file9

	>>> testPP('TFCPartitionProcessor BasicPartitionProcessor LocationPartitionProcessor')
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  /path/file0 /path/file1 /path/file2
	1  True []                      /path/file3 /path/file5
	2 False [('STORAGE', [])]       /path/file6 /path/file7 /path/file8 /path/file9

	>>> testPP('TFCPartitionProcessor BasicPartitionProcessor LocationPartitionProcessor',
	... config_dict={'partition tfc': 'prefix:'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  prefix:/path/file0 prefix:/path/file1 prefix:/path/file2
	1  True []                      prefix:/path/file3 prefix:/path/file5
	2 False [('STORAGE', [])]       prefix:/path/file6 prefix:/path/file7 prefix:/path/file8 prefix:/path/file9

	>>> testPP('TFCPartitionProcessor BasicPartitionProcessor LocationPartitionProcessor',
	... config_dict={'partition tfc': 'prefix:\\nSE4 => xrootd:'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  xrootd:/path/file0 xrootd:/path/file1 xrootd:/path/file2
	1  True []                      prefix:/path/file3 prefix:/path/file5
	2 False [('STORAGE', [])]       prefix:/path/file6 prefix:/path/file7 prefix:/path/file8 prefix:/path/file9

	>>> testPP('TFCPartitionProcessor BasicPartitionProcessor LocationPartitionProcessor',
	... config_dict={'partition tfc': 'SE4 => xrootd:'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  xrootd:/path/file0 xrootd:/path/file1 xrootd:/path/file2
	1  True []                      /path/file3 /path/file5
	2 False [('STORAGE', [])]       /path/file6 /path/file7 /path/file8 /path/file9

	>>> testPP('TFCPartitionProcessor BasicPartitionProcessor LocationPartitionProcessor',
	... config_dict={'partition tfc': 'prefix:\\nSE => local:\\nST => xrootd:'})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  local:/path/file0 local:/path/file1 local:/path/file2
	1  True []                      prefix:/path/file3 prefix:/path/file5
	2 False [('STORAGE', [])]       prefix:/path/file6 prefix:/path/file7 prefix:/path/file8 prefix:/path/file9
	"""

class Test_ReqPartitionProcessor:
	"""
	>>> testPP('BasicPartitionProcessor RequirementsPartitionProcessor', {'partition walltime factor': 2, 'partition walltime offset': 10})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('WALLTIME', 70)]      /path/file0 /path/file1 /path/file2
	1  True [('WALLTIME', 40)]      /path/file3 /path/file5
	2  True [('WALLTIME', 90)]      /path/file6 /path/file7 /path/file8 /path/file9

	>>> testPP('BasicPartitionProcessor RequirementsPartitionProcessor', {'partition cputime factor': 0.2, 'partition cputime offset': 10})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('CPUTIME', 16)]       /path/file0 /path/file1 /path/file2
	1  True [('CPUTIME', 13)]       /path/file3 /path/file5
	2  True [('CPUTIME', 18)]       /path/file6 /path/file7 /path/file8 /path/file9

	>>> testPP('BasicPartitionProcessor RequirementsPartitionProcessor', {'partition memory factor': 100, 'partition memory offset': -2000})
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('MEMORY', 1000)]      /path/file0 /path/file1 /path/file2
	1  True []                      /path/file3 /path/file5
	2  True [('MEMORY', 2000)]      /path/file6 /path/file7 /path/file8 /path/file9
	"""

class Test_PartitionProcessor:
	"""
	>>> try_catch(lambda: PartitionProcessor(create_config(), 'dataset').process(0, {}, {}), 'AbstractError', 'is an abstract function')
	caught

	>>> testPP('BasicPartitionProcessor')
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      /path/file0 /path/file1 /path/file2
	1  True []                      /path/file3 /path/file5
	2  True []                      /path/file6 /path/file7 /path/file8 /path/file9

	>>> testPP('CMSSWPartitionProcessor')
	Block /MY/DATASET#easy3 is not available at any site!
	0  True []                      "/path/file0", "/path/file1", "/path/file2"
	1  True []                      "/path/file3", "/path/file5"
	2  True []                      "/path/file6", "/path/file7", "/path/file8", "/path/file9"

	>>> testPP('LocationPartitionProcessor')
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  <missing>
	1  True []                      <missing>
	2 False [('STORAGE', [])]       <missing>

	>>> testPP('BasicPartitionProcessor LocationPartitionProcessor')
	Block /MY/DATASET#easy3 is not available at any site!
	0  True [('STORAGE', ['SE4'])]  /path/file0 /path/file1 /path/file2
	1  True []                      /path/file3 /path/file5
	2 False [('STORAGE', [])]       /path/file6 /path/file7 /path/file8 /path/file9
	"""

run_test()
