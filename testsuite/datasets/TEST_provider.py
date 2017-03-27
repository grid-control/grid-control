#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import logging
from testfwk import cmp_obj, create_config, run_test, testfwk_remove_files, try_catch
from grid_control import utils
from hpfwk import Plugin
from testDS import create_source
from python_compat import imap, lmap


config1 = create_config(config_dict={
	'datasetexample': {
		'dataset location filter': 'SE1 SE2 SE3 -SE4',
		'dataset check entry consistency': 'warn',
	},
}).change_view(set_sections=['datasetexample'])
config2 = create_config()
config3 = create_config(config_dict={
	'datasetfoo': {
		'dataset limit events': '21',
		'dataset check nickname collision': 'warn',
	},
})

DP = Plugin.get_class('DataProvider')
NP = Plugin.get_class('NickNameProducer')
BlockBoundarySplitter = Plugin.get_class('BlockBoundarySplitter')
EventBoundarySplitter = Plugin.get_class('EventBoundarySplitter')

def create_multi(value, defaultProvider):
	config = create_config()
	dp_list = []
	for x in DP.bind(value, config=config, provider_name_default=defaultProvider):
		dp_list.append(x.create_instance_bound())
	return DP.get_class('MultiDatasetProvider')(config, 'dataset', None, None, dp_list)

def test_bind(bind_iter):
	for bind in bind_iter:
		print(repr(bind))

logging.getLogger().setLevel(logging.INFO1)

class Test_DataProviderBind:
	"""
	>>> logging.getLogger().setLevel(logging.DEFAULT)
	>>> list(DP.bind('nick : file.dbs', config=create_config()))
	[<instance factory for 'grid_control.datasets.provider_basic.ListProvider'>]
	>>> logging.getLogger().setLevel(logging.INFO1)

	>>> test_bind(DP.bind('nick : file.dbs', config=create_config()))
	<instance factory for grid_control.datasets.provider_basic.ListProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset', 'file.dbs', 'nick', ...)>

	>>> test_bind(DP.bind('file.dbs \\n nick : file.dbs \\n nick: file : file.dbs', config=create_config()))
	<instance factory for grid_control.datasets.provider_basic.ListProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset provider 1', 'file.dbs', '', ...)>
	<instance factory for grid_control.datasets.provider_basic.ListProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset provider 2', 'file.dbs', 'nick', ...)>
	<instance factory for grid_control.datasets.provider_basic.FileProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset provider 3', 'file.dbs', 'nick', ...)>

	>>> test_bind(DP.bind('file.dbs \\n nick : file.dbs \\n nick: file : file.dbs', config=create_config(), provider_name_default ='FileProvider'))
	<instance factory for grid_control.datasets.provider_basic.FileProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset provider 1', 'file.dbs', '', ...)>
	<instance factory for grid_control.datasets.provider_basic.FileProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset provider 2', 'file.dbs', 'nick', ...)>
	<instance factory for grid_control.datasets.provider_basic.FileProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset provider 3', 'file.dbs', 'nick', ...)>

	>>> fac = list(DP.bind('nick : file.dbs', config=create_config()))[0]
	>>> fac
	<instance factory for grid_control.datasets.provider_basic.ListProvider(<SimpleConfigInterface(view = <SimpleConfigView(sections = None)>)>, 'dataset', 'file.dbs', 'nick', ...)>
	>>> try_catch(lambda: fac.create_instance_bound(test=True), 'PluginError', 'Error while creating instance')
	caught
	"""

class Test_NicknameProducer:
	"""
	>>> np1 = NP.create_instance('NickNameProducer', config1, 'dataset')
	>>> try_catch(lambda: np1.get_name('', '/PRIVATE/TESTDS#TESTBLOCK', None), 'AbstractError', 'is an abstract function')
	caught

	>>> np2 = NP.create_instance('SimpleNickNameProducer', config1, 'dataset')
	>>> np2.get_name('', '/PRIVATE/TESTDS#TESTBLOCK', None)
	'TESTDS'
	>>> np2.get_name('', '/LEVEL1/TESTDS#TESTBLOCK', None)
	'LEVEL1_TESTDS'
	>>> np2.get_name('nick1', '/PRIVATE/TESTDS#TESTBLOCK', None)
	'nick1'

	>>> np3 = NP.create_instance('InlineNickNameProducer', create_config(config_dict={'datasets': {'nickname expr': 'current_nickname or dataset.split("#")[0].lstrip("/").replace("/", "_")'}}), 'dataset')
	>>> np3.get_name('', '/PRIVATE/TESTDS#TESTBLOCK', None)
	'PRIVATE_TESTDS'
	>>> np3.get_name('', '/LEVEL1/TESTDS#TESTBLOCK', None)
	'LEVEL1_TESTDS'
	>>> np3.get_name('nick1', '/PRIVATE/TESTDS#TESTBLOCK', None)
	'nick1'
	"""

class Test_ConfigDataProvider:
	"""
	>>> dpConfig1 = create_source([('/PRIVATE/DSB#1', {'file2a': 1, 'file2b': 1})])
	>>> bc = dpConfig1.get_block_list_cached(show_stats=False)
	>>> cmp_obj(bc, [{DP.FileList: [{DP.NEntries: 1, DP.URL: 'file2a'}, {DP.NEntries: 1, DP.URL: 'file2b'}], DP.Nickname: 'DSB', DP.Provider: 'ConfigDataProvider', DP.NEntries: 2, DP.BlockName: '1', DP.Dataset: '/PRIVATE/DSB', DP.Locations: None}])
	"""

class Test_FileProvider:
	"""
	>>> dpFile1 = DP.create_instance('FileProvider', config1, 'dataset', '/path/ to /file | 123 @ SE1, SE2, SE4', None)
	>>> bf1 = list(dpFile1.get_block_list_cached(show_stats=False))
	>>> cmp_obj(bf1, [{DP.NEntries: 123, DP.BlockName: '0', DP.Dataset: '/path/ to /file', DP.Locations: ['SE1', 'SE2'], DP.Nickname: 'path_to_file', DP.FileList: [{DP.NEntries: 123, DP.URL: '/path/ to /file'}], DP.Provider: 'FileProvider'}])
	>>> try_catch(lambda: DP.create_instance('FileProvider', config1, 'dataset', '/path/ to /file @ SE1, SE2, SE4', None), 'PluginError', "Error while creating instance: grid_control.datasets.provider_basic.FileProvider")
	caught

	>>> dpFile3 = DP.create_instance('FileProvider', config1, 'dataset', '/path/ to /file | 123 @ SE4, SE5', None)
	>>> bf3 = list(dpFile3.get_block_list_cached(show_stats=False))
	Block /path/ to /file is not available at any selected site!
	>>> cmp_obj(bf3, [{DP.NEntries: 123, DP.BlockName: '0', DP.Dataset: '/path/ to /file', DP.Locations: [], DP.Nickname: 'path_to_file', DP.FileList: [{DP.NEntries: 123, DP.URL: '/path/ to /file'}], DP.Provider: 'FileProvider'}])

	>>> dpFile4 = DP.create_instance('FileProvider', config1, 'dataset', '/path/ to /file | 123 @ ', None)
	>>> bf4 = list(dpFile4.get_block_list_cached(show_stats=False))
	>>> cmp_obj(bf4, [{DP.NEntries: 123, DP.BlockName: '0', DP.Dataset: '/path/ to /file', DP.Locations: None, DP.Nickname: 'path_to_file', DP.FileList: [{DP.NEntries: 123, DP.URL: '/path/ to /file'}], DP.Provider: 'FileProvider'}])
	"""

class Test_ListProvider:
	"""
	>>> dpList1 = DP.create_instance('ListProvider', config1, 'dataset', 'dataA.dbs % unchanged', None)
	>>> bl1 = list(dpList1.get_block_list_cached(show_stats=False))
	>>> cmp_obj(bl1, [{DP.NEntries: 30, DP.BlockName: 'unchanged', DP.Dataset: '/MY/DATASET', DP.Locations: None, DP.Nickname: 'MY_DATASET', DP.FileList: [{DP.NEntries: 10, DP.URL: '/path/UC1'}, {DP.NEntries: 5, DP.URL: '/path/UC2'}, {DP.NEntries: 15, DP.URL: '/path/UC3'}], DP.Provider: 'ListProvider'}])
	>>> sum(imap(lambda x: x[DP.NEntries], bl1))
	30

	>>> dpList1.get_dataset_expr()
	'dataA.dbs % unchanged'

	>>> dpList2 = DP.create_instance('ListProvider', config3, 'dataset', 'dataA.dbs', None)
	>>> sum(imap(lambda x: x[DP.NEntries], dpList2.get_block_list_cached(show_stats=False))) <= 21
	Empty files removed: 0, Empty blocks removed 8
	True

	>>> DP.save_to_file('dataA1.dbs', dpList2.get_block_list_cached(show_stats=False))
	>>> dpList2a = DP.create_instance('ListProvider', config3, 'dataset', 'dataA1.dbs', None)
	>>> testfwk_remove_files(['dataA1.dbs'])
	>>> try_catch(lambda: dpList2a.get_block_list_cached(show_stats=False), 'DatasetError', 'Unable to open dataset file')
	caught

	>>> dpList3 = DP.create_instance('ListProvider', config1, 'dataset', 'dataC.dbs @ /enforced', None)
	>>> bl3 = list(dpList3.get_block_list_cached(show_stats=False))
	>>> bl3 == [{DP.NEntries: 20, DP.BlockName: 'test', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'], \
		DP.Nickname: 'TESTNICK', DP.Metadata: ['KEY1', 'KEY2'], \
		DP.FileList: [\
			{DP.NEntries: 5, DP.URL: '/enforced/file1', DP.Metadata: [[1,2,3], 'Test1']},\
			{DP.NEntries: 15, DP.URL: '/enforced/file2', DP.Metadata: [[9,8,7], 'Test2']}],\
		DP.Provider: 'ListProvider'}]
	True

	>>> dpList4 = DP.create_instance('ListProvider', config1, 'dataset', 'dataD.dbs', None)
	>>> try_catch(lambda: list(dpList4.get_block_list_cached(show_stats=False)), 'DatasetRetrievalError', "Unable to retrieve dataset 'dataD.dbs'")
	caught

	>>> dpList5 = DP.create_instance('ListProvider', config1, 'dataset', 'dataF.dbs', None)
	>>> bl5 = list(dpList5.get_block_list_cached(show_stats=False))
	Inconsistency in block /MY/DATASET#fail: Number of events doesn't match (b:200 != f:30)

	>>> lmap(lambda x: x[DP.Locations], DP.create_instance('ListProvider', config1, 'dataset', 'dataE.dbs', None).get_block_list_cached(show_stats=False))
	Block /MY/DATASET#easy1 is not available at any selected site!
	Block /MY/DATASET#easy3 is not available at any site!
	[[], None, []]

	>>> lmap(lambda x: x[DP.Locations], DP.create_instance('ListProvider', config2, 'dataset', 'dataE.dbs', None).get_block_list_cached(show_stats=False))
	Block /MY/DATASET#easy3 is not available at any site!
	[['SE4'], None, []]

	>>> dpRL1 = DP.create_instance('ListProvider', config1, 'dataset', 'dataRL.dbs').get_block_list_cached(show_stats=False)
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#f95a8d7e-f710-458d-8d0f-0d58a7667256 is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#b97fa5ba-925c-4bdb-98c9-bd92340f7440 is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#7bbb3050-fc83-4310-9944-e27821493fb6 is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#3bd6f9d3-aae2-4e94-ac5b-4eedd0a57a97 is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#343bb143-2385-4f11-9641-3b53743c2ccf is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#23c131a1-0fd2-4a77-9435-179f71c00b0e is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#1ce21f58-dd02-478e-866f-b208d5e450ab is not available at any selected site!
	Block /MinimumBias/Commissioning10-SD_Mu-Jun14thSkim_v1/RECO#07f7f674-dd7d-4508-9e49-9879f61b7c3e is not available at any selected site!

	>>> try_catch(lambda: DP.create_instance('ListProvider', config2, 'dataset', 'dataL.dbs', None).get_block_list_cached(show_stats=False), 'DatasetError', 'Unable to parse entries of file')
	caught

	>>> dpRL2 = DP.create_instance('ListProvider', config2, 'dataset', 'dataRL.dbs')
	>>> DP.save_to_file('dataRL1.dbs', dpRL2.get_block_list_cached(show_stats=False))
	>>> DP.load_from_file('dataRL1.dbs').get_block_list_cached(show_stats=False) == dpRL2.get_block_list_cached(show_stats=False)
	True
	>>> testfwk_remove_files(['dataRL1.dbs'])
	"""

class Test_Provider:
	"""
	>>> try_catch(lambda: DP(config1, 'dataset', 'DUMMY', 'NICK').get_block_list_cached(show_stats=False), 'DatasetRetrievalError', "Unable to retrieve dataset 'DUMMY'")
	caught
	"""

class Test_MultiProvider:
	"""
	>>> dpMulti1 = create_multi('nick : /path/file1|123\\n/path/file2|987', 'FileProvider')
	>>> dpMulti1.check_splitter(EventBoundarySplitter).__name__
	'EventBoundarySplitter'
	
	>>> def confuser(splitClass):
	...   if splitClass == EventBoundarySplitter:
	...      return BlockBoundarySplitter
	...   return EventBoundarySplitter
	>>> dpMulti1._provider_list[0].check_splitter = confuser
	>>> try_catch(lambda: dpMulti1.check_splitter(EventBoundarySplitter), 'DatasetError', 'Dataset providers could not agree on valid dataset splitter')
	caught
	>>> dpMulti1.get_query_interval()
	60

	>>> bm1 = list(dpMulti1.get_block_list_cached(show_stats=False))
	>>> cmp_obj(bm1, [\
{DP.NEntries: 123, DP.BlockName: '0', DP.Dataset: '/path/file1', DP.Locations: None, DP.FileList: [{DP.NEntries: 123, DP.URL: '/path/file1'}], DP.Nickname: 'nick', DP.Provider: 'FileProvider'}, \
{DP.NEntries: 987, DP.BlockName: '0', DP.Dataset: '/path/file2', DP.Locations: None, DP.FileList: [{DP.NEntries: 987, DP.URL: '/path/file2'}], DP.Nickname: 'path_file2', DP.Provider: 'FileProvider'}\
])

	>>> dpMulti2 = create_multi('dataA.dbs\\ndataD.dbs', 'ListProvider')
	>>> try_catch(create_multi('dataD.dbs\\ndataD.dbs', 'ListProvider').get_dataset_name_list, 'DatasetError', 'Could not retrieve all datasets')
	caught
	>>> try_catch(lambda: dpMulti2.get_block_list_cached(show_stats=False), 'DatasetError', 'Could not retrieve all datasets')
	caught
	>>> dpMulti3 = create_multi('/path/file1|123\\n/path/file2|123', 'FileProvider')
	>>> utils.abort(True)
	True
	>>> try_catch(lambda: list(dpMulti3.get_block_list_cached(show_stats=False)), 'DatasetError', 'Received abort request during retrieval of')
	caught
	>>> utils.abort(False)
	False
	"""

run_test()
