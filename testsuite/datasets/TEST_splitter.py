#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, try_catch
from grid_control.datasets import DataProvider, DataSplitter, PartitionReader
from grid_control.datasets.splitter_base import PartitionResyncHandler
from hpfwk import Plugin
from testDS import display_partitions, get_lfn_map


config1 = create_config(config_dict={
	'somesection': {
		'resync interactive': 'False',
		'events per job': '10',
		'files per job': '2',
	},
})

config1a = create_config(config_dict={
	'somesection': {
		'resync interactive': 'False',
		'events per job': '10',
		'files per job': '2',
		'run range': 2,
	},
})

config2 = create_config(config_dict={
	'myexampletask': {
		'events per job': '15',
		'files per job': '2',
		'splitter stack': 'BlockBoundarySplitter FileBoundarySplitter EventBoundarySplitter',
	},
})

configF = create_config(config_dict={
	'somesection': {
		'resync interactive': 'False',
		'events per job': '-1',
		'files per job': '-1',
	},
})


def test_splitter(name, datafile, config):
	datasrc = DataProvider.load_from_file(datafile)
	splitter = DataSplitter.create_instance(name, config, 'dataset')
	partition_iter = splitter.split_partitions(datasrc.get_block_list_cached(show_stats=False))
	display_partitions(get_lfn_map(datasrc), partition_iter)
	return splitter


class TestBase(object):
	"""
	>>> try_catch(lambda: test_splitter('FileLevelSplitter', 'dataE.dbs', config2), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: test_splitter('FileClassSplitter', 'dataE.dbs', config2), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: test_splitter('DataSplitter', 'dataE.dbs', config2), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: PartitionResyncHandler(None).resync(None, None, None, None), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: PartitionReader(None).get_partition_unchecked(123), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: Plugin.create_instance('PartitionWriter').save_partitions(None, None), 'AbstractError', 'is an abstract function')
	caught
	"""


class TestBlockBounarySplitter(object):
	"""
	>>> splitter_block = test_splitter('BlockBoundarySplitter', 'dataE.dbs', config1)
	/path/file0: A, /path/file1: B, /path/file2: C
	AAAAAAAAAABBBBBCCCCCCCCCCCCCCC  => 30
	------------------------------  => 0,30
	/path/file3: D, /path/file5: E
	DDDDDDDDDDEEEEE  => 15
	---------------  => 0,15
	/path/file6: F, /path/file7: G, /path/file8: H, /path/file9: I
	FFFFFFFFFFGGGGGHHHHHHHHHHIIIIIIIIIIIIIII  => 40
	----------------------------------------  => 0,40

	>>> DataSplitter.FileList in splitter_block.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitter_block.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitter_block.get_needed_enums()
	False
	"""


class TestFileBounarySplitter(object):
	"""
	>>> try_catch(lambda: test_splitter('FileBoundarySplitter', 'dataE.dbs', configF), 'PartitionError', 'Invalid number of files per job')
	caught
	>>> splitter_file = test_splitter('FileBoundarySplitter', 'dataE.dbs', config1)
	/path/file0: A, /path/file1: B
	AAAAAAAAAABBBBB  => 15
	---------------  => 0,15
	/path/file2: C
	CCCCCCCCCCCCCCC  => 15
	---------------  => 0,15
	/path/file3: D, /path/file5: E
	DDDDDDDDDDEEEEE  => 15
	---------------  => 0,15
	/path/file6: F, /path/file7: G
	FFFFFFFFFFGGGGG  => 15
	---------------  => 0,15
	/path/file8: H, /path/file9: I
	HHHHHHHHHHIIIIIIIIIIIIIII  => 25
	-------------------------  => 0,25

	>>> DataSplitter.FileList in splitter_file.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitter_file.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitter_file.get_needed_enums()
	False

	>>> dpFile1 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> tmp = list(splitter_file.split_partitions(dpFile1.get_block_list_cached(show_stats=False)))
	"""


class TestFLSplitter(object):
	"""
	>>> splitter_file = test_splitter('FLSplitStacker', 'dataE.dbs', config2)
	/path/file0: A, /path/file1: B
	AAAAAAAAAABBBBB  => 15
	---------------  => 0,15
	/path/file2: C
	CCCCCCCCCCCCCCC  => 15
	---------------  => 0,15
	/path/file3: D, /path/file5: E
	DDDDDDDDDDEEEEE  => 15
	---------------  => 0,15
	/path/file6: F, /path/file7: G
	FFFFFFFFFFGGGGG  => 15
	---------------  => 0,15
	/path/file8: H, /path/file9: I
	HHHHHHHHHHIIIIIIIIIIIIIII  => 25
	---------------            => 0,15
	/path/file9: I
	          IIIIIIIIIIIIIII  => 15
	               ----------  => 5,10

	>>> DataSplitter.FileList in splitter_file.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitter_file.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitter_file.get_needed_enums()
	False
	"""


class TestEventBounarySplitter(object):
	"""
	>>> splitter_event = test_splitter('EventBoundarySplitter', 'dataE.dbs', config1)
	/path/file0: A
	AAAAAAAAAA  => 10
	----------  => 0,10
	/path/file1: B, /path/file2: C
	BBBBBCCCCCCCCCCCCCCC  => 20
	----------            => 0,10
	/path/file2: C
	     CCCCCCCCCCCCCCC  => 15
	          ----------  => 5,10
	/path/file3: D
	DDDDDDDDDD  => 10
	----------  => 0,10
	/path/file5: E
	EEEEE  => 5
	-----  => 0,5
	/path/file6: F
	FFFFFFFFFF  => 10
	----------  => 0,10
	/path/file7: G, /path/file8: H
	GGGGGHHHHHHHHHH  => 15
	----------       => 0,10
	/path/file8: H, /path/file9: I
	     HHHHHHHHHHIIIIIIIIIIIIIII  => 25
	          ----------            => 5,10
	/path/file9: I
	               IIIIIIIIIIIIIII  => 15
	                    ----------  => 5,10

	>>> DataSplitter.FileList in splitter_event.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitter_event.get_needed_enums()
	True
	>>> DataSplitter.Skipped in splitter_event.get_needed_enums()
	True

	>>> dpFile1 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> try_catch(lambda: list(splitter_event.split_partitions(dpFile1.get_block_list_cached(show_stats=False))), 'DatasetError', 'does not support files with a negative number of events')
	caught

	>>> splitter_event = test_splitter('EventBoundarySplitter', 'dataC.dbs', config1)
	/path/to//file1: A, /path/to//file2: B
	AAAAABBBBBBBBBBBBBBB  => 20
	----------            => 0,10
	/path/to//file2: B
	     BBBBBBBBBBBBBBB  => 15
	          ----------  => 5,10
	"""


class TestHybridBounarySplitter(object):
	"""
	>>> try_catch(lambda: test_splitter('HybridSplitter', 'dataE.dbs', configF), 'PartitionError', 'Invalid number of entries per job')
	caught
	>>> splitHybrid = test_splitter('HybridSplitter', 'dataE.dbs', config1)
	/path/file0: A
	AAAAAAAAAA  => 10
	----------  => 0,10
	/path/file1: B
	BBBBB  => 5
	-----  => 0,5
	/path/file2: C
	CCCCCCCCCCCCCCC  => 15
	---------------  => 0,15
	/path/file3: D
	DDDDDDDDDD  => 10
	----------  => 0,10
	/path/file5: E
	EEEEE  => 5
	-----  => 0,5
	/path/file6: F
	FFFFFFFFFF  => 10
	----------  => 0,10
	/path/file7: G
	GGGGG  => 5
	-----  => 0,5
	/path/file8: H
	HHHHHHHHHH  => 10
	----------  => 0,10
	/path/file9: I
	IIIIIIIIIIIIIII  => 15
	---------------  => 0,15

	>>> DataSplitter.FileList in splitHybrid.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitHybrid.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitHybrid.get_needed_enums()
	False

	>>> dpFile1 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> tmp = list(splitHybrid.split_partitions(dpFile1.get_block_list_cached(show_stats=False)))

	>>> splitHybrid = test_splitter('HybridSplitter', 'dataC.dbs', config1)
	/path/to//file1: A
	AAAAA  => 5
	-----  => 0,5
	/path/to//file2: B
	BBBBBBBBBBBBBBB  => 15
	---------------  => 0,15
	"""


class TestUserMetadataSplitter(object):
	"""
	>>> splitRun = test_splitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict={'dataset': {'split metadata': 'NOTHING'}}))
	file1: A, file2: B, file3: C, filex: D
	AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDD  => 40
	----------------------------------------  => 0,40

	>>> splitRun = test_splitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict={'dataset': {'split metadata': 'KEY1'}}))
	file1: A, file2: B, file3: C, filex: D
	AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDD  => 40
	----------------------------------------  => 0,40

	>>> splitRun = test_splitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict={'dataset': {'split metadata': 'KEY2'}}))
	filex: D
	DDDDDDDDDD  => 10
	----------  => 0,10
	file1: A, file2: B, file3: C
	AAAAAAAAAABBBBBBBBBBCCCCCCCCCC  => 30
	------------------------------  => 0,30

	>>> splitRun = test_splitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict={'dataset': {'split metadata': 'KEY3'}}))
	filex: D
	DDDDDDDDDD  => 10
	----------  => 0,10
	file1: A
	AAAAAAAAAA  => 10
	----------  => 0,10
	file2: B
	BBBBBBBBBB  => 10
	----------  => 0,10
	file3: C
	CCCCCCCCCC  => 10
	----------  => 0,10
	"""


class TestRunSplitter(object):
	"""
	>>> splitRun = test_splitter('RunSplitter', 'dataG.dbs', config1)
	3240252F.root: D
	DDDDD  => 5
	-----  => 0,5
	FA648234.root: L
	LLLLLL  => 6
	------  => 0,6
	66690367.root: E
	EEEEEEEEE  => 9
	---------  => 0,9
	04F2FC24.root: B
	BBBBBBBB  => 8
	--------  => 0,8
	047B67E9.root: A
	AAAA  => 4
	----  => 0,4
	E62E1DFF.root: I
	II  => 2
	--  => 0,2
	880C71DC.root: H, F8D11ADA.root: J, 720D3ADD.root: F
	HHHHHHJJFFFF  => 12
	------------  => 0,12
	FA07400E.root: K, 7E81320A.root: G
	KKKKKKKKGGGGGGGGG  => 17
	-----------------  => 0,17
	2AE92A85.root: C
	CCCCCCCCC  => 9
	---------  => 0,9

	>>> splitRun = test_splitter('RunSplitter', 'dataG.dbs', config1a)
	3240252F.root: D
	DDDDD  => 5
	-----  => 0,5
	FA648234.root: L
	LLLLLL  => 6
	------  => 0,6
	66690367.root: E
	EEEEEEEEE  => 9
	---------  => 0,9
	04F2FC24.root: B
	BBBBBBBB  => 8
	--------  => 0,8
	047B67E9.root: A
	AAAA  => 4
	----  => 0,4
	E62E1DFF.root: I
	II  => 2
	--  => 0,2
	880C71DC.root: H, FA07400E.root: K, F8D11ADA.root: J, 7E81320A.root: G, 720D3ADD.root: F
	HHHHHHKKKKKKKKJJGGGGGGGGGFFFF  => 29
	-----------------------------  => 0,29
	2AE92A85.root: C
	CCCCCCCCC  => 9
	---------  => 0,9
	"""


run_test()
