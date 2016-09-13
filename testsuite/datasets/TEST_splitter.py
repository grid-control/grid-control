#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import create_config, remove_files_testsuite, run_test, try_catch
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.datasets.splitter_io import DataSplitterIO
from grid_control.utils.activity import ProgressActivity
from testDS import getLFNMap, printSplit, printSplitNice


remove_files_testsuite(['datamap.tar', 'datamap-new.tar'])

config1 = create_config(config_dict = {
	'somesection': {
		'resync interactive': 'False',
		'events per job': '10',
		'files per job': '2',
	},
})

config1a = create_config(config_dict = {
	'somesection': {
		'resync interactive': 'False',
		'events per job': '10',
		'files per job': '2',
		'run range': 2,
	},
})

config2 = create_config(config_dict = {
	'myexampletask': {
		'events per job': '15',
		'files per job': '2',
		'splitter stack': 'BlockBoundarySplitter FileBoundarySplitter EventBoundarySplitter',
	},
})

config3 = create_config(config_dict = {
	'dataset0': {'resync interactive': 'False', 'events per job': '7'},
})


def testSplitter(name, datafile, config):
	datasrc = DataProvider.load_from_file(datafile)
	splitter = DataSplitter.create_instance(name, config, 'dataset')
	splitter.partition_blocks('datamap.tar', datasrc.get_block_list_cached(show_stats = False))
	datamap = getLFNMap(datasrc)
	printSplitNice(splitter, datamap)
	return splitter

class Test_Base:
	"""
	>>> try_catch(lambda: testSplitter('FileLevelSplitter', 'dataE.dbs', config2), 'AbstractError', 'is an abstract function')
	caught
	>>> try_catch(lambda: testSplitter('FileClassSplitter', 'dataE.dbs', config2), 'AbstractError', 'is an abstract function')
	caught
	"""

class Test_FileFormatV1:
	"""
	>>> try_catch(lambda: DataSplitter.load_partitions_for_script('datamap_missing.tar'), 'PartitionError', 'No valid dataset splitting found in')
	caught
	>>> splitter = DataSplitter.load_partitions_for_script('datamap.v1.tar')
	>>> printSplit(splitter)
	-----
	 x | SIZE | SKIP |           Files           
	===+======+======+===========================
	 0 |  2   |  0   | ['/bin/env', '/bin/arch'] 
	-----

	>>> progress = ProgressActivity('msg', splitter.get_partition_len())
	>>> v1 = DataSplitterIO.create_instance('DataSplitterIOV1')
	>>> v1.save_partitions_and_info(progress, 'datamap.v1.new.tar', splitter.iter_partitions(), {'ClassName': splitter.__class__.__name__, 'files per job': 2})
	>>> progress.finish()
	>>> printSplit(DataSplitter.load_partitions_for_script('datamap.v1.new.tar'))
	-----
	 x | SIZE | SKIP |           Files           
	===+======+======+===========================
	 0 |  2   |  0   | ['/bin/env', '/bin/arch'] 
	-----

	>>> remove_files_testsuite(['datamap.v1.new.tar'])
	"""

class Test_FileFormatV20:
	"""
	>>> splitter = DataSplitter.load_partitions_for_script('datamap.v20.tar')
	>>> printSplit(splitter)
	-----
	 x | SIZE | SKIP |             Files             
	===+======+======+===============================
	 0 |  4   |  0   | ['/bin/sh']                   
	 1 |  1   |  4   | ['/bin/sh']                   
	 2 |  2   |  0   | ['/bin/gettext', '/bin/grep'] 
	 3 |  1   |  0   | ['/bin/bash']                 
	-----
	"""

class Test_BlockBounarySplitter:
	"""
	>>> splitBlock = testSplitter('BlockBoundarySplitter', 'dataE.dbs', config1)
	/path/file0: A, /path/file1: B, /path/file2: C
	AAAAAAAAAABBBBBCCCCCCCCCCCCCCC  => 30
	------------------------------  => 0,30
	/path/file3: D, /path/file5: E
	DDDDDDDDDDEEEEE  => 15
	---------------  => 0,15
	/path/file6: F, /path/file7: G, /path/file8: H, /path/file9: I
	FFFFFFFFFFGGGGGHHHHHHHHHHIIIIIIIIIIIIIII  => 40
	----------------------------------------  => 0,40

	>>> splitBlock.get_partition_len()
	3
	>>> try_catch(lambda: splitBlock.get_partition(5), 'PartitionError', 'out of range for available dataset')
	caught
	>>> DataSplitter.FileList in splitBlock.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitBlock.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitBlock.get_needed_enums()
	False

	>>> printSplit(DataSplitter.load_partitions_for_script("datamap.tar"))
	-----
	 x | SIZE | SKIP |                            Files                             
	===+======+======+==============================================================
	 0 |  30  |      | ['/path/file0', '/path/file1', '/path/file2']                
	 1 |  15  |      | ['/path/file3', '/path/file5']                               
	 2 |  40  |      | ['/path/file6', '/path/file7', '/path/file8', '/path/file9'] 
	-----

	>>> dpFile1 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> splitBlock.partition_blocks('datamap.tar', dpFile1.get_block_list_cached(show_stats = False))
	>>> printSplit(DataSplitter.load_partitions_for_script("datamap.tar"))
	-----
	 x | SIZE | SKIP |       Files       
	===+======+======+===================
	 0 |  -1  |      | ['/path/to/file'] 
	-----

	>>> remove_files_testsuite(['datamap.tar'])
	"""

class Test_FileBounarySplitter:
	"""
	>>> splitFile = testSplitter('FileBoundarySplitter', 'dataE.dbs', config1)
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

	>>> DataSplitter.FileList in splitFile.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitFile.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitFile.get_needed_enums()
	False

	>>> dpFile1 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> splitFile.partition_blocks('datamap.tar', dpFile1.get_block_list_cached(show_stats = False))
	>>> remove_files_testsuite(['datamap.tar'])
	"""

class Test_FLSplitter:
	"""
	>>> splitFile = testSplitter('FLSplitStacker', 'dataE.dbs', config2)
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

	>>> DataSplitter.FileList in splitFile.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitFile.get_needed_enums()
	False
	>>> DataSplitter.Skipped in splitFile.get_needed_enums()
	False
	"""

class Test_EventBounarySplitter:
	"""
	>>> splitEvent = testSplitter('EventBoundarySplitter', 'dataE.dbs', config1)
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

	>>> DataSplitter.FileList in splitEvent.get_needed_enums()
	True
	>>> DataSplitter.NEntries in splitEvent.get_needed_enums()
	True
	>>> DataSplitter.Skipped in splitEvent.get_needed_enums()
	True

	>>> dpFile1 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> try_catch(lambda: splitEvent.partition_blocks('datamap.tar', dpFile1.get_block_list_cached(show_stats = False)), 'DatasetError', 'does not support files with a negative number of events')
	caught

	>>> splitEvent = testSplitter('EventBoundarySplitter', 'dataC.dbs', config1)
	/path/to//file1: A, /path/to//file2: B
	AAAAABBBBBBBBBBBBBBB  => 20
	----------            => 0,10
	/path/to//file2: B
	     BBBBBBBBBBBBBBB  => 15
	          ----------  => 5,10

	>>> remove_files_testsuite(['datamap.tar'])
	"""

class Test_HybridBounarySplitter:
	"""
	>>> splitHybrid = testSplitter('HybridSplitter', 'dataE.dbs', config1)
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
	>>> splitHybrid.partition_blocks('datamap.tar', dpFile1.get_block_list_cached(show_stats = False))

	>>> splitHybrid = testSplitter('HybridSplitter', 'dataC.dbs', config1)
	/path/to//file1: A
	AAAAA  => 5
	-----  => 0,5
	/path/to//file2: B
	BBBBBBBBBBBBBBB  => 15
	---------------  => 0,15

	>>> try_catch(lambda: splitHybrid.get_partition(10), 'PartitionError', 'Partition 10 out of range for available dataset')
	caught
	>>> remove_files_testsuite(['datamap.tar'])
	"""

class Test_UserMetadataSplitter:
	"""
	>>> splitRun = testSplitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict = {'dataset': {'split metadata': 'NOTHING'}}))
	file1: A, file2: B, file3: C, filex: D
	AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDD  => 40
	----------------------------------------  => 0,40

	>>> splitRun = testSplitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict = {'dataset': {'split metadata': 'KEY1'}}))
	file1: A, file2: B, file3: C, filex: D
	AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDDDDDDDDDD  => 40
	----------------------------------------  => 0,40

	>>> splitRun = testSplitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict = {'dataset': {'split metadata': 'KEY2'}}))
	filex: D
	DDDDDDDDDD  => 10
	----------  => 0,10
	file1: A, file2: B, file3: C
	AAAAAAAAAABBBBBBBBBBCCCCCCCCCC  => 30
	------------------------------  => 0,30

	>>> splitRun = testSplitter('UserMetadataSplitter', 'dataK.dbs', create_config(config_dict = {'dataset': {'split metadata': 'KEY3'}}))
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

	>>> remove_files_testsuite(['datamap.tar'])
	"""

class Test_RunSplitter:
	"""
	>>> splitRun = testSplitter('RunSplitter', 'dataG.dbs', config1)
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

	>>> remove_files_testsuite(['datamap.tar'])

	>>> splitRun = testSplitter('RunSplitter', 'dataG.dbs', config1a)
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

	>>> remove_files_testsuite(['datamap.tar'])
	"""

class Test_SplitterResync:
	"""
	>>> dataA = DataProvider.load_from_file('dataA.dbs')
	>>> dataB = DataProvider.load_from_file('dataB.dbs')

	>>> splitA = DataSplitter.create_instance('EventBoundarySplitter', config1, 'dataset')
	>>> splitA.partition_blocks('datamap.tar', dataA.get_block_list_cached(show_stats = False))

	>>> splitB = DataSplitter.load_partitions_for_script('datamap.tar')
	>>> printSplit(splitB)
	-----
	 x  | SIZE | SKIP |           Files            
	====+======+======+============================
	 0  |  10  |  0   | ['/path/UC1']              
	 1  |  10  |  0   | ['/path/UC2', '/path/UC3'] 
	 2  |  10  |  5   | ['/path/UC3']              
	 3  |  10  |  0   | ['/path/MX1']              
	 4  |  10  |  10  | ['/path/MX1', '/path/MX2'] 
	 5  |  10  |  0   | ['/path/MX3']              
	 6  |  10  |  0   | ['/path/EX1', '/path/EX2'] 
	 7  |  10  |  5   | ['/path/EX2', '/path/EX3'] 
	 8  |  10  |  5   | ['/path/EX3']              
	 9  |  5   |  0   | ['/path/EX4']              
	 10 |  10  |  0   | ['/path/AD1']              
	 11 |  10  |  10  | ['/path/AD1', '/path/AD2'] 
	 12 |  10  |  0   | ['/path/AD3']              
	 13 |  10  |  0   | ['/path/RM1', '/path/RM2'] 
	 14 |  10  |  5   | ['/path/RM2', '/path/RM3'] 
	 15 |  10  |  5   | ['/path/RM3']              
	 16 |  10  |  0   | ['/path/RM4']              
	 17 |  10  |  0   | ['/path/SH1', '/path/SH2'] 
	 18 |  10  |  5   | ['/path/SH2']              
	 19 |  10  |  15  | ['/path/SH2', '/path/SH3'] 
	 20 |  10  |  5   | ['/path/SH3']              
	 21 |  10  |  0   | ['/path/SH4']              
	 22 |  10  |  10  | ['/path/SH4']              
	 23 |  10  |  0   | ['/path/SH0']              
	 24 |  10  |  0   | ['/path/RP1']              
	 25 |  10  |  10  | ['/path/RP1', '/path/RP2'] 
	 26 |  10  |  5   | ['/path/RP2', '/path/RP3'] 
	 27 |  10  |  0   | ['/path/BR1']              
	 28 |  10  |  0   | ['/path/SE1']              
	-----

	>>> dataAmap = getLFNMap(dataA)
	>>> charMap = {}
	>>> charMap = printSplitNice(splitA, dataAmap, reuse = charMap)
	/path/UC1: Y
	YYYYYYYYYY  => 10
	----------  => 0,10
	/path/UC2: Z, /path/UC3: 1
	ZZZZZ111111111111111  => 20
	----------            => 0,10
	/path/UC3: 1
	     111111111111111  => 15
	          ----------  => 5,10
	/path/MX1: I
	IIIIIIIIIIIIIII  => 15
	----------       => 0,10
	/path/MX1: I, /path/MX2: J
	IIIIIIIIIIIIIIIJJJJJ  => 20
	          ----------  => 10,10
	/path/MX3: K
	KKKKKKKKKK  => 10
	----------  => 0,10
	/path/EX1: E, /path/EX2: F
	EEEEEFFFFFFFFFF  => 15
	----------       => 0,10
	/path/EX2: F, /path/EX3: G
	     FFFFFFFFFFGGGGGGGGGGGGGGG  => 25
	          ----------            => 5,10
	/path/EX3: G
	               GGGGGGGGGGGGGGG  => 15
	                    ----------  => 5,10
	/path/EX4: H
	HHHHH  => 5
	-----  => 0,5
	/path/AD1: A
	AAAAAAAAAAAAAAA  => 15
	----------       => 0,10
	/path/AD1: A, /path/AD2: B
	AAAAAAAAAAAAAAABBBBB  => 20
	          ----------  => 10,10
	/path/AD3: C
	CCCCCCCCCC  => 10
	----------  => 0,10
	/path/RM1: L, /path/RM2: M
	LLLLLMMMMMMMMMM  => 15
	----------       => 0,10
	/path/RM2: M, /path/RM3: N
	     MMMMMMMMMMNNNNNNNNNNNNNNN  => 25
	          ----------            => 5,10
	/path/RM3: N
	               NNNNNNNNNNNNNNN  => 15
	                    ----------  => 5,10
	/path/RM4: O
	OOOOOOOOOO  => 10
	----------  => 0,10
	/path/SH1: U, /path/SH2: V
	UUUUUVVVVVVVVVVVVVVVVVVVV  => 25
	----------                 => 0,10
	/path/SH2: V
	     VVVVVVVVVVVVVVVVVVVV  => 20
	          ----------       => 5,10
	/path/SH2: V, /path/SH3: W
	     VVVVVVVVVVVVVVVVVVVVWWWWWWWWWWWWWWW  => 35
	                    ----------            => 15,10
	/path/SH3: W
	                         WWWWWWWWWWWWWWW  => 15
	                              ----------  => 5,10
	/path/SH4: X
	XXXXXXXXXXXXXXXXXXXX  => 20
	----------            => 0,10
	/path/SH4: X
	XXXXXXXXXXXXXXXXXXXX  => 20
	          ----------  => 10,10
	/path/SH0: T
	TTTTTTTTTT  => 10
	----------  => 0,10
	/path/RP1: P
	PPPPPPPPPPPPPPP  => 15
	----------       => 0,10
	/path/RP1: P, /path/RP2: Q
	PPPPPPPPPPPPPPPQQQQQQQQQQ  => 25
	          ----------       => 10,10
	/path/RP2: Q, /path/RP3: R
	               QQQQQQQQQQRRRRR  => 15
	                    ----------  => 5,10
	/path/BR1: D
	DDDDDDDDDD  => 10
	----------  => 0,10
	/path/SE1: S
	SSSSSSSSSS  => 10
	----------  => 0,10

	>>> splitB.resync_partitions('datamap-new.tar', dataA.get_block_list_cached(show_stats = False), dataB.get_block_list_cached(show_stats = False))
	([], [7, 13, 14, 16, 18, 19, 22, 23, 25, 26, 27])

	>>> splitB1 = DataSplitter.load_partitions_for_script('datamap-new.tar')
	>>> dataAmap.update(getLFNMap(dataB))
	>>> charMap = printSplitNice(splitB1, dataAmap, reuse = charMap)
	/path/UC1: Y
	YYYYYYYYYY  => 10
	----------  => 0,10
	/path/UC2: Z, /path/UC3: 1
	ZZZZZ111111111111111  => 20
	----------            => 0,10
	/path/UC3: 1
	     111111111111111  => 15
	          ----------  => 5,10
	/path/MX1: I
	IIIIIIIIIIIIIII  => 15
	----------       => 0,10
	/path/MX1: I, /path/MX2: J
	IIIIIIIIIIIIIIIJJJJJ  => 20
	          ----------  => 10,10
	/path/MX3: K
	KKKKKKKKKK  => 10
	----------  => 0,10
	/path/EX1: E, /path/EX2: F
	EEEEEFFFFFFFFFFFFFFFFFFFF  => 25
	----------                 => 0,10
	/path/EX2: F, /path/EX3: G
	     FFFFFFFFFFFFFFFFFFFFGGGGGGGGGGGGGGG  => 35    <disabled>
	          ----------                      => 5,10
	/path/EX3: G
	                         GGGGGGGGGGGGGGG  => 15
	                              ----------  => 5,10
	/path/EX4: H
	HHHHHHHHHH  => 10
	-----       => 0,5
	/path/AD1: A
	AAAAAAAAAAAAAAA  => 15
	----------       => 0,10
	/path/AD1: A, /path/AD2: B
	AAAAAAAAAAAAAAABBBBB  => 20
	          ----------  => 10,10
	/path/AD3: C
	CCCCCCCCCC  => 10
	----------  => 0,10
	/path/RM1: L, /path/RM2: M
	LLLLLMMMMMMMMMM  => 15    <disabled>
	----------       => 0,10
	/path/RM2: M, /path/RM3: N
	     MMMMMMMMMMNNNNNNNNNNNNNNN  => 25    <disabled>
	          ----------            => 5,10
	/path/RM3: N
	               NNNNNNNNNNNNNNN  => 15
	                    ----------  => 5,10
	<disabled partition without files>
	/path/SH1: U, /path/SH2: V
	UUUUUVVVVVVVVVV  => 15
	----------       => 0,10
	/path/SH2: V
	     VVVVVVVVVV  => 10    <disabled>
	          ----------  => 5,10
	/path/SH2: V, /path/SH3: W
	     VVVVVVVVVVWWWWWWWWWWWWWWW  => 25    <disabled>
	                    ----------  => 15,10
	/path/SH3: W
	               WWWWWWWWWWWWWWW  => 15
	                    ----------  => 5,10
	/path/SH4: X
	XXXXXXXXXXXXXXX  => 15
	----------       => 0,10
	/path/SH4: X
	XXXXXXXXXXXXXXX  => 15    <disabled>
	          ----------  => 10,10
	/path/SH0: T
	TTTTT  => 5    <disabled>
	----------  => 0,10
	/path/RP1: P
	PPPPPPPPPPPPPPP  => 15
	----------       => 0,10
	/path/RP1: P, /path/RP2: Q
	PPPPPPPPPPPPPPPQQQQQQQQQQ  => 25    <disabled>
	          ----------       => 10,10
	/path/RP2: Q, /path/RP3: R
	               QQQQQQQQQQRRRRR  => 15    <disabled>
	                    ----------  => 5,10
	<disabled partition without files>
	/path/SE1: S
	SSSSSSSSSS  => 10
	----------  => 0,10
	/path/EX2: F, /path/EX3: G
	     FFFFFFFFFFFFFFFFFFFFGGGGGGGGGGGGGGG  => 35
	          --------------------            => 5,20
	/path/EX4: H
	HHHHHHHHHH  => 10
	     -----  => 5,5
	/path/RM1: L
	LLLLL  => 5
	-----  => 0,5
	/path/RM3: N
	               NNNNNNNNNNNNNNN  => 15
	               -----            => 0,5
	/path/SH2: V
	     VVVVVVVVVV  => 10
	          -----  => 5,5
	/path/SH3: W
	               WWWWWWWWWWWWWWW  => 15
	               -----            => 0,5
	/path/SH4: X
	XXXXXXXXXXXXXXX  => 15
	          -----  => 10,5
	/path/SH0: T
	TTTTT  => 5
	-----  => 0,5
	/path/RP1: P
	PPPPPPPPPPPPPPP  => 15
	          -----  => 10,5
	/path/RP3: R
	                         RRRRR  => 5
	                         -----  => 0,5
	/path/AD4: 2
	2222222222  => 10
	----------  => 0,10
	/path/AD5: 3
	3333333333  => 10
	----------  => 0,10
	/path/BN1: 4
	4444444444  => 10
	----------  => 0,10
	/path/RP4: 5
	55555555555555555555  => 20
	----------            => 0,10
	/path/RP4: 5
	55555555555555555555  => 20
	          ----------  => 10,10

	>>> splitC = DataSplitter.load_partitions_for_script('datamap-new.tar')
	>>> splitC.resync_partitions('datamap-newer.tar', dataB.get_block_list_cached(show_stats = False), dataB.get_block_list_cached(show_stats = False))
	([], [])

	>>> remove_files_testsuite(['datamap.tar', 'datamap-new.tar', 'datamap-newer.tar'])
	"""

run_test()
