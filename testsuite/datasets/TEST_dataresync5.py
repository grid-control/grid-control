#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, testfwk_remove_files
from grid_control.datasets import DataProvider, DataSplitter
from testDS import display_reader
from testResync import do_first_split, do_resync
from python_compat import irange


DP = DataProvider

# Resync with changing file in the middle of the block

def getBlock5(files):
	fl = []
	for i in files:
		fl.append({DP.NEntries: -1, DP.URL: '/path/file_%d' % i})
	return [{
		DP.BlockName: 'block1', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'],
		DP.Nickname: 'TESTNICK', DP.FileList: fl
	}]

class Test_SplitterResync5:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', getBlock5(irange(5)))
	>>> splitter = do_first_split(splitter_name='FileBoundarySplitter')
	/path/file_0: A
	/path/file_1: B
	/path/file_2: C
	/path/file_3: D
	/path/file_4: E
	ABC  => 3
	---  => 0,-3
	DE  => 2
	--  => 0,-2

	>>> reader = DataSplitter.load_partitions('datamap-resync.tar')
	>>> display_reader(reader, meta=True)
	-----
	 x | SIZE | SKIP |                      Files                       | Metadata | Invalid
	===+======+======+==================================================+==========+=========
	 0 |  -3  |      | ['/path/file_0', '/path/file_1', '/path/file_2'] |          |
	 1 |  -2  |      | ['/path/file_3', '/path/file_4']                 |          |
	-----

	>>> DataProvider.save_to_file('data-new.dbs', getBlock5(irange(5)))
	>>> do_resync(splitter)
	([], [], False)
	/path/file_0: A
	/path/file_1: B
	/path/file_2: C
	/path/file_3: D
	/path/file_4: E
	ABC  => 3
	---  => 0,-3
	DE  => 2
	--  => 0,-2

	>>> reader = DataSplitter.load_partitions('datamap-resync.tar')
	>>> display_reader(reader, meta=True)
	-----
	 x | SIZE | SKIP |                      Files                       | Metadata | Invalid
	===+======+======+==================================================+==========+=========
	 0 |  -3  |      | ['/path/file_0', '/path/file_1', '/path/file_2'] |          |
	 1 |  -2  |      | ['/path/file_3', '/path/file_4']                 |          |
	-----

	>>> DataProvider.save_to_file('data-new.dbs', getBlock5(irange(6)))
	>>> do_resync(splitter)
	([], [], True)
	/path/file_0: A
	/path/file_1: B
	/path/file_2: C
	/path/file_3: D
	/path/file_4: E
	/path/file_5: F
	ABC  => 3
	---  => 0,-3
	DE  => 2
	--  => 0,-2
	F  => 1
	-  => 0,-1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock5(irange(6)))
	>>> do_resync(splitter)
	([], [], False)
	/path/file_0: A
	/path/file_1: B
	/path/file_2: C
	/path/file_3: D
	/path/file_4: E
	/path/file_5: F
	ABC  => 3
	---  => 0,-3
	DE  => 2
	--  => 0,-2
	F  => 1
	-  => 0,-1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock5(irange(3)))
	>>> do_resync(splitter)
	([], [1, 2], True)
	/path/file_0: A
	/path/file_1: B
	/path/file_2: C
	ABC  => 3
	---  => 0,-3

	>>> DataProvider.save_to_file('data-new.dbs', getBlock5(irange(4)))
	>>> do_resync(splitter)
	([], [], True)
	/path/file_0: A
	/path/file_1: B
	/path/file_2: C
	/path/file_3: D
	ABC  => 3
	---  => 0,-3
	D  => 1
	-  => 0,-1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock5([1,2,4]))

	>>> testfwk_remove_files(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
