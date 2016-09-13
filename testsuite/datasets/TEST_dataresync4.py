#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import remove_files_testsuite, run_test
from grid_control.datasets import DataProvider
from testResync import doResync, do_initSplit


DP = DataProvider

# Resync with changing file in the middle of the block

def getBlock4(nEvent):
	return [{
		DP.BlockName: 'block1', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'],
		DP.Nickname: 'TESTNICK', DP.Metadata: ['KEY1', 'KEY2'],
		DP.FileList: [
			{DP.NEntries: 4, DP.URL: '/path/file_x', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: nEvent, DP.URL: '/path/file_y', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: 4, DP.URL: '/path/file_z', DP.Metadata: [[1,2,3], "Test1"]},
		],
	}]

class Test_SplitterResync4:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', getBlock4(1))
	>>> do_initSplit()
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCC  => 9
	   ---     => 3,3
	     CCCC  => 4
	      ---  => 1,3

	>>> DataProvider.save_to_file('data-new.dbs', getBlock4(1))
	>>> doResync()
	([], [], False)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCC  => 9
	   ---     => 3,3
	     CCCC  => 4
	      ---  => 1,3

	>>> DataProvider.save_to_file('data-new.dbs', getBlock4(3))
	>>> doResync()
	([], [1], True)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABBBCCCC  => 11    <disabled>
	   ---       => 3,3
	       CCCC  => 4
	        ---  => 1,3
	AAAABBBCCCC  => 11
	   -----     => 3,5

	>>> DataProvider.save_to_file('data-new.dbs', getBlock4(3))
	>>> doResync()
	([], [], False)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABBBCCCC  => 11    <disabled>
	   ---       => 3,3
	       CCCC  => 4
	        ---  => 1,3
	AAAABBBCCCC  => 11
	   -----     => 3,5

	>>> DataProvider.save_to_file('data-new.dbs', getBlock4(7))
	>>> doResync()
	([], [3], True)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABBBBBBBCCCC  => 15    <disabled>
	   ---           => 3,3
	           CCCC  => 4
	            ---  => 1,3
	AAAABBBBBBBCCCC  => 15    <disabled>
	   -----         => 3,5
	AAAABBBBBBBCCCC  => 15
	   ---------     => 3,9

	>>> DataProvider.save_to_file('data-new.dbs', getBlock4(7))
	>>> doResync()
	([], [], False)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABBBBBBBCCCC  => 15    <disabled>
	   ---           => 3,3
	           CCCC  => 4
	            ---  => 1,3
	AAAABBBBBBBCCCC  => 15    <disabled>
	   -----         => 3,5
	AAAABBBBBBBCCCC  => 15
	   ---------     => 3,9

	>>> remove_files_testsuite(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
