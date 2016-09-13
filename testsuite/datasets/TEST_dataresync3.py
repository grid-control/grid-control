#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import remove_files_testsuite, run_test
from grid_control.datasets import DataProvider
from testResync import doResync, do_initSplit


DP = DataProvider

# Resync with changing file at the beginning of the block

def getBlock3(nEvent):
	return [{
		DP.BlockName: 'block1', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'],
		DP.Nickname: 'TESTNICK', DP.Metadata: ['KEY1', 'KEY2'],
		DP.FileList: [
			{DP.NEntries: nEvent, DP.URL: '/path/file_x', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: 1, DP.URL: '/path/file_y', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: 4, DP.URL: '/path/file_z', DP.Metadata: [[1,2,3], "Test1"]},
		],
	}]

class Test_SplitterResync3:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', getBlock3(4))
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

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(4))
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

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(7))
	>>> doResync()
	([], [1], True)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAAAAA  => 7
	---      => 0,3
	AAAAAAABCCCC  => 12    <disabled>
	   ---        => 3,3
	        CCCC  => 4
	         ---  => 1,3
	AAAAAAABCCCC  => 12
	   ------     => 3,6

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(7))
	>>> doResync()
	([], [], False)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAAAAA  => 7
	---      => 0,3
	AAAAAAABCCCC  => 12    <disabled>
	   ---        => 3,3
	        CCCC  => 4
	         ---  => 1,3
	AAAAAAABCCCC  => 12
	   ------     => 3,6

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(10))
	>>> doResync()
	([], [3], True)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAAAAAAAA  => 10
	---         => 0,3
	AAAAAAAAAABCCCC  => 15    <disabled>
	   ---           => 3,3
	           CCCC  => 4
	            ---  => 1,3
	AAAAAAAAAABCCCC  => 15    <disabled>
	   ------        => 3,6
	AAAAAAAAAABCCCC  => 15
	   ---------     => 3,9

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(8))
	>>> doResync()
	([], [4], True)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAAAAAA  => 8
	---       => 0,3
	AAAAAAAABCCCC  => 13    <disabled>
	   ---         => 3,3
	         CCCC  => 4
	          ---  => 1,3
	AAAAAAAABCCCC  => 13    <disabled>
	   ------      => 3,6
	AAAAAAAABCCCC  => 13    <disabled>
	   ---------   => 3,9
	AAAAAAAABCCCC  => 13
	   -------     => 3,7

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(8))
	>>> doResync()
	([], [], False)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAAAAAA  => 8
	---       => 0,3
	AAAAAAAABCCCC  => 13    <disabled>
	   ---         => 3,3
	         CCCC  => 4
	          ---  => 1,3
	AAAAAAAABCCCC  => 13    <disabled>
	   ------      => 3,6
	AAAAAAAABCCCC  => 13    <disabled>
	   ---------   => 3,9
	AAAAAAAABCCCC  => 13
	   -------     => 3,7

	>>> DataProvider.save_to_file('data-new.dbs', getBlock3(11))
	>>> doResync()
	([], [5], True)
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAAAAAAAAA  => 11
	---          => 0,3
	AAAAAAAAAAABCCCC  => 16    <disabled>
	   ---            => 3,3
	            CCCC  => 4
	             ---  => 1,3
	AAAAAAAAAAABCCCC  => 16    <disabled>
	   ------         => 3,6
	AAAAAAAAAAABCCCC  => 16    <disabled>
	   ---------      => 3,9
	AAAAAAAAAAABCCCC  => 16    <disabled>
	   -------        => 3,7
	AAAAAAAAAAABCCCC  => 16
	   ----------     => 3,10

	>>> remove_files_testsuite(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
