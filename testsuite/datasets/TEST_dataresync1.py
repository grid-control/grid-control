#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import remove_files_testsuite, run_test
from grid_control.datasets import DataProvider
from testResync import doResync, do_initSplit


DP = DataProvider

# Changes in block with single file

def getBlock1(nEvent):
	return [{
		DP.BlockName: 'block1', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'],
		DP.Nickname: 'TESTNICK', DP.Metadata: ['KEY1', 'KEY2'],
		DP.FileList: [{DP.NEntries: nEvent, DP.URL: '/path/file1', DP.Metadata: [[1,2,3], "Test1"]}],
	}]

class Test_SplitterResync1:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', getBlock1(5))
	>>> do_initSplit()
	/path/file1: A
	AAAAA  => 5
	---    => 0,3
	AAAAA  => 5
	   --  => 3,2

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(5))
	>>> doResync()
	([], [], False)
	/path/file1: A
	AAAAA  => 5
	---    => 0,3
	AAAAA  => 5
	   --  => 3,2

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(7))
	>>> doResync()
	([], [], True)
	/path/file1: A
	AAAAAAA  => 7
	---      => 0,3
	AAAAAAA  => 7
	   --    => 3,2
	AAAAAAA  => 7
	     --  => 5,2

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(7))
	>>> doResync()
	([], [], False)
	/path/file1: A
	AAAAAAA  => 7
	---      => 0,3
	AAAAAAA  => 7
	   --    => 3,2
	AAAAAAA  => 7
	     --  => 5,2

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(10))
	>>> doResync()
	([], [], True)
	/path/file1: A
	AAAAAAAAAA  => 10
	---         => 0,3
	AAAAAAAAAA  => 10
	   --       => 3,2
	AAAAAAAAAA  => 10
	     --     => 5,2
	AAAAAAAAAA  => 10
	       ---  => 7,3

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(8))
	>>> doResync()
	([], [3], True)
	/path/file1: A
	AAAAAAAA  => 8
	---       => 0,3
	AAAAAAAA  => 8
	   --     => 3,2
	AAAAAAAA  => 8
	     --   => 5,2
	AAAAAAAA  => 8    <disabled>
	       ---  => 7,3
	AAAAAAAA  => 8
	       -  => 7,1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(8))
	>>> doResync()
	([], [], False)
	/path/file1: A
	AAAAAAAA  => 8
	---       => 0,3
	AAAAAAAA  => 8
	   --     => 3,2
	AAAAAAAA  => 8
	     --   => 5,2
	AAAAAAAA  => 8    <disabled>
	       ---  => 7,3
	AAAAAAAA  => 8
	       -  => 7,1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock1(11))
	>>> doResync()
	([], [], True)
	/path/file1: A
	AAAAAAAAAAA  => 11
	---          => 0,3
	AAAAAAAAAAA  => 11
	   --        => 3,2
	AAAAAAAAAAA  => 11
	     --      => 5,2
	AAAAAAAAAAA  => 11    <disabled>
	       ---   => 7,3
	AAAAAAAAAAA  => 11
	       -     => 7,1
	AAAAAAAAAAA  => 11
	        ---  => 8,3

	>>> remove_files_testsuite(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
