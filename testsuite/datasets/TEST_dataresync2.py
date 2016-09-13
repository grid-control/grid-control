#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import remove_files_testsuite, run_test
from grid_control.datasets import DataProvider
from testResync import doResync, do_initSplit


DP = DataProvider

# Resync with changing file at the end of the block

def getBlock2(nEvent):
	return [{
		DP.BlockName: 'block1', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'],
		DP.Nickname: 'TESTNICK', DP.Metadata: ['KEY1', 'KEY2'],
		DP.FileList: [
			{DP.NEntries: 4, DP.URL: '/path/file_a', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: 1, DP.URL: '/path/file_b', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: nEvent, DP.URL: '/path/file_c', DP.Metadata: [[1,2,3], "Test1"]},
		],
	}]

class Test_SplitterResync2:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', getBlock2(5))
	>>> do_initSplit()
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCC  => 10
	   ---      => 3,3
	     CCCCC  => 5
	      ---   => 1,3
	     CCCCC  => 5
	         -  => 4,1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(5))
	>>> doResync()
	([], [], False)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCC  => 10
	   ---      => 3,3
	     CCCCC  => 5
	      ---   => 1,3
	     CCCCC  => 5
	         -  => 4,1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(7))
	>>> doResync()
	([], [], True)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCCCC  => 12
	   ---        => 3,3
	     CCCCCCC  => 7
	      ---     => 1,3
	     CCCCCCC  => 7
	         -    => 4,1
	     CCCCCCC  => 7
	          --  => 5,2

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(7))
	>>> doResync()
	([], [], False)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCCCC  => 12
	   ---        => 3,3
	     CCCCCCC  => 7
	      ---     => 1,3
	     CCCCCCC  => 7
	         -    => 4,1
	     CCCCCCC  => 7
	          --  => 5,2

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(10))
	>>> doResync()
	([], [], True)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCCCCCCC  => 15
	   ---           => 3,3
	     CCCCCCCCCC  => 10
	      ---        => 1,3
	     CCCCCCCCCC  => 10
	         -       => 4,1
	     CCCCCCCCCC  => 10
	          --     => 5,2
	     CCCCCCCCCC  => 10
	            ---  => 7,3

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(8))
	>>> doResync()
	([], [5], True)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCCCCC  => 13
	   ---         => 3,3
	     CCCCCCCC  => 8
	      ---      => 1,3
	     CCCCCCCC  => 8
	         -     => 4,1
	     CCCCCCCC  => 8
	          --   => 5,2
	     CCCCCCCC  => 8    <disabled>
	            ---  => 7,3
	     CCCCCCCC  => 8
	            -  => 7,1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(8))
	>>> doResync()
	([], [], False)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCCCCC  => 13
	   ---         => 3,3
	     CCCCCCCC  => 8
	      ---      => 1,3
	     CCCCCCCC  => 8
	         -     => 4,1
	     CCCCCCCC  => 8
	          --   => 5,2
	     CCCCCCCC  => 8    <disabled>
	            ---  => 7,3
	     CCCCCCCC  => 8
	            -  => 7,1

	>>> DataProvider.save_to_file('data-new.dbs', getBlock2(11))
	>>> doResync()
	([], [], True)
	/path/file_a: A
	/path/file_b: B
	/path/file_c: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCCCCCCCCC  => 16
	   ---            => 3,3
	     CCCCCCCCCCC  => 11
	      ---         => 1,3
	     CCCCCCCCCCC  => 11
	         -        => 4,1
	     CCCCCCCCCCC  => 11
	          --      => 5,2
	     CCCCCCCCCCC  => 11    <disabled>
	            ---   => 7,3
	     CCCCCCCCCCC  => 11
	            -     => 7,1
	     CCCCCCCCCCC  => 11
	             ---  => 8,3

	>>> remove_files_testsuite(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
