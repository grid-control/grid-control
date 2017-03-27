#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, testfwk_remove_files
from testfwk_datasets import get_dataset_block
from grid_control.datasets import DataProvider
from testResync import do_first_split, do_resync


def get_blocks(num_events):
	# Resync with changing file at the end of the block
	return [get_dataset_block([
			{DataProvider.NEntries: 4, DataProvider.URL: '/path/file_a', DataProvider.Metadata: [[1,2,3], "Test1"]},
			{DataProvider.NEntries: 1, DataProvider.URL: '/path/file_b', DataProvider.Metadata: [[1,2,3], "Test1"]},
			{DataProvider.NEntries: num_events, DataProvider.URL: '/path/file_c', DataProvider.Metadata: [[1,2,3], "Test1"]},
		], ['KEY1', 'KEY2'])]


class Test_SplitterResync2:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', get_blocks(5))
	>>> splitter = do_first_split()
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(5))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(7))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(7))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(10))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(8))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(8))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(11))
	>>> do_resync(splitter)
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

	>>> testfwk_remove_files(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
