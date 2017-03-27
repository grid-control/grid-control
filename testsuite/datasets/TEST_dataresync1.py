#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, testfwk_remove_files
from testfwk_datasets import get_dataset_block
from grid_control.datasets import DataProvider
from testResync import do_first_split, do_resync


def get_blocks(num_events):
	# Changes in block with single file
	return [get_dataset_block([{
			DataProvider.NEntries: num_events,
			DataProvider.URL: '/path/file1',
			DataProvider.Metadata: [[1,2,3], "Test1"]
		}], ['KEY1', 'KEY2'])]


class Test_SplitterResync1:
	"""
	# Initial splitting
	>>> DataProvider.save_to_file('data-old.dbs', get_blocks(5))
	>>> splitter = do_first_split()
	/path/file1: A
	AAAAA  => 5
	---    => 0,3
	AAAAA  => 5
	   --  => 3,2

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(5))
	>>> do_resync(splitter)
	([], [], False)
	/path/file1: A
	AAAAA  => 5
	---    => 0,3
	AAAAA  => 5
	   --  => 3,2

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(7))
	>>> do_resync(splitter)
	([], [], True)
	/path/file1: A
	AAAAAAA  => 7
	---      => 0,3
	AAAAAAA  => 7
	   --    => 3,2
	AAAAAAA  => 7
	     --  => 5,2

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(7))
	>>> do_resync(splitter)
	([], [], False)
	/path/file1: A
	AAAAAAA  => 7
	---      => 0,3
	AAAAAAA  => 7
	   --    => 3,2
	AAAAAAA  => 7
	     --  => 5,2

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(10))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(8))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(8))
	>>> do_resync(splitter)
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

	>>> DataProvider.save_to_file('data-new.dbs', get_blocks(11))
	>>> do_resync(splitter)
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

	>>> testfwk_remove_files(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
