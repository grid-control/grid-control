#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import run_test, testfwk_remove_files
from grid_control.datasets import DataProvider as DP
from testResync import do_first_split, do_resync


def get_blocks(num_events):
	# Resync with changing file at the beginning of the block
	return [{
		DP.BlockName: 'block1', DP.Dataset: '/MY/DATASET', DP.Locations: ['SE1', 'SE2'],
		DP.Nickname: 'TESTNICK', DP.Metadata: ['KEY1', 'KEY2'],
		DP.FileList: [
			{DP.NEntries: num_events, DP.URL: '/path/file_x', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: 1, DP.URL: '/path/file_y', DP.Metadata: [[1,2,3], "Test1"]},
			{DP.NEntries: 4, DP.URL: '/path/file_z', DP.Metadata: [[1,2,3], "Test1"]},
		],
	}]


class Test_SplitterResync3:
	"""
	# Initial splitting
	>>> DP.save_to_file('data-old.dbs', get_blocks(4))
	>>> splitter = do_first_split()
	/path/file_x: A
	/path/file_y: B
	/path/file_z: C
	AAAA  => 4
	---   => 0,3
	AAAABCCCC  => 9
	   ---     => 3,3
	     CCCC  => 4
	      ---  => 1,3

	>>> DP.save_to_file('data-new.dbs', get_blocks(4))
	>>> do_resync(splitter)
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

	>>> DP.save_to_file('data-new.dbs', get_blocks(7))
	>>> do_resync(splitter)
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

	>>> DP.save_to_file('data-new.dbs', get_blocks(7))
	>>> do_resync(splitter)
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

	>>> DP.save_to_file('data-new.dbs', get_blocks(10))
	>>> do_resync(splitter)
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

	>>> DP.save_to_file('data-new.dbs', get_blocks(8))
	>>> do_resync(splitter)
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

	>>> DP.save_to_file('data-new.dbs', get_blocks(8))
	>>> do_resync(splitter)
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

	>>> DP.save_to_file('data-new.dbs', get_blocks(11))
	>>> do_resync(splitter)
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

	>>> testfwk_remove_files(['data-new.dbs', 'data-old.dbs', 'datamap-resync.tar', 'datamap-resync-new.tar'])
	"""

run_test()
