#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, testfwk_remove_files, try_catch
from grid_control.datasets import DataProvider, DataSplitter
from testDS import display_partitions, display_reader, get_lfn_map


config1 = create_config(config_dict={
	'somesection': {
		'resync interactive': 'False',
		'events per job': '10',
		'files per job': '2',
	},
})


class TestFileFormat(object):
	"""
	>>> try_catch(lambda: DataSplitter.load_partitions('datamap_missing.tar'), 'PartitionReaderError', 'No valid dataset splitting found in')
	caught
	"""


class TestFileFormatV1(object):
	"""
	>>> reader = DataSplitter.load_partitions('datamap.v1.tar', reader_name='version_1')
	>>> display_reader(reader)
	-----
	 x | SIZE | SKIP |           Files
	===+======+======+===========================
	 0 |  2   |  0   | ['/bin/env', '/bin/arch']
	-----

	>>> DataSplitter.save_partitions('datamap.v1.new.tar', reader.iter_partitions(), writer_name='version_1')
	>>> display_reader(DataSplitter.load_partitions('datamap.v1.new.tar', reader_name='version_1'))
	-----
	 x | SIZE | SKIP |           Files
	===+======+======+===========================
	 0 |  2   |  0   | ['/bin/env', '/bin/arch']
	-----

	>>> testfwk_remove_files(['datamap.v1.new.tar'])
	"""


class TestFileFormatV20(object):
	"""
	>>> display_reader(DataSplitter.load_partitions('datamap.v20.tar', reader_name='version_2'))
	-----
	 x | SIZE | SKIP |             Files
	===+======+======+===============================
	 0 |  4   |  0   | ['/bin/sh']
	 1 |  1   |  4   | ['/bin/sh']
	 2 |  2   |  0   | ['/bin/gettext', '/bin/grep']
	 3 |  1   |  0   | ['/bin/bash']
	-----
	"""


class TestPartitionIO(object):
	"""
	>>> dp1 = DataProvider.load_from_file('dataE.dbs')
	>>> splitter = DataSplitter.create_instance('BlockBoundarySplitter', config1, 'dataset')
	>>> partition_list = list(splitter.split_partitions(dp1.get_block_list_cached(show_stats=False)))
	>>> display_partitions(get_lfn_map(dp1), partition_list)
	/path/file0: A, /path/file1: B, /path/file2: C
	AAAAAAAAAABBBBBCCCCCCCCCCCCCCC  => 30
	------------------------------  => 0,30
	/path/file3: D, /path/file5: E
	DDDDDDDDDDEEEEE  => 15
	---------------  => 0,15
	/path/file6: F, /path/file7: G, /path/file8: H, /path/file9: I
	FFFFFFFFFFGGGGGHHHHHHHHHHIIIIIIIIIIIIIII  => 40
	----------------------------------------  => 0,40

	>>> DataSplitter.save_partitions('datamap.tar', partition_list)

	>>> reader = DataSplitter.load_partitions('datamap.tar')
	>>> reader.get_partition_len()
	3
	>>> try_catch(lambda: reader.get_partition_checked(5), 'PartitionError', 'out of range for available partitions')
	caught

	>>> display_reader(reader)
	-----
	 x | SIZE | SKIP |                            Files
	===+======+======+==============================================================
	 0 |  30  |      | ['/path/file0', '/path/file1', '/path/file2']
	 1 |  15  |      | ['/path/file3', '/path/file5']
	 2 |  40  |      | ['/path/file6', '/path/file7', '/path/file8', '/path/file9']
	-----

	>>> dp2 = DataProvider.create_instance('FileProvider', config1, 'dataset', '/path/to/file | -1', None)
	>>> DataSplitter.save_partitions('datamap.tar', list(splitter.split_partitions(dp2.get_block_list_cached(show_stats=False))))
	>>> display_reader(DataSplitter.load_partitions('datamap.tar'))
	-----
	 x | SIZE | SKIP |       Files
	===+======+======+===================
	 0 |  -1  |      | ['/path/to/file']
	-----

	>>> testfwk_remove_files(['datamap.tar'])
	"""


class Test_SplitterResync:
	"""
	>>> provider_a = DataProvider.load_from_file('dataA.dbs')
	>>> lfn_map_a = get_lfn_map(provider_a)
	>>> bl_a = provider_a.get_block_list_cached(show_stats=False)

	>>> provider_b = DataProvider.load_from_file('dataB.dbs')
	>>> lfn_map_b = get_lfn_map(provider_b)
	>>> bl_b = provider_b.get_block_list_cached(show_stats=False)

	>>> splitter = DataSplitter.create_instance('EventBoundarySplitter', config1, 'dataset')
	>>> DataSplitter.save_partitions('datamap.tar', splitter.split_partitions(bl_a))

	>>> reader = DataSplitter.load_partitions('datamap.tar')
	>>> display_reader(reader)
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

	>>> charMap = {}
	>>> charMap = display_partitions(lfn_map_a, reader.iter_partitions(), reuse=charMap)
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

	>>> resync_result = splitter.get_resync_handler().resync(splitter, reader, bl_a, bl_b)
	>>> DataSplitter.save_partitions('datamap-new.tar', resync_result.partition_iter)
	>>> (resync_result.pnum_list_redo, resync_result.pnum_list_disable)
	([], [7, 13, 14, 16, 18, 19, 22, 23, 25, 26, 27])

	>>> reader1 = DataSplitter.load_partitions('datamap-new.tar')
	>>> lfn_map_a.update(lfn_map_b)
	>>> charMap = display_partitions(lfn_map_a, reader1.iter_partitions(), reuse=charMap)
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

	>>> reader2 = DataSplitter.load_partitions('datamap-new.tar')
	>>> pnum_list_redo = []
	>>> pnum_list_disable = []
	>>> resync_result = splitter.get_resync_handler().resync(splitter, reader2, bl_b, bl_b)
	>>> tmp = list(resync_result.partition_iter)
	>>> (resync_result.pnum_list_redo, resync_result.pnum_list_disable)
	([], [])

	>>> testfwk_remove_files(['datamap.tar', 'datamap-new.tar', 'datamap-newer.tar'])
	"""

run_test()
