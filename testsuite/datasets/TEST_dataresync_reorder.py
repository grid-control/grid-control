#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
from testfwk import create_config, run_test, testfwk_remove_files
from grid_control.datasets import DataProvider, DataSplitter
from testDS import checkCoverage, modDS, ss2bl
from python_compat import irange


def part_str2part_list(ss):
	processed = {}
	evCount = 0
	skipped = 0
	fileList = []
	for x in ss.strip() + ' ':
		if x == '-':
			fileList = []
			yield {DataSplitter.Dataset: 'Dataset', DataSplitter.BlockName: 'Block',
				DataSplitter.Invalid: True, DataSplitter.FileList: ['?'], DataSplitter.NEntries: 0}
		elif x == ' ':
			if fileList:
				yield {DataSplitter.Dataset: 'Dataset', DataSplitter.BlockName: 'Block',
					DataSplitter.NEntries: evCount, DataSplitter.Skipped: skipped,
					DataSplitter.FileList: list(fileList)}
			evCount = 0
			fileList = []
		else:
			if x not in fileList:
				fileList.append(x)
			if evCount == 0:
				skipped = processed.get(x, 0)
			processed[x] = processed.get(x, 0) + 1
			evCount += 1


def part_list2part_str(bl, sl):
	lenDict = {}
	sl = list(sl)
	if len(bl):
		for fi in bl[0][DataProvider.FileList]:
			lenDict[fi[DataProvider.URL]] = fi[DataProvider.NEntries]
	result = ''# + str(len(sl))
	for si in sl:
		result += ' '
		if si.get(DataSplitter.Invalid, False):
			result += '-'
			continue
		fileIdx = 0
		fileEvIdx = si[DataSplitter.Skipped]
		splitStr = ''
		for x in irange(si[DataSplitter.NEntries]):
			splitStr += si[DataSplitter.FileList][fileIdx]
			fileEvIdx += 1
			if fileEvIdx == lenDict[si[DataSplitter.FileList][fileIdx]]:
				fileIdx += 1
				fileEvIdx = 0
		result += splitStr
		assert(len(splitStr) == si[DataSplitter.NEntries])
	return result.strip()


def test_resync(ss, modstr, mode, num_events=3):
	config = create_config(config_dict={'mysection': {'events per job': num_events, 'resync jobs': mode}})

	data_raw = ss2bl(ss)
	data_mod = modDS(data_raw, modstr)

	splitter = DataSplitter.create_instance('EventBoundarySplitter', config, 'dataset')
	DataSplitter.save_partitions('short_raw.tar', part_str2part_list(ss))
	reader_raw = DataSplitter.load_partitions('short_raw.tar')
	checkCoverage(reader_raw, data_raw)

	resync_result = splitter.get_resync_handler().resync(splitter, reader_raw, data_raw, data_mod)
	DataSplitter.save_partitions('short_mod.tar', resync_result.partition_iter)

	reader_mod = DataSplitter.load_partitions('short_mod.tar')
	checkCoverage(reader_mod, data_mod)

	testfwk_remove_files(['short_raw.tar', 'short_mod.tar'])
	return part_list2part_str(data_mod, reader_mod.iter_partitions())


class Test_SplitterTestShort:
	"""
	>>> test_resync('XA BY CZD', 'X:0 Y:0 Z:0', mode='preserve')
	'A B CD'
	>>> test_resync('XX XAA BBY YY CCZZDD', 'X:0 Y:0 Z:0', mode='preserve')
	'- AA BB - CCDD'
	>>> test_resync('AA AB BB', 'A:0', mode='preserve')
	'- B BB'
	>>> test_resync('AA AB BB', 'B:0', mode='preserve')
	'AA A'
	>>> test_resync('ABBCCCD', 'B:3 C:2', mode='preserve')
	'ABBBCCD'
	>>> test_resync('AA A AA AAA AABBBC CCC DDEEE EFFFGHI III', 'A:0 J:5 D:6 E:3', mode='preserve')
	'- - - - BBBC CCC DDDDDDEEE FFFGHI III JJJ JJ'
	"""


class Test_SplitterTestModes:
	"""
	>>> test_resync('A A A', 'A:0', mode='preserve')
	''
	>>> test_resync('A A A', 'A:5', mode='preserve')
	'A A A AA'
	>>> test_resync('A - A - A', '', mode='preserve')
	'A - A - A'

	>>> test_resync('A - A - A - -', '', mode='preserve')
	'A - A - A'
	>>> test_resync('A - A - A - -', '', mode='append')
	'A - A - A'
	>>> test_resync('A - A - A - -', '', mode='reorder')
	'A A A'
	>>> test_resync('A - A - A - -', '', mode='fillgap')
	'A - A - A'

	>>> test_resync('A - A - A', 'B:2', mode='preserve')
	'A - A - A BB'
	>>> test_resync('A - A - A', 'B:2', mode='append')
	'A - A - A BB'
	>>> test_resync('A - A - A', 'B:2', mode='reorder')
	'A BB A A'
	>>> test_resync('A - A - A', 'B:2', mode='fillgap')
	'A BB A - A'

	>>> test_resync('A - A - A', 'B:5', mode='preserve')
	'A - A - A BBB BB'
	>>> test_resync('A - A - A', 'B:5', mode='append')
	'A - A - A BBB BB'
	>>> test_resync('A - A - A', 'B:5', mode='reorder')
	'A BB A BBB A'
	>>> test_resync('A - A - A', 'B:5', mode='fillgap')
	'A BBB A BB A'

	>>> test_resync('A - A - A', 'A:5', mode='preserve')
	'A - A - A AA'
	>>> test_resync('A - A - A', 'A:5', mode='append')
	'A - A - A AA'
	>>> test_resync('A - A - A', 'A:5', mode='reorder')
	'A AA A A'
	>>> test_resync('A - A - A', 'A:5', mode='fillgap')
	'A AA A - A'

	>>> test_resync('A - A - A', 'A:8', mode='preserve')
	'A - A - A AAA AA'
	>>> test_resync('A - A - A', 'A:8', mode='append')
	'A - A - A AAA AA'
	>>> test_resync('A - A - A', 'A:8', mode='reorder')
	'A AA A AAA A'
	>>> test_resync('A - A - A', 'A:8', mode='fillgap')
	'A AAA A AA A'

	>>> test_resync('A - A - A', 'A:11', mode='preserve')
	'A - A - A AAA AAA AA'
	>>> test_resync('A - A - A', 'A:11', mode='append')
	'A - A - A AAA AAA AA'
	>>> test_resync('A - A - A', 'A:11', mode='reorder')
	'A AA A AAA A AAA'
	>>> test_resync('A - A - A', 'A:11', mode='fillgap')
	'A AAA A AAA A AA'

	>>> test_resync('A - AA - A', 'A:2', mode='preserve')
	'A - A'
	>>> test_resync('A - AA - A', 'A:2', mode='append')
	'A - - - - A'
	>>> test_resync('A - AA - A', 'A:2', mode='reorder')
	'A A'
	>>> test_resync('A - AA - A', 'A:2', mode='fillgap')
	'A - A'
	"""


run_test()
