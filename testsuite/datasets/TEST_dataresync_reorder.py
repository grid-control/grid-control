#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
from testFwk import create_config, remove_files_testsuite, run_test
from grid_control.datasets import DataProvider, DataSplitter
from testDS import checkCoverage, modDS, ss2bl
from python_compat import irange


def ss2sl(ss):
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

def sl2ss(bl, sl):
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


def testResync(ss, modstr, mode, nEvent = 3):
	config = create_config(config_dict = {'mysection': {'events per job': nEvent, 'resync jobs': mode}})

	data_raw = ss2bl(ss)
	DataProvider.save_to_file('short_raw.dbs', data_raw)
	DataProvider.save_to_file('short_mod.dbs', modDS(data_raw, modstr))
	data_raw = DataProvider.load_from_file('short_raw.dbs').get_block_list_cached(show_stats = False)
	data_mod = DataProvider.load_from_file('short_mod.dbs').get_block_list_cached(show_stats = False)

	splitting_tmp = DataSplitter.create_instance('EventBoundarySplitter', config, 'dataset')
	splitting_tmp.partition_blocks('short_raw.tar', data_raw)
	splitting_tmp.save_partitions('short_raw.tar', ss2sl(ss))

	splitting_raw = DataSplitter.load_partitions_for_script('short_raw.tar', config)
	splitting_raw.resync_partitions('short_mod.tar', data_raw, data_mod)
	checkCoverage(splitting_raw, data_raw)

	splitting_mod = DataSplitter.load_partitions_for_script('short_mod.tar')
	checkCoverage(splitting_mod, data_mod)

	remove_files_testsuite(['short_raw.tar', 'short_raw.dbs', 'short_mod.tar', 'short_mod.dbs'])
	return sl2ss(data_mod, splitting_mod.iter_partitions())

class Test_SplitterTestShort:
	"""
	>>> testResync('XA BY CZD', 'X:0 Y:0 Z:0', mode = 'preserve')
	'A B CD'
	>>> testResync('XX XAA BBY YY CCZZDD', 'X:0 Y:0 Z:0', mode = 'preserve')
	'- AA BB - CCDD'
	>>> testResync('AA AB BB', 'A:0', mode = 'preserve')
	'- B BB'
	>>> testResync('AA AB BB', 'B:0', mode = 'preserve')
	'AA A'
	>>> testResync('ABBCCCD', 'B:3 C:2', mode = 'preserve')
	'ABBBCCD'
	>>> testResync('AA A AA AAA AABBBC CCC DDEEE EFFFGHI III', 'A:0 J:5 D:6 E:3', mode = 'preserve')
	'- - - - BBBC CCC DDDDDDEEE FFFGHI III JJJ JJ'
	"""

class Test_SplitterTestModes:
	"""
	>>> testResync('A A A', 'A:0', mode = 'preserve')
	''
	>>> testResync('A A A', 'A:5', mode = 'preserve')
	'A A A AA'
	>>> testResync('A - A - A', '', mode = 'preserve')
	'A - A - A'

	>>> testResync('A - A - A - -', '', mode = 'preserve')
	'A - A - A'
	>>> testResync('A - A - A - -', '', mode = 'append')
	'A - A - A'
	>>> testResync('A - A - A - -', '', mode = 'reorder')
	'A A A'
	>>> testResync('A - A - A - -', '', mode = 'fillgap')
	'A - A - A'

	>>> testResync('A - A - A', 'B:2', mode = 'preserve')
	'A - A - A BB'
	>>> testResync('A - A - A', 'B:2', mode = 'append')
	'A - A - A BB'
	>>> testResync('A - A - A', 'B:2', mode = 'reorder')
	'A BB A A'
	>>> testResync('A - A - A', 'B:2', mode = 'fillgap')
	'A BB A - A'

	>>> testResync('A - A - A', 'B:5', mode = 'preserve')
	'A - A - A BBB BB'
	>>> testResync('A - A - A', 'B:5', mode = 'append')
	'A - A - A BBB BB'
	>>> testResync('A - A - A', 'B:5', mode = 'reorder')
	'A BB A BBB A'
	>>> testResync('A - A - A', 'B:5', mode = 'fillgap')
	'A BBB A BB A'

	>>> testResync('A - A - A', 'A:5', mode = 'preserve')
	'A - A - A AA'
	>>> testResync('A - A - A', 'A:5', mode = 'append')
	'A - A - A AA'
	>>> testResync('A - A - A', 'A:5', mode = 'reorder')
	'A AA A A'
	>>> testResync('A - A - A', 'A:5', mode = 'fillgap')
	'A AA A - A'

	>>> testResync('A - A - A', 'A:8', mode = 'preserve')
	'A - A - A AAA AA'
	>>> testResync('A - A - A', 'A:8', mode = 'append')
	'A - A - A AAA AA'
	>>> testResync('A - A - A', 'A:8', mode = 'reorder')
	'A AA A AAA A'
	>>> testResync('A - A - A', 'A:8', mode = 'fillgap')
	'A AAA A AA A'

	>>> testResync('A - A - A', 'A:11', mode = 'preserve')
	'A - A - A AAA AAA AA'
	>>> testResync('A - A - A', 'A:11', mode = 'append')
	'A - A - A AAA AAA AA'
	>>> testResync('A - A - A', 'A:11', mode = 'reorder')
	'A AA A AAA A AAA'
	>>> testResync('A - A - A', 'A:11', mode = 'fillgap')
	'A AAA A AAA A AA'

	>>> testResync('A - AA - A', 'A:2', mode = 'preserve')
	'A - A'
	>>> testResync('A - AA - A', 'A:2', mode = 'append')
	'A - - - - A'
	>>> testResync('A - AA - A', 'A:2', mode = 'reorder')
	'A A'
	>>> testResync('A - AA - A', 'A:2', mode = 'fillgap')
	'A - A'
	"""

run_test()
