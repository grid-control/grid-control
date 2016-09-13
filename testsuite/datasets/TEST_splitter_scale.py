#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import time, random, logging
from testFwk import create_config, remove_files_testsuite
from grid_control.datasets import DataProvider, DataSplitter
from testDS import getRandomDatasets
from python_compat import irange


logging.getLogger().setLevel(logging.ERROR)

class RandomProvider(DataProvider):
	alias_list = ['rng']
	def __init__(self, config, datasetExpr, datasetNick):
		DataProvider.__init__(self, config, datasetExpr, datasetNick)
		self.nBlocks = int(datasetExpr)

	def _iter_blocks_raw(self):
		return getRandomDatasets(10, self.nBlocks)

config = create_config(config_dict = {
	'None': {
		'events per job': 25000,
		'files per job': 10,
		'nickname check collision': False,
		'splitter stack': 'BlockBoundarySplitter FileBoundarySplitter HybridSplitter',
	},
	'dummy': {
		'parameters': "cross(data(), var('A'))",
		'A': '1 2 3'
	},
	'dataset': {
		'resync interactive': False
	}
})

def display(info, t_start, total):
	diff = time.time() - start
	print(info + ' %f %f' % (diff, diff / float(total) * 1e3))

start = time.time()
provider = RandomProvider(config, '1000', None)
alldata = list(provider.get_block_list_cached(show_stats = False))
data1 = random.sample(alldata, int(len(alldata) * 0.8))
data2 = random.sample(alldata, int(len(alldata) * 0.8))
display ('t_get / 1kBlocks', start, len(alldata))

provider.save_to_file('data_large_all.dbs', alldata)
provider.save_to_file('data_large_1.dbs', data1)
provider.save_to_file('data_large_2.dbs', data2)

def check_splitter(sc):
	start = time.time()
	splitFile = sc(config, 'dataset')
	splitFile.partition_blocks('datamap-resync.tar', data1)
	display ('t_split / 1kJobs', start, splitFile.get_partition_len())

	for x in irange(1):
		splitter = DataSplitter.load_partitions_for_script('datamap-resync.tar')
		start = time.time()
		tmp = splitter.resync_partitions('datamap-resync1.tar', data1, data2)
		splitter = DataSplitter.load_partitions_for_script('datamap-resync1.tar')
		display ('t_sync / 1kJobs', start, splitFile.get_partition_len())

check_splitter(DataSplitter.get_class('EventBoundarySplitter'))
#check_splitter(DataSplitter.get_class('FLSplitStacker'))
remove_files_testsuite(['data_large_all.dbs', 'data_large_1.dbs', 'data_large_2.dbs', 'datamap-resync.tar', 'datamap-resync1.tar'])
