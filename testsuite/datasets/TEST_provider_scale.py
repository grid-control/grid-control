#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testFwk').setup(__file__)
# - prolog marker
import time, random, logging
from testFwk import create_config, remove_files_testsuite
from grid_control.datasets import DataProvider, DataSplitter
from testDS import getRandomDatasets


logging.getLogger().setLevel(logging.ERROR)

class RandomProvider(DataProvider):
	alias_list = ['rng']
	def __init__(self, config, datasetExpr, datasetNick):
		DataProvider.__init__(self, config, datasetExpr, datasetNick)
		self.nBlocks = int(datasetExpr)

	def _iter_blocks_raw(self):
		return getRandomDatasets(self.nBlocks)

config = create_config(config_dict = {
	'foosection': {
		'events per job': 25000,
		'files per job': 10,
		'splitter stack': 'BlockBoundarySplitter FileBoundarySplitter HybridSplitter',
		'nickname check collision': False,
		'resync interactive': False
	}
})

def display(info, t_start, total):
	diff = time.time() - start
	print(info + ' %f %f' % (diff, diff / float(total) * 1e3))

start = time.time()
provider = RandomProvider(config, 1000000, None)
allData = provider.get_block_list_cached(show_stats = False)
display('t_get / 1kBlocks', start, len(allData))

data1 = random.sample(allData, int(len(allData) * 0.9))
data2 = random.sample(allData, int(len(allData) * 0.9))

start = time.time()
provider.save_to_file('datacache-1.dat', data1)
display('t_dsave1 / 1kBlocks', start, len(data1))

start = time.time()
provider.save_to_file('datacache-2.dat', data2)
display('t_dsave2 / 1kBlocks', start, len(data2))

start = time.time()
splitFile = DataSplitter.create_instance('FLSplitStacker', config, 'dataset')
splitFile.partition_blocks('datamap-large.tar', data1)
display('t_split / 1kJobs', start, splitFile.get_partition_len())

start = time.time()
for partition in splitFile.iter_partitions():
	pass
display('t_iter / 1kJobs', start, splitFile.get_partition_len())

start = time.time()
jobChanges = splitFile.resync_partitions('datamap-resync.tar', data1, data2)
display('t_sync / 1kJobs', start, splitFile.get_partition_len())

remove_files_testsuite(['datacache-1.dat', 'datacache-2.dat', 'datamap-large.tar', 'datamap-resync.tar'])
