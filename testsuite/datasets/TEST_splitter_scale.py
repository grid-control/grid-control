#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os, time, random, logging
from testfwk import create_config, testfwk_remove_files
from grid_control.datasets import DataProvider, DataSplitter
from python_compat import irange


logging.getLogger().setLevel(logging.ERROR)

config = create_config(config_dict={
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
provider = DataProvider.create_instance('TestsuiteProvider', config, 'dataset', 'blocks=100')
alldata = list(provider.get_block_list_cached(show_stats=False))
data1 = random.sample(alldata, int(len(alldata) * 0.8))
data2 = random.sample(alldata, int(len(alldata) * 0.8))
display ('t_get / 1kBlocks', start, len(alldata))

provider.save_to_file('data_large_all.dbs', alldata)
provider.save_to_file('data_large_1.dbs', data1)
provider.save_to_file('data_large_2.dbs', data2)

def check_splitter(splitter_name):
	start = time.time()
	splitter = DataSplitter.create_instance(splitter_name, config, 'dataset')
	partition_list = list(splitter.split_partitions(data1))
	DataSplitter.save_partitions('datamap-resync.tar', partition_list)
	display ('t_split / 1kJobs', start, len(partition_list))

	for x in irange(5):
		reader = DataSplitter.load_partitions('datamap-resync.tar')
		start = time.time()
		resync_result = splitter.get_resync_handler().resync(splitter, reader, data1, data2)
		part_list = list(resync_result.partition_iter)
		DataSplitter.save_partitions('datamap-resync1.tar', part_list)
		os.rename('datamap-resync1.tar', 'datamap-resync1.tar')
		display ('t_sync / 1kJobs', start, len(part_list))

check_splitter('EventBoundarySplitter')
#check_splitter('FLSplitStacker')
testfwk_remove_files(['data_large_all.dbs', 'data_large_1.dbs', 'data_large_2.dbs', 'datamap-resync.tar', 'datamap-resync1.tar'])
