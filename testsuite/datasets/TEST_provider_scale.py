#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import time, random, logging
from testfwk import create_config, testfwk_remove_files
from grid_control.datasets import DataProvider, DataSplitter


logging.getLogger().setLevel(logging.ERROR)

config = create_config(config_dict={
	'foosection': {
		'events per job': 25000,
		'files per job': 10,
		'splitter stack': 'BlockBoundarySplitter FileBoundarySplitter HybridSplitter',
		'nickname check collision': False,
		'resync interactive': False
	}
})


def display(info, t_start, total, details=''):
	diff = time.time() - start
	print(info + ' %f %f %s' % (diff, diff / float(total) * 1e3, details))


start = time.time()
provider = DataProvider.create_instance('TestsuiteProvider', config, 'dataset', 'blocks=1000')
allData = provider.get_block_list_cached(show_stats=False)
display('t_dsget / 1kBlocks', start, len(allData), '%s blocks' % len(allData))

data1 = random.sample(allData, int(len(allData) * 0.9))
data2 = random.sample(allData, int(len(allData) * 0.9))

start = time.time()
provider.save_to_file('datacache-1.dat', data1)
display('t_save1 / 1kBlocks', start, len(data1), '%s blocks' % len(data1))

start = time.time()
provider.save_to_file('datacache-2.dat', data2)
display('t_save2 / 1kBlocks', start, len(data2), '%s blocks' % len(data2))

start = time.time()
splitter = DataSplitter.create_instance('FLSplitStacker', config, 'dataset')
part_list = list(splitter.split_partitions(data1))
display('t_split / 1kJobs', start, len(part_list), '%s partitions' % len(part_list))

start = time.time()
DataSplitter.save_partitions('datamap-large.tar', part_list)
display('t_psave / 1kJobs', start, len(part_list))

start = time.time()
reader = DataSplitter.load_partitions('datamap-large.tar')
display('t_pread / 1kJobs', start, len(part_list))

start = time.time()
for partition in reader.iter_partitions():
	pass
display('t_piter / 1kJobs', start, reader.get_partition_len())

start = time.time()
resync_result = splitter.get_resync_handler().resync(splitter, reader, data1, data2)
part_list_new = list(resync_result.partition_iter)
display('t_psync / 1kJobs', start, len(part_list_new), '%s partitions' % len(part_list_new))

testfwk_remove_files(['datacache-1.dat', 'datacache-2.dat', 'datamap-large.tar'])
