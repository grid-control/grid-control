#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import sys, copy, random
from testfwk import create_config, testfwk_remove_files
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.utils.activity import Activity
from grid_control_gui.stream_gui import MultiActivityMonitor
from testDS import checkCoverage
from testResync import do_first_split, do_resync
from python_compat import irange, itemgetter, lmap, md5_hex


MultiActivityMonitor(sys.stdout, register_callback=True)


def modifyBlock(block, seed=0):
	block = copy.deepcopy(block)
	random.seed(seed)
	rng = lambda x: random.random() < x
	fl = block[DataProvider.FileList]
	if rng(0.05): # remove random files
		fl = list(random.sample(fl, max(1, len(fl) - random.randint(0, 4))))
	avgEvents = 0
	for fi in fl:
		if rng(0.1): # change number of entries
			if rng(0.5):
				fi[DataProvider.NEntries] += random.randint(0, 5)
			else:
				fi[DataProvider.NEntries] -= random.randint(0, 5)
			fi[DataProvider.NEntries] = max(1, fi[DataProvider.NEntries])
		avgEvents += fi[DataProvider.NEntries]
	avgEvents = int(avgEvents / len(fl))
	if rng(0.05): # add new files
		for x in irange(random.randint(0, 4)):
			lfn = fl[0][DataProvider.URL].split('FILE')[0]
			lfn += 'FILE_%s' % md5_hex(str(random.random()))
			nev = random.randint(max(1, avgEvents - 5), max(1, avgEvents + 5))
			fl.append({DataProvider.URL: lfn, DataProvider.NEntries: nev})
	if rng(0.1): # reorder files
		random.shuffle(fl)
	block[DataProvider.FileList] = fl
	return block


def check_resync_fuzz(n_test=100, n_mod=1, n_resync=1, n_events=15):
	hash_map = {}
	try:
		activity = Activity('Sync test')
		for idx_test in irange(n_test):
			provider = DataProvider.create_instance('TestsuiteProvider', create_config(), 'dataset',
				'datasets_avail=1 blocks=1 files=50 entries=%s locations_max=1 seed=%s' % (n_events, idx_test + 1))
			block_list = provider.get_block_list_cached(show_stats=False)
			DataProvider.save_to_file('data-ori.dbs', block_list)
			DataProvider.save_to_file('data-old.dbs', block_list)

			splitter = do_first_split(False, 5)
			checkCoverage(DataSplitter.load_partitions('datamap-resync.tar'), block_list)
			seed = 10000000 * idx_test
			for idx_mod in irange(n_mod):
				for idx_resync in irange(n_resync):
					seed += 1
					activity.update('Sync test %s/%s mod %s/%s resync %s/%s' % (idx_test, n_test,
						idx_mod, n_mod, idx_resync, n_resync))
					block_list_modified = lmap(lambda b: modifyBlock(b, seed), block_list)
					DataProvider.save_to_file('data-new.dbs', block_list_modified)
					do_resync(splitter, False)
					reader = DataSplitter.load_partitions('datamap-resync.tar')
					partition_hash = get_partition_hash(reader)
					partition_key = (idx_test, idx_mod, idx_resync)
					hash_map[partition_hash] = partition_key
					try:
						checkCoverage(reader, block_list_modified)
					except Exception:
						activity.finish()
						print('Resync seeds %s' % repr(partition_key))
						print(block_list)
						print(block_list_modified)
						raise
					block_list = block_list_modified
		activity.finish()
		print('Unique partitions: %s' % len(hash_map))
	except KeyboardInterrupt:
		testfwk_remove_files(['data-ori.dbs', 'data-new.dbs', 'data-old.dbs', 'datamap-resync.tar'])
		print('Cleanup complete\n')

def get_partition_hash(reader):
	hash_list = []
	for partition in reader.iter_partitions():
		comment = partition.pop(DataSplitter.Comment, None)
		hash_list.append(itemgetter(DataSplitter.FileList, DataSplitter.NEntries,
			DataSplitter.Skipped, DataSplitter.Locations, DataSplitter.Dataset, DataSplitter.BlockName)(partition))
		partition[DataSplitter.Comment] = comment
	return md5_hex(repr(hash_list))

check_resync_fuzz(20, 5, 5)
