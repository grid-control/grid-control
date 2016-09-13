import os
from testFwk import create_config
from grid_control.datasets import DataProvider, DataSplitter
from testDS import getLFNMap, printSplitNice


def do_initSplit(doprint = True, nEvent = 3, srcFN = "data-old.dbs", splitter = 'EventBoundarySplitter'):
	config = create_config(config_dict={'dataset': {'events per job': nEvent, 'files per job': nEvent}})
	dsplit = DataSplitter.create_instance(splitter, config, 'dataset')
	data_old = DataProvider.load_from_file(srcFN)
	dsplit.partition_blocks("datamap-resync.tar", data_old.get_block_list_cached(show_stats = False))
	if doprint:
		printSplitNice(DataSplitter.load_partitions_for_script("datamap-resync.tar"), getLFNMap(blocks = data_old.get_block_list_cached(show_stats = False)), False)

def doResync(doprint = True, config_dict = {}, srcFN = "data-old.dbs", modFN = "data-new.dbs", doRename = True):
	config_dict.setdefault('dataset', {})['resync interactive'] = 'False'
	config = create_config(config_dict = config_dict)
	split_old = DataSplitter.load_partitions_for_script("datamap-resync.tar", config)
	data_old = DataProvider.load_from_file(srcFN)
	data_new = DataProvider.load_from_file(modFN)
	tmp = split_old.resync_partitions("datamap-resync-new.tar", data_old.get_block_list_cached(show_stats = False), data_new.get_block_list_cached(show_stats = False))
	os.rename("datamap-resync-new.tar", "datamap-resync.tar")
	if doRename:
		os.rename(modFN, srcFN)
	if doprint:
		split_new = DataSplitter.load_partitions_for_script("datamap-resync.tar")
		print("(%s, %s, %s)" % (tmp[0], tmp[1], split_new.get_partition_len() != split_old.get_partition_len()))
		lfnMap = getLFNMap(blocks = data_new.get_block_list_cached(show_stats = False))
		printSplitNice(split_new, lfnMap, False)
