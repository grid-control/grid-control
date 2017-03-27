import os
from testfwk import create_config
from grid_control.datasets import DataProvider, DataSplitter
from testDS import display_partitions, get_lfn_map


def do_first_split(doprint=True, split_param=3, srcFN='data-old.dbs', splitter_name='EventBoundarySplitter'):
	config = create_config(config_dict={'dataset': {'events per job': split_param, 'files per job': split_param}})
	splitter = DataSplitter.create_instance(splitter_name, config, 'dataset')
	provider = DataProvider.load_from_file(srcFN)
	block_list = provider.get_block_list_cached(show_stats=False)
	DataSplitter.save_partitions('datamap-resync.tar', splitter.split_partitions(block_list))
	if doprint:
		reader = DataSplitter.load_partitions('datamap-resync.tar')
		lfn_map = get_lfn_map(blocks=block_list)
		display_partitions(lfn_map, reader.iter_partitions(), False)
	return splitter


def do_resync(splitter, doprint=True, config_dict={}, srcFN='data-old.dbs', modFN='data-new.dbs', doRename=True):
	config_dict.setdefault('dataset', {})['resync interactive'] = 'False'
	config_dict['dataset']['events per job'] = 100
	config_dict['dataset']['files per job'] = 100
	config = create_config(config_dict=config_dict)
	reader = DataSplitter.load_partitions('datamap-resync.tar')
	splitter = DataSplitter.create_instance(splitter.__class__.__name__, config, 'dataset')
	block_list_old = DataProvider.load_from_file(srcFN).get_block_list_cached(show_stats=False)
	block_list_new = DataProvider.load_from_file(modFN).get_block_list_cached(show_stats=False)
	resync_result = splitter.get_resync_handler().resync(splitter, reader, block_list_old, block_list_new)
	DataSplitter.save_partitions('datamap-resync-new.tar', resync_result.partition_iter)
	os.rename('datamap-resync-new.tar', 'datamap-resync.tar')
	if doRename:
		os.rename(modFN, srcFN)
	if doprint:
		reader_new = DataSplitter.load_partitions('datamap-resync.tar')
		print('(%s, %s, %s)' % (resync_result.pnum_list_redo, resync_result.pnum_list_disable, reader_new.get_partition_len() != reader.get_partition_len()))
		lfnMap = get_lfn_map(blocks=block_list_new)
		display_partitions(lfnMap, reader_new.iter_partitions(), False)
