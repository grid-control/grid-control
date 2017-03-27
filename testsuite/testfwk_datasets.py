from grid_control.config import create_config
from grid_control.datasets import DataProvider, DataSplitter, PartitionReader
from grid_control.parameters import ParameterSource


def createfun_get_list_provider(default_fn=None, default_config_dict=None):
	def get_list_provider(fn=default_fn, config_dict=default_config_dict):
		config_dict_sum = dict(default_config_dict or {})
		config_dict_sum.update(config_dict or {})
		config = create_config(config_dict={'dataset': config_dict_sum})
		return DataProvider.create_instance('ListProvider', config, 'dataset', fn)
	return get_list_provider


def get_data_psrc(data_src, splitter_name='FileBoundarySplitter',
		config_dict=None, pproc_name_list=None, repository=None):
	config_dict = config_dict or {}
	config_dict['partition processor'] = str.join(' ', pproc_name_list or ['BasicPartitionProcessor'])
	config = create_config(config_dict={'dataset': config_dict or {}})
	splitter = DataSplitter.create_instance(splitter_name, config, 'dataset')
	piter = splitter.split_partitions(data_src.get_block_list_cached(show_stats=False))
	preader = PartitionReader.create_instance('TrivialPartitionReader', piter)
	if repository is None:
		repository = {}
	return ParameterSource.create_instance('BaseDataParameterSource',
		config, 'dataset', repository, preader)


def get_dataset_block(fi_list, metadata_list):
	# Changes in block with single file
	return {
		DataProvider.Dataset: '/MY/DATASET',
		DataProvider.BlockName: 'block',
		DataProvider.Locations: ['SE1', 'SE2'],
		DataProvider.Nickname: 'TESTNICK',
		DataProvider.Metadata: metadata_list,
		DataProvider.FileList: fi_list,
	}
