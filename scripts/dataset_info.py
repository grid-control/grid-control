#!/usr/bin/env python
# | Copyright 2017 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys
from gc_scripts import ScriptOptions, gc_create_config, utils
from grid_control.datasets import DataProvider, DatasetError
from grid_control.utils import thread_tools
from python_compat import imap, itemgetter, izip, lzip, set, sort_inplace, sorted


def display_metadata(dataset_list, block, metadata_key_list, metadata_list, base_header_list=None):
	header_list = (base_header_list or []) + lzip(sorted(metadata_key_list), sorted(metadata_key_list))
	for metadata in metadata_list:
		metadata['Block'] = block.get(DataProvider.BlockName)
	header_list = [('Block', 'Block')] + header_list
	if len(dataset_list) > 1:
		for metadata in metadata_list:
			metadata['Dataset'] = block[DataProvider.Dataset]
		header_list = [('Dataset', 'Dataset')] + header_list
	title = ''
	if len(dataset_list) == 1:
		title = 'Dataset: %s' % dataset_list[0]
	utils.display_table(header_list,
		metadata_list, title=title, pivot=True)


def get_dataset_config(opts, args):
	dataset = args[0].strip()
	if os.path.exists(dataset):
		opts.provider = 'ListProvider'
	else:
		opts.provider = 'DBS3Provider'
	config_dict = {'dbs blacklist T1 *': 'False', 'remove empty blocks *': 'False',
		'remove empty files *': 'False', 'location format *': opts.location,
		'nickname check collision *': 'False',
		'dataset *': dataset, 'dataset provider *': opts.provider}
	if opts.metadata or opts.block_metadata:
		config_dict['lumi filter *'] = '-'
		config_dict['keep lumi metadata *'] = 'True'
	return gc_create_config(config_file=opts.settings, config_dict={'dataset': config_dict})


def get_dataset_info(opts, args):
	config = get_dataset_config(opts, args)
	provider = config.get_plugin('dataset', cls=DataProvider)
	block_list = provider.get_block_list_cached(show_stats=False)
	if len(block_list) == 0:
		raise DatasetError('No blocks!')
	dataset_list = sorted(set(imap(itemgetter(DataProvider.Dataset), block_list)))
	if opts.ordered:
		sort_inplace(block_list, key=itemgetter(DataProvider.Dataset, DataProvider.BlockName))
		for block in block_list:
			sort_inplace(block[DataProvider.FileList], key=itemgetter(DataProvider.URL))
	return (provider, dataset_list, block_list)


def get_file_metadata(block, fi):
	return dict(izip(block[DataProvider.Metadata], fi[DataProvider.Metadata]))


def get_total(block_list, label_key):
	total_dict = {label_key: 'Sum'}
	for block in block_list:
		block.setdefault(DataProvider.NFiles, len(block.get(DataProvider.FileList, [])))
		total_dict[DataProvider.NBlocks] = total_dict.get(DataProvider.NBlocks, 0) + \
			block.get(DataProvider.NBlocks, 1)
		total_dict[DataProvider.NFiles] = total_dict.get(DataProvider.NFiles, 0) + \
			block.get(DataProvider.NFiles, 0)
		total_dict[DataProvider.NEntries] = total_dict.get(DataProvider.NEntries, 0) + \
			block.get(DataProvider.NEntries, 0)
		yield block
	yield '='
	yield total_dict


def list_blocks(dataset_list, block_list):
	title = 'Dataset: %s\n' % dataset_list[0]
	header_list = []
	if len(dataset_list) > 1:
		title = ''
		header_list = [(DataProvider.Dataset, 'Dataset')]
	utils.display_table(header_list + [(DataProvider.BlockName, 'Block'),
		(DataProvider.NFiles, '#Files'), (DataProvider.NEntries, '#Entries')],
		get_total(block_list, DataProvider.BlockName), title=title)


def list_config_entries(dataset_list, block_list, opts, provider):
	dataset_config_str_list = []
	provider_name = sorted(provider.get_class_name_list(), key=len)[0]
	max_nick_len = 0
	for ds_info in merge_blocks(block_list):
		max_nick_len = max(max_nick_len, len(ds_info.get(DataProvider.Nickname, '')))
	for ds_info in merge_blocks(block_list):
		nickname = ds_info.get(DataProvider.Nickname, '').rjust(max_nick_len)
		dataset_config_str = '\t%s : %s : %s' % (nickname, provider_name, provider.get_dataset_expr())
		if provider_name == 'list':
			dataset_config_str += ' %% %s' % ds_info[DataProvider.Dataset]
		dataset_config_str_list.append(dataset_config_str)
	sys.stdout.write('\ndataset =\n' + str.join('\n', dataset_config_str_list) + '\n\n')


def list_datasets(block_list):
	header_list = [(DataProvider.Dataset, 'Dataset'), (DataProvider.NBlocks, '#Blocks'),
		(DataProvider.NFiles, '#Files'), (DataProvider.NEntries, '#Entries')]
	utils.display_table(header_list, get_total(merge_blocks(block_list), DataProvider.Dataset))


def list_files(block_list):
	for block in block_list:
		title = 'Dataset:   %s\nBlockname: %s' % (
			block[DataProvider.Dataset], block[DataProvider.BlockName])
		utils.display_table([(DataProvider.URL, 'Filename'), (DataProvider.NEntries, '#Entries')],
			get_total(block[DataProvider.FileList], DataProvider.URL), title=title)


def list_metadata(dataset_list, block_list):
	for block in block_list:
		if DataProvider.Metadata not in block:
			metadata = {'': '<no common metadata>'}
			continue

		metadata_list = []
		metadata_keys = set()
		for fi in block[DataProvider.FileList]:
			metadata = get_file_metadata(block, fi)
			metadata_keys.update(metadata.keys())
			metadata['Filename'] = fi[DataProvider.URL]
			metadata_list.append(metadata)

		display_metadata(dataset_list, block, metadata_keys, metadata_list,
			base_header_list=[('Filename', 'Filename')])


def list_metadata_common(dataset_list, block_list):
	def _get_common_metadata(block):
		result = {}
		if block[DataProvider.FileList]:
			result = get_file_metadata(block, block[DataProvider.FileList][0])
		for fi in block[DataProvider.FileList]:
			utils.intersect_first_dict(result, get_file_metadata(block, fi))
		return result

	for block in block_list:
		metadata = {}
		if DataProvider.Metadata in block:
			metadata = _get_common_metadata(block)
		if not metadata:
			metadata = {'': '<no common metadata>'}
		display_metadata(dataset_list, block, list(metadata.keys()), [metadata])


def list_storage(dataset_list, block_list):
	def _iter_storage(block_list):
		for block in block_list:
			result = {
				DataProvider.Dataset: block[DataProvider.Dataset],
				DataProvider.BlockName: block.get(DataProvider.BlockName),
			}
			location_list = block.get(DataProvider.Locations)
			if location_list is None:
				result[DataProvider.Locations] = 'No location contraint specified'
				yield result
			elif not location_list:
				result[DataProvider.Locations] = 'Not located at anywhere'
				yield result
			else:
				for location in location_list:
					result[DataProvider.Locations] = location
					yield result
					result = {}
	header_list = []
	title = 'Dataset: %s\n' % dataset_list[0]
	if len(dataset_list) != 1:
		header_list = [(DataProvider.Dataset, 'Dataset')]
		title = ''
	utils.display_table(header_list + [(DataProvider.BlockName, 'Block'),
		(DataProvider.Locations, 'Location')], _iter_storage(block_list), title=title)


def merge_blocks(block_list):
	ds_name_list = []
	ds_info_dict = {}
	for block in block_list:
		ds_name = block.get(DataProvider.Dataset, '')
		if not ds_info_dict.get(ds_name):
			ds_name_list.append(ds_name)
			ds_info_dict[ds_name] = {
				DataProvider.Dataset: block[DataProvider.Dataset],
				DataProvider.NEntries: 0,
				DataProvider.NFiles: 0,
				DataProvider.NBlocks: 0,
			}
		ds_info_dict[ds_name][DataProvider.NBlocks] += 1
		ds_info_dict[ds_name][DataProvider.NFiles] += len(block.get(DataProvider.FileList, []))
		ds_info_dict[ds_name][DataProvider.NEntries] += block.get(DataProvider.NEntries, 0)
		if block.get(DataProvider.Nickname):
			ds_info_dict[ds_name][DataProvider.Nickname] = block[DataProvider.Nickname]
	for ds_name in ds_name_list:
		yield ds_info_dict[ds_name]


def save_dataset(fn, block_list):
	DataProvider.save_to_file(fn, block_list)
	sys.stdout.write('Dataset information saved to %r\n' % fn)


def _main():
	# Disable threaded queries
	def _no_thread(desc, fun, *args, **kargs):
		fun(*args, **kargs)
		return type('DummyThread', (), {'join': lambda self: None})()
	thread_tools.start_daemon = _no_thread

	# Add some enums for consistent access to info dicts
	DataProvider.NFiles = -1
	DataProvider.NBlocks = -2

	options = _parse_cmd_line()
	(provider, dataset_list, block_list) = get_dataset_info(options.opts, options.args)

	if options.opts.list_datasets:
		list_datasets(block_list)
	if options.opts.list_blocks:
		list_blocks(dataset_list, block_list)
	if options.opts.list_files:
		list_files(block_list)
	if options.opts.list_storage:
		list_storage(dataset_list, block_list)
	if options.opts.metadata and not options.opts.save:
		list_metadata(dataset_list, block_list)
	if options.opts.block_metadata and not options.opts.save:
		list_metadata_common(dataset_list, block_list)
	if options.opts.config_entry:
		list_config_entries(dataset_list, block_list, options.opts, provider)
	if options.opts.save:
		save_dataset(options.opts.save, block_list)


def _parse_cmd_line():
	usage = '%s [OPTIONS] <DBS dataset path> | <dataset cache file>' % sys.argv[0]
	parser = ScriptOptions(usage)
	parser.add_bool(None, 'l', 'list-datasets', default=False,
		help='Show list of all datasets in query / file')
	parser.add_bool(None, 'b', 'list-blocks', default=False,
		help='Show list of blocks of the dataset(s)')
	parser.add_bool(None, 'f', 'list-files', default=False,
		help='Show list of all files grouped according to blocks')
	parser.add_bool(None, 's', 'list-storage', default=False,
		help='Show list of locations where data is stored')
	parser.add_bool(None, 'm', 'metadata', default=False,
		help='Get metadata infomation of dataset files')
	parser.add_bool(None, 'M', 'block-metadata', default=False,
		help='Get common metadata infomation of dataset blocks')
	parser.add_bool(None, 'O', 'ordered', default=False,
		help='Sort dataset blocks and files')
	parser.add_text(None, 'p', 'provider', default='',
		help='Default dataset provider')
	parser.add_text(None, 'C', 'settings', default='',
		help='Specify config file as source of detailed dataset settings')
	parser.add_text(None, 'S', 'save', default='',
		help='Saves dataset information to specified file')
	parser.add_bool(None, 'c', 'config-entry', default=False,
		help='Gives config file entries to run over given dataset(s)')
	parser.add_bool(None, 'n', 'config-nick', default=False,
		help='Use dataset path to derive nickname in case it it undefined')
	parser.add_text(None, 'L', 'location', default='hostname',
		help='Format of location information')
	options = parser.script_parse()
	# we need exactly one positional argument (dataset path)
	if len(options.args) != 1:
		parser.exit_with_usage()
	return options


if __name__ == '__main__':
	sys.exit(_main())
