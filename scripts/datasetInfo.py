#!/usr/bin/env python
# | Copyright 2009-2016 Karlsruhe Institute of Technology
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
from gcSupport import Options, getConfig, scriptOptions, utils
from grid_control.datasets import DataProvider, DatasetError
from grid_control.utils import thread_tools
from hpfwk import clear_current_exception
from python_compat import imap, itemgetter, izip, lmap, lzip, set, sort_inplace, sorted

usage = '%s [OPTIONS] <DBS dataset path> | <dataset cache file>' % sys.argv[0]
parser = Options(usage)
parser.addBool(None, 'l', 'list-datasets',  default = False, help = 'Show list of all datasets in query / file')
parser.addBool(None, 'b', 'list-blocks',    default = False, help = 'Show list of blocks of the dataset(s)')
parser.addBool(None, 'f', 'list-files',     default = False, help = 'Show list of all files grouped according to blocks')
parser.addBool(None, 's', 'list-storage',   default = False, help = 'Show list of locations where data is stored')
parser.addBool(None, 'm', 'metadata',       default = False, help = 'Get metadata infomation of dataset files')
parser.addBool(None, 'M', 'block-metadata', default = False, help = 'Get common metadata infomation of dataset blocks')
parser.addBool(None, 'O', 'ordered',        default = False, help = 'Sort dataset blocks and files')
parser.addText(None, 'p', 'provider',       default = '',    help = 'Default dataset provider')
parser.addText(None, 'C', 'settings',       default = '',    help = 'Specify config file as source of detailed dataset settings')
parser.addText(None, 'S', 'save',           default = '',    help = 'Saves dataset information to specified file')
parser.addBool(None, 'i', 'info',           default = False, help = 'Gives machine readable info of given dataset(s)')
parser.addBool(None, 'c', 'config-entry',   default = False, help = 'Gives config file entries to run over given dataset(s)')
parser.addBool(None, 'n', 'config-nick',    default = False, help = 'Use dataset path to derive nickname in case it it undefined')
parser.addText(None, 'L', 'location',  default = 'hostname', help = 'Format of location information')
options = scriptOptions(parser)

# we need exactly one positional argument (dataset path)
if len(options.args) != 1:
	utils.exitWithUsage(usage)

# Disable threaded queries
def noThread(desc, fun, *args, **kargs):
	fun(*args, **kargs)
	return type('DummyThread', (), {'join': lambda self: None})()
thread_tools.start_thread = noThread

def get_dataset_config(opts, args):
	dataset = args[0].strip()
	if os.path.exists(dataset):
		opts.provider = 'ListProvider'
	else:
		opts.provider = 'DBS3Provider'
	cfgSettings = {'dbs blacklist T1 *': 'False', 'remove empty blocks *': 'False',
		'remove empty files *': 'False', 'location format *': opts.location,
		'nickname check collision *': 'False',
		'dataset *': dataset, 'dataset provider *': opts.provider}
	if opts.metadata or opts.block_metadata:
		cfgSettings['lumi filter *'] = '-'
		cfgSettings['keep lumi metadata *'] = 'True'
	return getConfig(configFile = opts.settings, configDict = {'dataset': cfgSettings})

def list_datasets(blocks):
	# Add some enums for consistent access to info dicts
	DataProvider.NFiles = -1
	DataProvider.NBlocks = -2

	infos = {}
	order = []
	infosum = {DataProvider.Dataset : 'Sum'}
	for block in blocks:
		dsName = block.get(DataProvider.Dataset, '')
		if not infos.get(dsName, None):
			order.append(dsName)
			infos[dsName] = {DataProvider.Dataset: block[DataProvider.Dataset]}
		def updateInfos(target):
			target[DataProvider.NBlocks]  = target.get(DataProvider.NBlocks, 0) + 1
			target[DataProvider.NFiles]   = target.get(DataProvider.NFiles, 0) + len(block[DataProvider.FileList])
			target[DataProvider.NEntries] = target.get(DataProvider.NEntries, 0) + block[DataProvider.NEntries]
		updateInfos(infos[dsName])
		updateInfos(infosum)
	head = [(DataProvider.Dataset, 'Dataset'), (DataProvider.NEntries, '#Events'),
		(DataProvider.NBlocks, '#Blocks'), (DataProvider.NFiles, '#Files')]
	utils.printTabular(head, lmap(lambda x: infos[x], order) + ['=', infosum])

def list_blocks(blocks, headerbase):
	utils.printTabular(headerbase + [(DataProvider.BlockName, 'Block'), (DataProvider.NEntries, 'Events')], blocks)

def list_files(datasets, blocks):
	print('')
	for block in blocks:
		if len(datasets) > 1:
			print('Dataset: %s' % block[DataProvider.Dataset])
		print('Blockname: %s' % block[DataProvider.BlockName])
		utils.printTabular([(DataProvider.URL, 'Filename'), (DataProvider.NEntries, 'Events')], block[DataProvider.FileList])
		print('')

def print_metadata(src, maxlen):
	for (mk, mv) in src:
		if len(str(mv)) > 200:
			mv = '<metadata entry size: %s> %s...' % (len(str(mv)), repr(mv)[:200])
		print('\t%s: %s' % (mk.rjust(maxlen), mv))
	if src:
		print('')

def list_metadata(datasets, blocks):
	print('')
	for block in blocks:
		if len(datasets) > 1:
			print('Dataset: %s' % block[DataProvider.Dataset])
		print('Blockname: %s' % block[DataProvider.BlockName])
		mk_len = max(imap(len, block.get(DataProvider.Metadata, [''])))
		for f in block[DataProvider.FileList]:
			print('%s [%d events]' % (f[DataProvider.URL], f[DataProvider.NEntries]))
			print_metadata(lzip(block.get(DataProvider.Metadata, []), f.get(DataProvider.Metadata, [])), mk_len)
		print('')

def list_block_metadata(datasets, blocks):
	for block in blocks:
		if len(datasets) > 1:
			print('Dataset: %s' % block[DataProvider.Dataset])
		print('Blockname: %s' % block[DataProvider.BlockName])
		if DataProvider.Metadata not in block:
			print('<no metadata>\n')
			continue
		mkdict = lambda x: dict(izip(block[DataProvider.Metadata], x[DataProvider.Metadata]))
		metadata = utils.QM(block[DataProvider.FileList], mkdict(block[DataProvider.FileList][0]), {})
		for fileInfo in block[DataProvider.FileList]:
			utils.intersectDict(metadata, mkdict(fileInfo))
		print_metadata(metadata.items(), max([0] + lmap(len, metadata.keys())))

def list_storage(blocks, headerbase):
	print('')
	print('Storage elements:')
	for block in blocks:
		dsName = block[DataProvider.Dataset]
		if len(headerbase) > 0:
			print('Dataset: %s' % dsName)
		if block.get(DataProvider.BlockName, None):
			print('Blockname: %s' % block[DataProvider.BlockName])
		if block[DataProvider.Locations] is None:
			print('\tNo location contraint specified')
		elif block[DataProvider.Locations] == []:
			print('\tNot located at anywhere')
		else:
			for se in block[DataProvider.Locations]:
				print('\t%s' % se)
		print('')

def list_config_entries(opts, blocks, provider):
	print('')
	print('dataset =')
	infos = {}
	order = []
	maxnick = 5
	for block in blocks:
		dsName = block[DataProvider.Dataset]
		if not infos.get(dsName, None):
			order.append(dsName)
			infos[dsName] = dict([(DataProvider.Dataset, dsName)])
			if DataProvider.Nickname not in block and opts.confignick:
				try:
					if '/' in dsName:
						block[DataProvider.Nickname] = dsName.lstrip('/').split('/')[1]
					else:
						block[DataProvider.Nickname] = dsName
				except Exception:
					clear_current_exception()
			if DataProvider.Nickname in block:
				nick = block[DataProvider.Nickname]
				infos[dsName][DataProvider.Nickname] = nick
				maxnick = max(maxnick, len(nick))
			if len(block[DataProvider.FileList]):
				infos[dsName][DataProvider.URL] = block[DataProvider.FileList][0][DataProvider.URL]
	for dsID, dsName in enumerate(order):
		info = infos[dsName]
		providerName = sorted(provider.get_class_names(), key = len)[0]
		nickname = info.get(DataProvider.Nickname, 'nick%d' % dsID).rjust(maxnick)
		filterExpr = utils.QM(providerName == 'list', ' %% %s' % info[DataProvider.Dataset], '')
		print('\t%s : %s : %s%s' % (nickname, providerName, provider.get_dataset_expr(), filterExpr))

def list_infos(blocks):
	evSum = 0
	for block in blocks:
		blockId = '%s %s' % (block.get(DataProvider.Dataset, '-'), block.get(DataProvider.BlockName, '-'))
		blockStorage = '-'
		if block.get(DataProvider.Locations, None):
			blockStorage = str.join(',', block.get(DataProvider.Locations, '-'))
		evSum += block.get(DataProvider.NEntries, 0)
		print('%s %s %d %d' % (blockId, blockStorage, block.get(DataProvider.NEntries, 0), evSum))

def save_dataset(opts, provider):
	print('')
	blocks = provider.getBlocks(show_stats = False)
	if opts.ordered:
		sort_inplace(blocks, key = itemgetter(DataProvider.Dataset, DataProvider.BlockName))
		for b in blocks:
			sort_inplace(b[DataProvider.FileList], key = itemgetter(DataProvider.URL))
	DataProvider.saveToFile(opts.save, blocks)
	print('Dataset information saved to ./%s' % opts.save)

def get_dataset_info(opts, args):
	config = get_dataset_config(opts, args)

	provider = config.getPlugin('dataset', cls = DataProvider)
	blocks = provider.getBlocks(show_stats = False)
	if len(blocks) == 0:
		raise DatasetError('No blocks!')

	datasets = set(imap(itemgetter(DataProvider.Dataset), blocks))
	if len(datasets) > 1 or opts.info:
		headerbase = [(DataProvider.Dataset, 'Dataset')]
	else:
		print('Dataset: %s' % blocks[0][DataProvider.Dataset])
		headerbase = []
	return (provider, datasets, blocks, headerbase)

def main(opts, args):
	(provider, datasets, blocks, headerbase) = get_dataset_info(opts, args)

	if opts.list_datasets:
		list_datasets(blocks)
	if opts.list_blocks:
		list_blocks(blocks, headerbase)
	if opts.list_files:
		list_files(datasets, blocks)
	if opts.list_storage:
		list_storage(blocks, headerbase)
	if opts.metadata and not opts.save:
		list_metadata(datasets, blocks)
	if opts.block_metadata and not opts.save:
		list_block_metadata(datasets, blocks)
	if opts.config_entry:
		list_config_entries(opts, blocks, provider)
	if opts.info:
		list_infos(blocks)
	if opts.save:
		save_dataset(opts, provider)

if __name__ == '__main__':
	sys.exit(main(options.opts, options.args))
