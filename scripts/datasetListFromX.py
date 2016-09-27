#!/usr/bin/env python
# | Copyright 2010-2016 Karlsruhe Institute of Technology
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

import sys
from gcSupport import Plugin, getConfig


def addDatasetListOptions(parser):
	parser.add_text(None, 'd', 'dataset',           dest = 'dataset name pattern',  default = '',
		help = 'Name pattern of dataset')
	parser.add_text(None, 'b', 'block',             dest = 'block name pattern',    default = '',
		help = 'Name pattern of block')
	parser.add_text(None, 'o', 'output',            dest = 'output',                default = '',
		help = 'Output filename')
	parser.add_text(None, 'e', 'events',            dest = 'events default',        default = '-1',
		help = 'Number of events in files')
	parser.add_text(None, 'E', 'events-cmd',        dest = 'events command',        default = '',
		help = 'Application used to determine number of events in files')
	parser.add_bool(None, 'y', 'events-empty',      dest = 'events ignore empty',   default = True,
		help = 'Keep empty files with zero events')
	parser.add_bool(None, 'k', 'keep-metadata',     dest = 'strip',                 default = True,
		help = 'Keep metadata in output')
	parser.add_bool(None, ' ', 'dump-config',       dest = 'dump config',           default = False,
		help = 'Dump config settings')
	parser.add_text(None, 's', 'selection',         dest = 'filename filter',       default = '*.root',
		help = 'File to include in dataset (Default: *.root)')
	parser.add_text(None, 'S', 'delimeter-select',  dest = 'delimeter match',       default = '',
		help = '<delimeter>:<number of required delimeters>')
	parser.add_text(None, 'D', 'delimeter-dataset', dest = 'delimeter dataset key', default = '',
		help = 'Multi dataset mode - files are sorted into different datasets according to <delimeter>:<start>:<end>')
	parser.add_text(None, 'B', 'delimeter-block',   dest = 'delimeter block key',   default = '',
		help = 'Multi block mode - files are sorted into different blocks according to <delimeter>:<start>:<end>')
	parser.add_text(None, 'H', 'hash-dataset',      dest = 'dataset hash keys',     default = '',
		help = 'Multi dataset mode - files are sorted into different datasets according to hash of variables')
	parser.add_text(None, ' ', 'hash-block',        dest = 'block hash keys',       default = '',
		help = 'Multi block mode - files are sorted into different blocks according to hash of variables')


def discoverDataset(providerName, config_dict):
	config = getConfig(config_dict = {'dataset': config_dict}).change_view(setSections = ['dataset'])
	DataProvider = Plugin.get_class('DataProvider')
	provider = DataProvider.create_instance(providerName, config, 'dataset', config_dict['dataset'], None)
	if config_dict['dump config'] == 'True':
		config.write(sys.stdout, print_default = True, print_minimal = True)
		return
	stripMetadata = config_dict['strip'] == 'True'
	if config_dict['output']:
		return DataProvider.saveToFile(config_dict['output'], provider.getBlocks(show_stats = False), stripMetadata)
	for _ in DataProvider.saveToStream(sys.stdout, provider.getBlocks(show_stats = False), stripMetadata):
		pass
