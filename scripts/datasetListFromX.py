#!/usr/bin/env python
#-#  Copyright 2010-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import sys
from gcSupport import Plugin, getConfig

def addDatasetListOptions(parser):
	parser.addText(None, 'dataset',           short = '-d', dest = 'dataset name pattern',  default = '',
		help = 'Name pattern of dataset')
	parser.addText(None, 'block',             short = '-b', dest = 'block name pattern',    default = '',
		help = 'Name pattern of block')
	parser.addText(None, 'output',            short = '-o', dest = 'output',                default = '',
		help = 'Output filename')
	parser.addText(None, 'events',            short = '-e', dest = 'events default',        default = '-1',
		help = 'Number of events in files')
	parser.addText(None, 'events-cmd',        short = '-E', dest = 'events command',        default = '',
		help = 'Application used to determine number of events in files')
	parser.addFlag(None, 'events-empty',      short = '-y', dest = 'events ignore empty',   default = True,
		help = 'Keep empty files with zero events')
	parser.addFlag(None, 'keep-metadata',     short = '-k', dest = 'strip',                 default = True,
		help = 'Keep metadata in output')
	parser.addText(None, 'selection',         short = '-s', dest = 'filename filter',       default = '*.root',
		help = 'File to include in dataset (Default: *.root)')
	parser.addText(None, 'delimeter-select',  short = '-S', dest = 'delimeter match',       default = '',
		help = '<delimeter>:<number of required delimeters>')
	parser.addText(None, 'delimeter-dataset', short = '-D', dest = 'delimeter dataset key', default = '',
		help = 'Multi dataset mode - files are sorted into different datasets according to <delimeter>:<start>:<end>')
	parser.addText(None, 'delimeter-block',   short = '-B', dest = 'delimeter block key',   default = '',
		help = 'Multi block mode - files are sorted into different blocks according to <delimeter>:<start>:<end>')
	parser.addText(None, 'hash-dataset',      short = '-H', dest = 'dataset hash keys',     default = '',
		help = 'Multi dataset mode - files are sorted into different datasets according to hash of variables')
	parser.addText(None, 'hash-block',        short = '',   dest = 'block hash keys',       default = '',
		help = 'Multi block mode - files are sorted into different blocks according to hash of variables')


def discoverDataset(providerName, config_dict):
	config = getConfig(configDict = {'dataset': config_dict})
	DataProvider = Plugin.getClass('DataProvider')
	provider = DataProvider.createInstance(providerName, config, config_dict['dataset'], None)
	if config_dict['output']:
		return DataProvider.saveToFile(opts.output, provider.getBlocks(), config_dict['strip'])
	return DataProvider.saveToStream(sys.stdout, provider.getBlocks(), config_dict['strip'])
