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

import logging
from gc_scripts import Plugin, gc_create_config
from python_compat import StringBuffer


def add_dataset_list_options(parser):
	mm_help = 'Multi %s mode - files are sorted into different %s according to '
	parser.add_text(None, 'd', 'dataset', dest='dataset name pattern', default='',
		help='Name pattern of dataset')
	parser.add_text(None, 'b', 'block', dest='block name pattern', default='',
		help='Name pattern of block')
	parser.add_text(None, 'o', 'output', dest='output', default='',
		help='Output filename')
	parser.add_text(None, 'e', 'events', dest='events default', default='-1',
		help='Number of events in files')
	parser.add_text(None, 'E', 'events-cmd', dest='events command', default='',
		help='Application used to determine number of events in files')
	parser.add_bool(None, 'y', 'events-empty', dest='events ignore empty', default=True,
		help='Keep empty files with zero events')
	parser.add_bool(None, 'k', 'keep-metadata', dest='strip', default=True,
		help='Keep metadata in output')
	parser.add_bool(None, ' ', 'dump-config', dest='dump config', default=False,
		help='Dump config settings')
	parser.add_text(None, 's', 'selection', dest='filename filter', default='*.root',
		help='File to include in dataset (Default: *.root)')
	parser.add_text(None, 'S', 'delimeter-select', dest='delimeter match', default='',
		help='<delimeter>:<number of required delimeters>')
	parser.add_text(None, 'D', 'delimeter-dataset', dest='delimeter dataset key', default='',
		help=mm_help % ('dataset', 'dataset') + '<delimeter>:<start>:<end>')
	parser.add_text(None, 'B', 'delimeter-block', dest='delimeter block key', default='',
		help=mm_help % ('block', 'block') + '<delimeter>:<start>:<end>')
	parser.add_text(None, 'H', 'hash-dataset', dest='dataset hash keys', default='',
		help=mm_help % ('dataset', 'dataset') + 'hash of variables')
	parser.add_text(None, ' ', 'hash-block', dest='block hash keys', default='',
		help=mm_help % ('block', 'block') + 'hash of variables')


def discover_dataset(provider_name, config_dict):
	buffer = StringBuffer()
	config = gc_create_config(config_dict={'dataset': config_dict})
	config = config.change_view(set_sections=['dataset'])
	provider = Plugin.get_class('DataProvider').create_instance(provider_name,
		config, 'dataset', config_dict['dataset'], None)
	if config_dict['dump config'] == 'True':
		config.write(buffer, print_default=True, print_minimal=True)
		return logging.getLogger('script').info(buffer.getvalue().rstrip())
	strip_metadata = config_dict['strip'] == 'True'
	block_iter = provider.get_block_list_cached(show_stats=False)
	if config_dict['output']:
		return provider.save_to_file(config_dict['output'], block_iter, strip_metadata)
	for _ in provider.save_to_stream(buffer, block_iter, strip_metadata):
		pass
	logging.getLogger('script').info(buffer.getvalue().rstrip())
