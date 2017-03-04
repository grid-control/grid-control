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

import sys
from dataset_list_from import add_dataset_list_options, discover_dataset
from gc_scripts import ScriptOptions


def _main():
	parser = ScriptOptions(usage='%s [OPTIONS] <data path> <dataset name> <pattern (*.root) / files>')
	parser.add_text(None, 'p', 'path', dest='dataset', default='.',
		help='Path to dataset files')
	parser.add_bool(None, 'r', 'recurse', dest='source recurse', default=False,
		help='Recurse into subdirectories if supported')
	add_dataset_list_options(parser)
	options = parser.script_parse(arg_keys=['dataset', 'dataset name pattern', 'filename filter'])

	def _conditional_set(target, cond, value):
		if options.config_dict.get(cond) and not options.config_dict.get(target):
			options.config_dict[target] = value

	_conditional_set('dataset name pattern', 'delimeter dataset key', '/PRIVATE/@DELIMETER_DS@')
	_conditional_set('block name pattern', 'delimeter block key', '@DELIMETER_B@')
	discover_dataset('ScanProvider', options.config_dict)


if __name__ == '__main__':
	sys.exit(_main())
