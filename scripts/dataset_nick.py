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
from gc_scripts import ConsoleTable, Plugin, ScriptOptions, display_plugin_list_for, gc_create_config  # pylint:disable=line-too-long
from python_compat import lmap


def _main():
	parser = ScriptOptions(usage='%s [OPTIONS] <DBS dataset path>')
	parser.add_text(None, '', 'producer', default='SimpleNickNameProducer',
		help='Name of the nickname producer')
	parser.add_bool(None, 'L', 'nick-list', default=False,
		help='List available nickname producer classes')
	options = parser.script_parse()

	if options.opts.nick_list:
		display_plugin_list_for('NickNameProducer', title='Available nickname producer classes')
	if not options.args:
		parser.exit_with_usage()

	dataset_path = options.args[0]
	if ('*' in dataset_path) or os.path.exists(dataset_path):
		dataset_provider = 'DBS3Provider'
		if os.path.exists(dataset_path):
			dataset_provider = 'ListProvider'
		provider = Plugin.create_instance(dataset_provider, gc_create_config(), 'dataset', dataset_path)
		dataset_path_list = provider.get_dataset_name_list()
	else:
		dataset_path_list = [dataset_path]

	nn_prod = Plugin.get_class('NickNameProducer').create_instance(options.opts.producer,
		gc_create_config(), 'dataset')
	ConsoleTable.create([(1, 'Dataset'), (0, 'Nickname')],
		lmap(lambda ds: {0: nn_prod.get_name('', ds, None), 1: ds}, dataset_path_list), 'll')


if __name__ == '__main__':
	sys.exit(_main())
