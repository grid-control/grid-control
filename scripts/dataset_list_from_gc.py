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
	parser = ScriptOptions(usage='%s [OPTIONS] <config file / work directory>')
	parser.add_text(None, 'J', 'job-selector', dest='external job selector', default='',
		help='Specify which jobs to process')
	parser.add_text(None, 'i', 'info-scanner',
		help='Specify which info scanner to run')
	parser.add_text(None, 'm', 'event-mode', dest='mode', default='CMSSW-Out',
		help='Specify how to determine events - available: [CMSSW-Out], CMSSW-In, DataMod')
	parser.add_text(None, 'l', 'lfn', dest='lfn marker', default='/store/',
		help='Assume everything starting with marker to be a logical file name')
	parser.add_bool(None, 'c', 'config', dest='include config infos', default=False,
		help='CMSSW specific: Add configuration data to metadata')
	parser.add_bool(None, 'p', 'parents', dest='include parent infos', default=False,
		help='CMSSW specific: Add parent infos to metadata')
	add_dataset_list_options(parser)
	options = parser.script_parse(arg_keys=['dataset'])

	# Positional parameters override options
	if not options.args:
		parser.exit_with_usage()
	tmp = {'cmssw-out': 'CMSSW_EVENTS_WRITE', 'cmssw-in': 'CMSSW_EVENTS_READ', 'datamod': 'MAX_EVENTS'}
	if options.opts.info_scanner:
		options.config_dict['scanner'] = options.opts.info_scanner.replace(',', ' ')
	options.config_dict['events key'] = tmp.get(options.config_dict['mode'].lower(), '')
	sys.exit(discover_dataset('GCProvider', options.config_dict))


if __name__ == '__main__':
	sys.exit(_main())
