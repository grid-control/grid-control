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

import sys
from datasetListFromX import addDatasetListOptions, discoverDataset
from gcSupport import Options, scriptOptions, utils

parser = Options(usage = '%s [OPTIONS] <config file / work directory>')
parser.addText(None, 'J', 'job-selector', dest = 'external job selector', default = '',
	help = 'Specify which jobs to process')
parser.addText(None, 'm', 'event-mode',   dest = 'mode',                  default = 'CMSSW-Out',
	help = 'Specify how to determine events - available: [CMSSW-Out], CMSSW-In, DataMod')
parser.addText(None, 'l', 'lfn',          dest = 'lfn marker',            default = '/store/',
	help = 'Assume everything starting with marker to be a logical file name')
parser.addBool(None, 'c', 'config',       dest = 'include config infos',  default = False,
	help = 'CMSSW specific: Add configuration data to metadata')
parser.addBool(None, 'p', 'parents',      dest = 'include parent infos',  default = False,
	help = 'CMSSW specific: Add parent infos to metadata')
addDatasetListOptions(parser)
options = scriptOptions(parser, arg_keys = ['dataset'])

# Positional parameters override options
if len(options.args) == 0:
	utils.exitWithUsage(parser.usage())
tmp = {'cmssw-out': 'CMSSW_EVENTS_WRITE', 'cmssw-in': 'CMSSW_EVENTS_READ', 'datamod': 'MAX_EVENTS'}
options.config_dict['events key'] = tmp.get(options.config_dict['mode'].lower(), '')
sys.exit(discoverDataset('GCProvider', options.config_dict))
