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

import sys, logging
from datasetListFromX import addDatasetListOptions, discoverDataset
from gcSupport import Options, scriptOptions

parser = Options(usage = '%s [OPTIONS] <data path> <dataset name> <pattern (*.root) / files>')
parser.addText(None, 'p', 'path', dest = 'dataset', default = '.', help = 'Path to dataset files')
addDatasetListOptions(parser)
options = scriptOptions(parser, arg_keys = ['dataset', 'dataset name pattern', 'filename filter'])
logging.getLogger('user').setLevel(logging.CRITICAL)

def conditionalSet(name, source, sourceKey):
	if options.config_dict.get(source) and not options.config_dict.get(name):
		options.config_dict[name] = options.config_dict[source]
conditionalSet('dataset name pattern', 'delimeter dataset key', '/PRIVATE/@DELIMETER_DS@')
conditionalSet('block name pattern', 'delimeter block key', '@DELIMETER_B@')
sys.exit(discoverDataset('ScanProvider', options.config_dict))
