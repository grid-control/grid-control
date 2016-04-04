#!/usr/bin/env python
# | Copyright 2011-2016 Karlsruhe Institute of Technology
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
from gcSupport import Options, Plugin, getConfig, scriptOptions, utils
from python_compat import lmap

parser = Options('%s [options] <DBS dataset path>' % sys.argv[0])
parser.addText(None, 'producer', default = 'SimpleNickNameProducer',
	help = 'Name of the nickname producer')
options = scriptOptions(parser)

def main(opts, args):
	if len(args) == 0:
		utils.exitWithUsage('Dataset path not specified!')
	datasetPath = args[0]
	if '*' in datasetPath:
		dbs3 = Plugin.createInstance('DBS3Provider', getConfig(), datasetPath, None)
		toProcess = dbs3.getCMSDatasetsImpl(datasetPath)
	else:
		toProcess = [datasetPath]

	nProd = Plugin.getClass('NickNameProducer').createInstance(opts.producer, getConfig())
	utils.printTabular(
		[(0, 'Nickname'), (1, 'Dataset')],
		lmap(lambda ds: {0: nProd.getName('', ds, None), 1: ds}, toProcess), 'll')

sys.exit(main(options.opts, options.args))
