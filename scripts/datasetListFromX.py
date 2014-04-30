#!/usr/bin/env python
#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
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

import os, gcSupport, sys, optparse

def addOptions(parser):
	parser.add_option('-d', '--dataset', dest='dataset name pattern', default='', help='Name pattern of dataset')
	parser.add_option('-b', '--block',   dest='block name pattern',   default='', help='Name pattern of block')
	parser.add_option('-o', '--output',  dest='output',               default='', help='Output filename')
	parser.add_option('-e', '--events',  dest='events default',       default='-1',
		help='Number of events in files')
	parser.add_option('-E', '--events-cmd',        dest='events command',        default='',
		help='Application used to determine number of events in files')
	parser.add_option('-y', '--events-empty',      dest='events ignore empty',   default=True, action='store_false',
		help='Keep empty files with zero events')
	parser.add_option('-k', '--keep-metadata',     dest='strip',                 default=True, action='store_false',
		help='Keep metadata in output')
	parser.add_option('-s', '--selection',         dest='filename filter',       default='*.root',
		help='File to include in dataset (Default: *.root)')
	parser.add_option('-S', '--delimeter-select',  dest='delimeter match',       default='',
		help='<delimeter>:<number of required delimeters>')
	parser.add_option('-D', '--delimeter-dataset', dest='delimeter dataset key', default='',
		help='Multi dataset mode - files are sorted into different datasets according to <delimeter>:<start>:<end>')
	parser.add_option('-B', '--delimeter-block',   dest='delimeter block key',   default='',
		help='Multi block mode - files are sorted into different blocks according to <delimeter>:<start>:<end>')


def discoverDataset(opts, parser, providerName, datasetExpr):
	def main():
		configEntries = map(lambda (k,v): (k, str(v)), parser.values.__dict__.items())
		config = gcSupport.config.Config([
			gcSupport.config.DictConfigFiller({'dataset': dict(configEntries)})]).addSections(['dataset'])
		provider = gcSupport.datasets.DataProvider.open(providerName, config, datasetExpr, None)
		if opts.output:
			provider.saveState(opts.output, None, opts.strip)
		else:
			gcSupport.datasets.DataProvider.saveStateRaw(sys.stdout, provider.getBlocks(), opts.strip)
	gcSupport.handleException(main)
