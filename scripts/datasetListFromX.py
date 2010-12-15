#!/usr/bin/env python
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
	try:
		config = gcSupport.config.Config(configDict = {None: dict(parser.values.__dict__)})
		provider = gcSupport.datasets.DataProvider.open(providerName, config, None, datasetExpr, None)
		if opts.output:
			provider.saveState(os.path.dirname(opts.output), os.path.basename(opts.output), None, opts.strip)
		else:
			gcSupport.datasets.DataProvider.saveStateRaw(sys.stdout, provider.getBlocks(), opts.strip)
	except gcSupport.GCError:
		gcSupport.utils.eprint(gcSupport.GCError.message)
