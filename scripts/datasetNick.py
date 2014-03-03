#!/usr/bin/env python
from gcSupport import *
from grid_control.datasets import DataProvider, NickNameProducer
from grid_control_cms.provider_dbsv2 import createDBSAPI

usage = '%s [OPTIONS] <DBS dataset path>' % sys.argv[0]
parser = optparse.OptionParser(usage=usage)
parser.add_option('-n', '--nickproducer', dest='nprod', default='SimpleNickNameProducer',
	help='Name of the nickname producer')
(opts, args) = parseOptions(parser)

def main():
	if len(args) == 0:
		print 'Dataset path not specified!'
		sys.exit(0)
	datasetPath = args[0]
	if '*' in datasetPath:
		api = createDBSAPI('')
		pd, sd, dt = (datasetPath.lstrip("/") + "/*/*/*").split("/")[:3]
		toProcess = map(lambda x: x.get("PathList", [])[-1], api.listProcessedDatasets(pd, dt, sd))
	else:
		toProcess = [datasetPath]

	nProd = NickNameProducer.open(opts.nprod, Config().addSections(['dataset']))
	utils.printTabular(
		[(0, 'Nickname'), (1, 'Dataset')],
		map(lambda ds: {0: nProd.getName('', ds, None), 1: ds}, toProcess), 'll')

handleException(main)
