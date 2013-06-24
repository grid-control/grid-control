#!/usr/bin/env python
from gcSupport import *
from grid_control_cms.webservice_api import readJSON

def lfn2pfn(node, lfn):
	return readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		{'node': node, 'protocol': 'srmv2', 'lfn': lfn})['phedex']['mapping'][0]['pfn']

parser = optparse.OptionParser()
parser.add_option("-s", "--SE", dest="SE", default=None, help="Resolve LFN on CMS SE into PFN")
parser.add_option("", "--lfn", dest="lfn", default="/store/user", help="Name of default LFN")
parser.add_option("", "--se-prot", dest="seprot", default="srmv2", help="Name of default SE protocol")
(opts, args) = parseOptions(parser)

if opts.SE:
	tmp = readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn',
		{'node': opts.SE, 'protocol': opts.seprot, 'lfn': opts.lfn})['phedex']['mapping']
	for entry in tmp:
		if len(tmp) > 1:
			print entry['node'],
		print entry['pfn']
