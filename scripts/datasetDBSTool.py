#!/usr/bin/env python
import gcSupport, sys, os, fcntl, gzip, xml.dom.minidom, optparse
from grid_control import *

import DBSAPI_v2.dbsApi
from DBSAPI_v2.dbsApiException import *
from DBSAPI_v2.dbsOptions import DbsOptionParser

def main(argss):
	args = {'version': "DBS_2_0_6", 'level': "CRITICAL"}
	#args['url'] = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
	args['url'] = "http://ekpcms2.physik.uni-karlsruhe.de:8080/DBS/servlet/DBSServlet"
	api = DBSAPI_v2.dbsApi.DbsApi(args)

	parser = optparse.OptionParser()
	parser.add_option("-l", "--list", dest="list", default=False, action="store_true")
	parser.add_option("-r", "--remove", dest="remove")
	parser.add_option("-w", "--wipe", dest="wipe", default=False, action="store_true")
	parser.add_option("-d", "--dump", dest="dump")
	parser.add_option("-i", "--import", dest="imp")
	(opts, args) = parser.parse_args()

	if opts.list:
		for block in api.listBlocks():
			print block["Name"]
		sys.exit(0)

	def eraseBlock(block):
		api.deleteBlock(block.split("#")[0], block)
		if opts.wipe:
			api.deleteRecycleBin(block.split("#")[0], block)

	if opts.remove:
		if "#" in opts.remove:
			eraseBlock(opts.remove)
			pass
		else:
			for block in api.listBlocks(opts.remove.split("#")[0]):
				eraseBlock(block["Name"])
			api.deleteProcDS(block["Name"].split("#")[0])
		sys.exit(0)

	if opts.dump:
		print api.listDatasetContents(opts.dump.split("#")[0], opts.dump)
		sys.exit(0)

	if opts.imp:
		f = open(opts.imp);
		api.insertDatasetContents(f.read())
		f.close()
		sys.exit(0)

	print "Abandon all data, ye who tinker here!"

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
