#!/usr/bin/env python
import gcSupport, sys, optparse
from grid_control import *
from grid_control.CMSSW import provider_dbsv2, formatLumi, mergeLumi
from grid_control.CMSSW.provider_dbsv2 import *

parser = optparse.OptionParser()
parser.add_option("-l", "--list", dest="list", default=None)
parser.add_option("-L", "--listlumis", dest="listlumis", default=None)
parser.add_option("-r", "--remove", dest="remove")
parser.add_option("-w", "--wipe", dest="wipe", default=False, action="store_true")
parser.add_option("-d", "--dump", dest="dump")
parser.add_option("-u", "--url", dest="url", default="http://ekpcms2.physik.uni-karlsruhe.de:8080/DBS/servlet/DBSServlet")
parser.add_option("-i", "--import", dest="imp")
(opts, args) = parser.parse_args()

api = createDBSAPI(opts.url)

if opts.remove:
	def eraseBlock(block):
		api.deleteBlock(block.split("#")[0], block)
		if opts.wipe:
			api.deleteRecycleBin(block.split("#")[0], block)
	if "#" in opts.remove:
		eraseBlock(opts.remove)
	else:
		for block in api.listBlocks(opts.remove.split("#")[0]):
			eraseBlock(block["Name"])
		api.deleteProcDS(block["Name"].split("#")[0])

elif opts.list and opts.listlumis:
	for fileInfo in api.listFiles(opts.list):
		lfn = fileInfo['LogicalFileName']
		rl = []
		for lumi in api.listFileLumis(lfn):
			rl.append(([int(lumi["RunNumber"]), int(lumi["LumiSectionNumber"])], [int(lumi["RunNumber"]), int(lumi["LumiSectionNumber"])]))
		print lfn
		for line in map(lambda x: str.join(", ", x), gcSupport.utils.lenSplit(formatLumi(mergeLumi(rl)), 70)):
			print "\t", line
		

elif opts.list:
	for block in api.listBlocks(opts.list):
		print block["Name"]

elif opts.listlumis:
	for lumi in api.listFileLumis(opts.listlumis):
		print lumi["RunNumber"], lumi["LumiSectionNumber"]

elif opts.dump:
	print api.listDatasetContents(opts.dump.split("#")[0], opts.dump)

elif opts.imp:
	f = open(opts.imp);
	api.insertDatasetContents(f.read())
	f.close()

else:
	print "Abandon all data, ye who tinker here!"
