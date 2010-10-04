#!/usr/bin/env python
import gcSupport, sys, optparse
from grid_control import *
from grid_control.CMSSW import provider_dbsv2, formatLumi, mergeLumi
from grid_control.CMSSW.provider_dbsv2 import *

parser = optparse.OptionParser()
parser.add_option("-l", "--list", dest="list", default=None)
parser.add_option("-f", "--files", dest="files", default=None)
parser.add_option("-L", "--listlumis", dest="listlumis", default=None)
parser.add_option("-r", "--remove", dest="remove")
parser.add_option("-w", "--wipe", dest="wipe", default=False, action="store_true")
parser.add_option("-d", "--dump", dest="dump")
parser.add_option("-u", "--url", dest="url",
#	default="https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet"
	default="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
)
parser.add_option("-i", "--import", dest="imp")
parser.add_option("-s", "--se", dest="se")
parser.add_option("-p", "--parents", dest="parents")
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
		api.deleteProcDS(opts.remove.split("#")[0])

elif opts.listlumis:
	allrl = []
	for fileInfo in api.listFiles(opts.listlumis, retriveList=['retrive_lumi']):
		lfn = fileInfo['LogicalFileName']
		rl = []
		for lumi in fileInfo['LumiList']:
			rl.append(([int(lumi["RunNumber"]), int(lumi["LumiSectionNumber"])], [int(lumi["RunNumber"]), int(lumi["LumiSectionNumber"])]))
		print lfn
		for line in map(lambda x: str.join(", ", x), gcSupport.utils.lenSplit(formatLumi(mergeLumi(rl)), 70)):
			print "\t", line
		allrl.extend(rl)
	print "\nComplete dataset:"
	for line in map(lambda x: str.join(", ", x), gcSupport.utils.lenSplit(formatLumi(mergeLumi(allrl)), 70)):
		print "\t", line

elif opts.list:
	for block in api.listBlocks(opts.list):
		print block["Name"]

elif opts.files:
	for f in api.listFiles(opts.files, retriveList=['retrive_block', 'retrive_run', 'retrive_lumi']):
		print f
		print

elif opts.listlumis:
	for lumi in api.listFileLumis(opts.listlumis):
		print lumi["RunNumber"], lumi["LumiSectionNumber"]

elif opts.dump:
	print api.listDatasetContents(opts.dump.split("#")[0], opts.dump)

elif opts.imp:
	f = open(opts.imp);
	api.insertDatasetContents(f.read())
	f.close()

elif opts.parents:
	parents = []
	for p1 in api.listDatasetParents(opts.parents):
		for path in p1["PathList"]:
			parents.append(path)
	print str.join(",", parents)

elif opts.se:
	selist = []
	for block in api.listBlocks(opts.se):
		selist.extend(map(lambda x: x["Name"], block["StorageElementList"]))
	print str.join(",", set(selist))

else:
	print "Abandon all data, ye who tinker here!"
