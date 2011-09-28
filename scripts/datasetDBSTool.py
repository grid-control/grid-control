#!/usr/bin/env python
import sys, optparse
from gcSupport import *
from grid_control_cms.lumi_tools import formatLumi, mergeLumi
from grid_control_cms.provider_dbsv2 import DataProvider, createDBSAPI

parser = optparse.OptionParser()
parser.add_option("-l", "--list", dest="list", default=None)
parser.add_option("-f", "--files", dest="files", default=None)
parser.add_option("-L", "--listlumis", dest="listlumis", default=None)
parser.add_option("-R", "--lumiranges", dest="lumiranges", default=None)
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

if opts.lumiranges:
	dummyConfig = Config(configDict={'dummy': {'lumi filter': '-', 'dbs blacklist T1': False,
		'remove empty blocks': False, 'remove empty files': False}})
	provider = DataProvider.create(dummyConfig, 'dummy', opts.lumiranges, 'DBSApiv2')
	blocks = provider.getBlocks()
	lrInfo = {}
	for block in blocks[:-1]:
		ds = block[DataProvider.Dataset]
		for fi in block[DataProvider.FileList]:
			runList = fi[DataProvider.Metadata][block[DataProvider.Metadata].index('Runs')]
			f = lambda fun, idx: fun(fun(runList), lrInfo.get(ds, (9999999, 0))[idx])
			lrInfo[ds] = (f(min, 0), f(max, 1))
	mkDict = lambda (ds, min_max): {0: ds, 1: min_max[0], 2: min_max[1]}
	print
	utils.printTabular([(0, 'Dataset'), (1, 'MinRun'), (2, 'MaxRun')], map(mkDict, lrInfo.items()))
	sys.exit(0)

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
		print gcSupport.utils.wrapList(formatLumi(mergeLumi(rl)), 70, ',\n\t')
		allrl.extend(rl)
	print "\nComplete dataset:"
	print gcSupport.utils.wrapList(formatLumi(mergeLumi(allrl)), 70, ',\n\t')

elif opts.list:
	for block in api.listBlocks(opts.list):
		print block["Name"]

elif opts.files:
	for f in api.listFiles(opts.files, retriveList=['retrive_block', 'retrive_run', 'retrive_lumi']):
		print f
		print

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
