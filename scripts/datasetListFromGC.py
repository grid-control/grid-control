#!/usr/bin/env python
import gcSupport, sys, os, optparse
from grid_control import *
from grid_control.datasets import DataProvider

parser = optparse.OptionParser(usage="%prog [options] <config file> [<job id>, ...]")
parser.add_option("-m", "--mode",   dest="mode",   default="CMSSW-Out",
	help="Specify how to process output files - available: [CMSSW-Out], CMSSW-In")
parser.add_option("-e", "--events", dest="events", default="0",
	help="User defined event number - zero means skipping files without event infos")
parser.add_option("-E", "--force-events", dest="forceevents", default=False, action="store_true",
	help="Force usage of user supplied number of events - default: off")
parser.add_option("-s", "--strip",  dest="strip",  default=False,
    action="store_const", const="/store", help="Strip everything before /store in path")
(opts, args) = parser.parse_args()
opts.mode = opts.mode.lower()

if len(args) == 3:
	(jobid, wmsid, retcode) = args
	if retcode != '0':
		sys.exit(0)
	workDir = os.environ['GC_WORKDIR']
	jobList = [ jobid ]
else:
	(workDir, jobList) = gcSupport.getWorkJobs(args)

# Lock file in case several instances of this program are running
mutex = gcSupport.FileMutex(os.path.join(workDir, 'datasetCacheAdd.lock'))

try:
	taskInfo = utils.PersistentDict(os.path.join(workDir, 'task.dat'), ' = ')
	provider = DataProvider.loadState(Config(), workDir, 'production.dbs')

	# Try to read all existing Blocks from production.dbs
	try:
		blocks = provider.getBlocks()
	except:
		blocks = []

	log = None
	jobList = utils.sorted(jobList)
	for jobNum in jobList:
		del log
		log = utils.ActivityLog('Reading job logs - [%d / %d]' % (jobNum, jobList[-1]))
		outputDir = os.path.join(workDir, 'output', 'job_' + str(jobNum))

		# Read the file hash entries from job info file
		files = gcSupport.getFileInfo(workDir, jobNum, lambda retCode: retCode == 0, rejected = [])
		for (hash, name_local, name_dest, pathSE) in files:
			dataset = "/PRIVATE/%s" % (name_local.replace('.root', ''))
			blockname = "%s-%05d" % (taskInfo['task id'][2:], int(os.environ.get('GC_PARAM_ID', 0)))

			# Try to find block with given dataset + blockname
			cblock = None
			for block in blocks:
				if (block[DataProvider.Dataset] == dataset) and (block[DataProvider.BlockName] == blockname):
					cblock = block
			# No block found => Create new block
			if cblock == None:
				cblock = {
					DataProvider.Dataset: dataset,
					DataProvider.BlockName: blockname,
					DataProvider.NEvents: 0,
					DataProvider.SEList: None,
					DataProvider.FileList: []
				}
				blocks.append(cblock)

			lfn = os.path.join(pathSE, name_dest)
			nEvents = int(opts.events)

			# Read framework report files to get number of events
			if opts.mode.startswith("cmssw"):
				try:
					for fwkXML in gcSupport.getCMSSWInfo(os.path.join(outputDir, "cmssw.dbs.tar.gz")):
						if opts.mode == "cmssw-out":
							for outFile in fwkXML.getElementsByTagName("File"):
								pfn = outFile.getElementsByTagName("PFN")[0].childNodes[0].data
								if pfn == name_local:
									nEvents = int(outFile.getElementsByTagName("TotalEvents")[0].childNodes[0].data)
						elif opts.mode == "cmssw-in":
							nEvents = 0
							for inFile in fwkXML.getElementsByTagName("InputFile"):
								nEvents += int(inFile.getElementsByTagName("EventsRead")[0].childNodes[0].data)
				except KeyboardInterrupt:
					sys.exit(0)
				except:
					print "Error while parsing framework output of job %s!" % jobNum
					continue

			if opts.forceevents:
				nEvents = int(opts.events)
			if nEvents == 0:
				continue

			# Add file to filelist of the current block
			filelist = cblock[DataProvider.FileList]
			if not lfn in map(lambda x: x[DataProvider.lfn], filelist):
				if opts.strip:
					lfn = str.join(opts.strip, [''] + lfn.split(opts.strip)[1:])
				filelist.append({ DataProvider.lfn: lfn, DataProvider.NEvents: nEvents })
			cblock[DataProvider.NEvents] = reduce(lambda x, y: x+y, map(lambda x: x[DataProvider.NEvents], filelist))

	provider.saveState(workDir, "production.dbs", blocks)
except:
	del mutex
	raise
del mutex
