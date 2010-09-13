#!/usr/bin/env python
import gcSupport, os, sys, optparse
from grid_control import *
from grid_control.datasets import DataSplitter

parser = optparse.OptionParser()
parser.add_option("-j", "--jdl", dest="jdl", default=False, action="store_true",
	help="Get JDL file")
parser.add_option("-s", "--state", dest="state", default="",
	help="Force new job state")
parser.add_option("-S", "--splitting", dest="splitting", default="",
	help="Show splitting of dataset")
parser.add_option("-C", "--checksplitting", dest="checkSplitting", default="",
	help="Check splitting of dataset")
(opts, args) = parser.parse_args()

# we need exactly one positional argument (config file)
if opts.jdl or opts.state:
	if len(args) != 1:
		utils.exitWithUsage("%s <job info file>" % sys.argv[0])
	job = Job.load(args[0])

if opts.jdl:
	print job.get("jdl")

if opts.state:
	try:
		newState = getattr(Job, opts.state)
	except:
		print "Invalid state: %s", opts.state
	oldState = job.state
	utils.vprint("Job state changed from %s to %s" % (Job.states[oldState], Job.states[newState]), -1, True, False)
	job.state = newState
	job.save(args[0])

if opts.splitting:
	splitter = DataSplitter.loadState(opts.splitting)
	utils.verbosity(10)
	if not opts.checkSplitting:
		splitter.printAllJobInfo()
	else:
		print "Checking %d jobs..." % splitter.getMaxJobs()
		fail = utils.set()
		for jobNum in range(splitter.getMaxJobs()):
			splitInfo = splitter.getSplitInfo(jobNum)
			try:
				(events, skip, files) = (0, 0, [])
				for line in open(os.path.join(opts.checkSplitting, "jobs", "job_%d.var" % jobNum)).readlines():
					if 'MAX_EVENTS' in line:
						events = int(line.split('MAX_EVENTS', 1)[1].replace("=", ""))
					if 'SKIP_EVENTS' in line:
						skip = int(line.split('SKIP_EVENTS', 1)[1].replace("=", ""))
					if 'FILE_NAMES' in line:
						files = line.split('FILE_NAMES', 1)[1].replace("=", "").replace("\"", "").replace("\\", "")
						files = map(lambda x: x.strip().strip(","), files.split())
				def printError(curJ, curS, msg):
					if curJ != curS:
						print "%s in job %d (j:%s != s:%s)" % (msg, jobNum, curJ, curS)
						fail.add(jobNum)
				printError(events, splitInfo[DataSplitter.NEvents], "Inconsistent number of events")
				printError(skip, splitInfo[DataSplitter.Skipped], "Inconsistent number of skipped events")
				printError(files, splitInfo[DataSplitter.FileList], "Inconsistent list of files")
			except:
				print "Job %d was never initialized!" % jobNum
		print str.join("\n", map(str, fail))
