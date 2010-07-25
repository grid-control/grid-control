#!/usr/bin/env python
import gcSupport, sys, optparse
from grid_control import *
from grid_control.datasets import DataSplitter

parser = optparse.OptionParser()
parser.add_option("-j", "--jdl", dest="jdl", default=False, action="store_true",
	help="Get JDL file")
parser.add_option("-s", "--state", dest="state", default="",
	help="Force new job state")
parser.add_option("-S", "--splitting", dest="splitting", default="",
	help="Show splitting of dataset")
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
	splitter.printAllJobInfo()
