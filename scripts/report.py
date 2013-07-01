#!/usr/bin/env python
import sys, optparse
from gcSupport import utils, Config, Module, JobManager, JobSelector, Report, GCError, parseOptions, handleException

parser = optparse.OptionParser()
parser.add_option('-J', '--job-selector', dest='selector', default=None)
parser.add_option('-m', '--map', dest='showMap', default=False, action='store_true',
	help='Draw map of sites')
parser.add_option('-C', '--cpu', dest='showCPU', default=False, action='store_true',
	help='Display time overview')
Report.addOptions(parser)
(opts, args) = parseOptions(parser)

if len(args) != 1:
	utils.exitWithUsage('%s [options] <config file>' % sys.argv[0])

def main():
	# try to open config file
	config = Config(args[0])
	config.opts = config
	config.opts.init = False
	config.opts.resync = False

	# Initialise application module
	module = Module.open(config.get('global', 'module'), config)

	# Initialise job database
	jobs = JobManager(config, module, None).jobDB
	log = utils.ActivityLog('Filtering job entries')
	selected = jobs.getJobs(JobSelector.create(opts.selector, module=module))
	del log

	# Show reports
	report = Report(jobs, selected)
	if opts.showCPU:
		cpuTime = 0
		for jobNum in selected:
			jobObj = jobs.get(jobNum)
			cpuTime += jobObj.get('runtime', 0)
		print 'Used wall time:', utils.strTime(cpuTime)
		print 'Estimated cost: $%.2f' % ((cpuTime / 60 / 60) * 0.1)
	elif opts.showMap:
		from grid_control_gui import geomap
		geomap.drawMap(report)
	else:
		report.show(opts, module)

handleException(main)
