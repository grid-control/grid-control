#!/usr/bin/env python
import sys, optparse
from gcSupport import utils, Config, Module, JobManager, JobSelector, Report, GCError, parseOptions

parser = optparse.OptionParser()
parser.add_option('-J', '--job-selector', dest='selector', default=None)
parser.add_option('-m', '--map', dest='showMap', default=False, action='store_true',
	help='Draw map of sites')
Report.addOptions(parser)
(opts, args) = parseOptions(parser)

if len(args) != 1:
	utils.exitWithUsage('%s [options] <config file>' % sys.argv[0])
try:
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
	if opts.showMap:
		from grid_control_gui import geomap
		geomap.drawMap(report)
	else:
		report.show(opts, module)
except GCError:
	utils.eprint(GCError.message)
