#!/usr/bin/env python
import sys, optparse
from gcSupport import utils, Config, Module, JobDB, MultiJobSelector, Report, GCError

parser = optparse.OptionParser()
parser.add_option('-J', '--job-selector', dest='selector', default=None)
Report.addOptions(parser)
(opts, args) = parser.parse_args()

if len(args) != 1:
	utils.exitWithUsage('%s [options] <config file>' % sys.argv[0])
try:
	# try to open config file
	config = Config(args[0])
	config.opts = config
	config.opts.init = False
	config.opts.resync = False
	# Check work dir validity (default work directory is the config file name)
	config.workDir = config.getPath('global', 'workdir', config.workDirDefault)

	# Initialise application module
	module = Module.open(config.get('global', 'module'), config)

	# Initialise job database
	jobs = JobDB(config)
	selected = None
	if opts.selector:
		log = utils.ActivityLog('Filtering job entries')
		selected = jobs.getJobs(MultiJobSelector(opts.selector, module=module).select)
		del log

	# Show reports
	Report(jobs, selected).show(opts, module)
except GCError:
	utils.eprint(GCError.message)
