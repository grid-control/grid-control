#!/usr/bin/env python
import sys, os, signal, optparse

# add python subdirectory from where go.py was started to search path
sys.path.insert(1, os.path.join(sys.path[0], 'python'))

# and include grid_control python module
from grid_control import *

usage = "Syntax: %s [OPTIONS] <config file>\n" % sys.argv[0]
def print_help(*args):
	utils.eprint("%s\n%s" % (usage, open(utils.pathGC('share', 'help.txt'), 'r').read()))
	sys.exit(0)

def main(args):
	global log, handler
	utils.abort(False)
	log = None

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global log, handler
		utils.abort(True)
		log = utils.ActivityLog('Quitting grid-control! (This can take a few seconds...)')
		signal.signal(signal.SIGINT, handler)
	handler = signal.signal(signal.SIGINT, interrupt)

	# display the 'grid-control' logo and version
	utils.vprint(open(utils.pathGC('share', 'logo.txt'), 'r').read(), -1)
	utils.vprint('Revision: %s' % utils.getVersion(), -1)
	pyver = sys.version_info[0] + sys.version_info[1] / 10.0
	if pyver < 2.3:
		utils.deprecated("This python version (%.1f) is not supported anymore!" % pyver)

	parser = optparse.OptionParser(add_help_option=False)
	parser.add_option("-h", "--help",          action="callback", callback=print_help)
	parser.add_option("",   "--help-vars",     dest="help_vars",  default=False, action="store_true")
	parser.add_option("",   "--help-conf",     dest="help_cfg",   default=False, action="store_true")
	parser.add_option("",   "--help-confmin",  dest="help_scfg",  default=False, action="store_true")
	parser.add_option("-s", "--no-submission", dest="submission", default=True,  action="store_false")
	parser.add_option("-q", "--resync",        dest="resync",     default=False, action="store_true")
	parser.add_option("-i", "--init",          dest="init",       default=False, action="store_true")
	parser.add_option("-c", "--continuous",    dest="continuous", default=False, action="store_true")
	parser.add_option("-G", "--gui",           dest="gui",        default=False, action="store_true")
	parser.add_option("-r", '--report',        dest="report",     default=False, action="store_true")
	parser.add_option("-R", '--site-report',   dest="reportSite", default=0,     action="count")
	parser.add_option("-T", '--time-report',   dest="reportTime", default=0,     action="count")
	parser.add_option("-M", '--module-report', dest="reportMod",  default=0,     action="count")
	parser.add_option("-J", "--job-report",    dest="reportJob",  default=None)
	parser.add_option("-d", '--delete',        dest="delete",     default=None)
	parser.add_option("-S", '--seed',          dest="seed",       default=None)
	parser.add_option("-m", '--max-retry',     dest="maxRetry",   default=None,  type="int")
	parser.add_option("-v", "--verbose",       dest="verbosity",  default=0,     action="count")
	(opts, args) = parser.parse_args()
	utils.verbosity.setting = opts.verbosity

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		utils.exitWithUsage(usage, "Config file not specified!")

	# big try... except block to catch exceptions and print error message
	try:
		config = Config(args[0])
		# Read default command line options from config file
		defaultCmdLine = config.get("global", "cmdargs", "", volatile=True)
		(opts.reportSite, opts.reportTime, opts.reportMod) = (0, 0, 0)
		parser.parse_args(args = defaultCmdLine.split() + sys.argv[1:], values = opts)
		def setConfigFromOpt(option, section, item, fun = lambda x: str(x)):
			if option != None:
				config.set(section, item, fun(option))
		setConfigFromOpt(opts.seed, 'jobs', 'seeds', lambda x: x.rstrip('S'))
		setConfigFromOpt(opts.maxRetry, 'jobs', 'max retry')
		setConfigFromOpt(opts.continuous, 'jobs', 'continuous')
		config.opts = opts

		# Check work dir validity (default work directory is the config file name)
		config.workDir = config.getPath('global', 'workdir', config.workDirDefault, check = False)
		if not os.path.exists(config.workDir):
			if not opts.init:
				utils.vprint("Will force initialization of %s if continued!" % config.workDir, -1)
				opts.init = True
			if utils.getUserBool("Do you want to create the working directory %s?" % config.workDir, True):
				os.makedirs(config.workDir)
		checkSpace = config.getInt('global', 'workdir space', 10, volatile=True)

		# Initialise application module
		module = Module.open(config.get('global', 'module'), config)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(module)
			return 0

		# Initialise monitoring module
		monitor = MonitoringMultiplexer(config, module, config.get('jobs', 'monitor', 'scripts'))

		# Initialise workload management interface
		backend = config.get('global', 'backend', 'grid')
		defaultwms = { 'grid': 'GliteWMS', 'local': 'LocalWMS' }
		if backend == 'grid':
			wms = WMS.open(config.get(backend, 'wms', 'GliteWMS'), config, module, monitor)
		elif backend == 'local':
			wms = WMS.open(defaultwms[backend], config, module, monitor)
		else:
			raise ConfigError("Invalid backend specified!" % config.workDir)

		# Initialise job database
		jobs = JobDB(config, module, monitor)

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			Help().getConfig(config, opts.help_cfg)
			return 0

		# If invoked in report mode, just show report and exit
		if Report(jobs, jobs).show(opts, module):
			return 0

		# Check if jobs have to be deleted and exit
		if opts.delete != None:
			jobs.delete(wms, opts.delete)
			return 0

		savedConfigPath = os.path.join(config.workDir, 'work.conf')
		if opts.init:
			# Save working config file - no runtime config file changes should happen after this point
			config.allowSet = False
			config.parser.write(open(savedConfigPath, 'w'))
		else:
			# Compare config files
			if config.needInit(savedConfigPath):
				if utils.getUserBool("Quit grid-control in order to initialize the task again?", False):
					return 0

		if opts.continuous and not opts.gui:
			utils.vprint(level = -1)
			Report(jobs, jobs).summary()
			utils.vprint("Running in continuous mode. Press ^C to exit.", -1)

		# Job submission loop
		def jobCycle(wait = utils.wait):
			while True:
				didWait = False
				# Check whether wms can submit
				if not wms.canSubmit(module.wallTime, opts.submission):
					opts.submission = False
				# Check free disk space
				freeSpace = lambda x: x.f_bavail * x.f_bsize / 1024**2
				if (checkSpace > 0) and freeSpace(os.statvfs(config.workDir)) < checkSpace:
					raise RuntimeError("Not enough space left in working directory")

				# check for jobs
				if not utils.abort() and jobs.check(wms):
					didWait = wait(wms.getTimings()[1])
				# retrieve finished jobs
				if not utils.abort() and jobs.retrieve(wms):
					didWait = wait(wms.getTimings()[1])
				# try submission
				if opts.submission:
					if not utils.abort() and jobs.submit(wms):
						didWait = wait(wms.getTimings()[1])

				# quit if abort flag is set or not in continuous mode
				if utils.abort() or not opts.continuous:
					break
				# idle timeout
				if not didWait:
					wait(wms.getTimings()[0])

		if opts.gui:
			from grid_control import gui
			gui.ANSIGUI(jobs, jobCycle)
		else:
			jobCycle()

	except GCError:
		sys.stderr.write(GCError.message)
		return 1

	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
