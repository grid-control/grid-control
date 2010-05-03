#!/usr/bin/env python
import sys, os, signal, optparse

# add python subdirectory from where go.py was started to search path
sys.path.insert(1, os.path.join(sys.path[0], 'python'))

# and include grid_control python module
from grid_control import *

usage = "Syntax: %s [OPTIONS] <config file>\n" % sys.argv[0]
def print_help(*args):
	sys.stderr.write("%s\n%s" % (usage, open(utils.pathGC('share', 'help.txt'), 'r').read()))
	sys.exit(0)

def main(args):
	global opts, log, handler
	log = None

	# display the 'grid-control' logo and version
	print(open(utils.pathGC('share', 'logo.txt'), 'r').read())
	print('Revision: %s' % utils.getVersion())
	pyver = sys.version_info[0] + sys.version_info[1] / 10.0
	if pyver < 2.3:
		utils.deprecated("This python version (%.1f) is not supported anymore!" % pyver)

	parser = optparse.OptionParser(add_help_option=False)
	parser.add_option("-h", "--help",          action="callback", callback=print_help),
	parser.add_option("",   "--help-vars",     dest="help_vars",  default=False, action="store_true")
	parser.add_option("",   "--help-conf",     dest="help_cfg",   default=False, action="store_true")
	parser.add_option("",   "--help-confmin",  dest="help_scfg",  default=False, action="store_true")
	parser.add_option("-s", "--no-submission", dest="submission", default=True,  action="store_false")
	parser.add_option("-q", "--requery",       dest="resync",     default=False, action="store_true")
	parser.add_option("-i", "--init",          dest="init",       default=False, action="store_true")
	parser.add_option("-c", "--continuous",    dest="continuous", default=False, action="store_true")
	parser.add_option("-G", "--gui",           dest="gui",        default=False, action="store_true")
	parser.add_option("-r", '--report',        dest="report",     default=False, action="store_true")
	parser.add_option("-v", "--verbose",       dest="verbosity",  default=0,     action="count")
	parser.add_option("-R", '--site-report',   dest="reportSite", default=0,     action="count")
	parser.add_option("-T", '--time-report',   dest="reportTime", default=0,     action="count")
	parser.add_option("-M", '--module-report', dest="reportMod",  default=0,     action="count")
	parser.add_option("-m", '--max-retry',     dest="maxRetry",   default=None,  type="int")
	parser.add_option("-d", '--delete',        dest="delete",     default=None)
	parser.add_option("-S", '--seed',          dest="seed",       default=None)
	(opts, args) = parser.parse_args()
	utils.verbosity.setting = opts.verbosity
	opts.abort = False

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		utils.exitWithUsage(usage, "Config file not specified!")

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global opts, log, handler
		opts.abort = True
		log = utils.ActivityLog('Quitting grid-control! (This can take a few seconds...)')
		signal.signal(signal.SIGINT, handler)
	handler = signal.signal(signal.SIGINT, interrupt)

	# big try... except block to catch exceptions and print error message
	try:
		# try to open config file
		try:
			open(args[0], 'r')
			config = Config(args[0])
			# Read default command line options from config file
			defaultCmdLine = config.get("global", "cmdargs", "", volatile=True)
			(opts.reportSite, opts.reportTime, opts.reportMod) = (0, 0, 0)
			parser.parse_args(args = defaultCmdLine.split() + sys.argv[1:], values = opts)
			config.opts = opts
		except IOError:
			raise ConfigError("Error while reading configuration file '%s'!" % args[0])

		# Check work dir validity (default work directory is the config file name)
		config.workDir = config.getPath('global', 'workdir', config.workDirDefault)
		if not os.path.exists(config.workDir):
			if not opts.init:
				print("Will force initialization of %s if continued!" % config.workDir)
				opts.init = True
			if utils.boolUserInput("Do you want to create the working directory %s?" % config.workDir, True):
				os.makedirs(config.workDir)

		# Initialise application module
		module = config.get('global', 'module')
		module = Module.open(module, config)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(module)
			return 0

		# Initialise monitoring module
		monitor = config.get('jobs', 'monitor', 'scripts', volatile=True)
		try:
			if config.getBool('jobs', 'monitor job', volatile=True):
				monitor = "dashboard"
			utils.deprecated("Please use [jobs] monitor = dashboard!")
		except:
			pass
		monitor = Monitoring.open(monitor, config, module)

		# Initialise workload management interface
		backend = config.get('global', 'backend', 'grid')
		defaultwms = { 'grid': 'GliteWMS', 'local': 'LocalWMS' }
		if backend == 'grid':
			wms = WMS.open(config.get(backend, 'wms', 'GliteWMS'), config, module, monitor)
		elif backend == 'local':
			wms = WMS.open(defaultwms[backend], config, module, monitor)
		else:
			raise UserError("Invalid backend specified!" % config.workDir)

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
			# Save working config file
			config.parser.write(open(savedConfigPath, 'w'))
		else:
			# Compare config files
			if config.needInit(savedConfigPath):
				if utils.boolUserInput("Quit grid-control in order to initialize the task again?", False):
					sys.exit(0)

		if opts.continuous and not opts.gui:
			print('')
			Report(jobs, jobs).summary()
			print("Running in continuous mode. Press ^C to exit.")

		if not wms.canSubmit(module.wallTime, opts.submission):
			opts.submission = False

		# Job submission loop
		def jobCycle(wait = utils.wait):
			while True:
				didWait = False
				# Check free disk space
				if int(os.popen("df -P -m '%s'" % config.workDir).readlines()[-1].split()[3]) < 10:
					raise RuntimeError("Not enough space left in working directory")

				# check for jobs
				if not opts.abort and jobs.check(wms):
					didWait = wait(opts, wms.getTimings()[1])
				# retrieve finished jobs
				if not opts.abort and jobs.retrieve(wms):
					didWait = wait(opts, wms.getTimings()[1])
				# try submission
				if not opts.abort and jobs.submit(wms):
					didWait = wait(opts, wms.getTimings()[1])

				# quit if abort flag is set or not in continuous mode
				if opts.abort or not opts.continuous:
					break
				# idle timeout
				wait(opts, wms.getTimings()[0])
				# Check whether wms can submit
				if not wms.canSubmit(module.wallTime, opts.submission):
					opts.submission = False

		if opts.gui:
			from grid_control import gui
			gui.ANSIGUI(jobs, jobCycle)
		else:
			jobCycle()

	except GCError, e:
		e.showMessage()
		return 1

	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
