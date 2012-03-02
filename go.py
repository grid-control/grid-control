#!/usr/bin/env python
import sys, os, signal, optparse, time

# Load grid-control package
sys.path.insert(1, os.path.join(sys.path[0], 'packages'))
from gcPackage import *

usage = 'Syntax: %s [OPTIONS] <config file>\n' % sys.argv[0]
def print_help(*args):
	utils.eprint('%s\n%s' % (usage, open(utils.pathShare('help.txt'), 'r').read()))
	sys.exit(0)

if __name__ == '__main__':
	global log, handler
	log = None

	# set up signal handler for interrupts
	def interrupt(sig, frame):
		global log, handler
		utils.abort(True)
		log = utils.ActivityLog('Quitting grid-control! (This can take a few seconds...)')
		signal.signal(signal.SIGINT, handler)
	handler = signal.signal(signal.SIGINT, interrupt)

	# display the 'grid-control' logo and version
	utils.vprint(open(utils.pathShare('logo.txt'), 'r').read(), -1)
	utils.vprint('Revision: %s' % utils.getVersion(), -1)
	pyver = sys.version_info[0] + sys.version_info[1] / 10.0
	if pyver < 2.3:
		utils.deprecated('This python version (%.1f) is not supported anymore!' % pyver)

	parser = optparse.OptionParser(add_help_option=False)
	parser.add_option('-h', '--help',          action='callback', callback=print_help)
	parser.add_option('',   '--help-vars',     dest='help_vars',  default=False, action='store_true')
	parser.add_option('',   '--help-conf',     dest='help_cfg',   default=False, action='store_true')
	parser.add_option('',   '--help-confmin',  dest='help_scfg',  default=False, action='store_true')
	parser.add_option('-c', '--continuous',    dest='continuous', default=False, action='store_true')
	parser.add_option('-G', '--gui',           dest='gui',        default=False, action='store_true')
	parser.add_option('-i', '--init',          dest='init',       default=False, action='store_true')
	parser.add_option('-q', '--resync',        dest='resync',     default=False, action='store_true')
	parser.add_option('-s', '--no-submission', dest='submission', default=True,  action='store_false')
	parser.add_option('-d', '--delete',        dest='delete',     default=None)
	parser.add_option('',   '--reset',         dest='reset',      default=None)
	parser.add_option('-a', '--action',        dest='action',     default=None)
	parser.add_option('-J', '--job-selector',  dest='selector',   default=None)
	parser.add_option('-S', '--seed',          dest='seed',       default=None)
	parser.add_option('-N', '--nseeds',        dest='nseeds',     default=None,  type='int')
	parser.add_option('-m', '--max-retry',     dest='maxRetry',   default=None,  type='int')
	parser.add_option('-v', '--verbose',       dest='verbosity',  default=0,     action='count')
	Report.addOptions(parser)
	(opts, args) = parser.parse_args()
	utils.verbosity(opts.verbosity)

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		utils.exitWithUsage(usage, 'Config file not specified!')

	# big try... except block to catch exceptions and print error message
	try:
		config = Config(args[0])
		# Read default command line options from config file
		defaultCmdLine = config.get('global', 'cmdargs', '', volatile=True)
		(opts.reportSite, opts.reportTime, opts.reportMod) = (0, 0, 0)
		parser.parse_args(args = defaultCmdLine.split() + sys.argv[1:], values = opts)
		def setConfigFromOpt(option, section, item, fun = lambda x: str(x)):
			if option != None:
				config.set(section, item, fun(option))
		for (cfgopt, cmdopt) in {'nseeds': opts.nseeds, 'max retry': opts.maxRetry,
			'action': opts.action, 'continuous': opts.continuous, 'selected': opts.selector}.items():
			setConfigFromOpt(cmdopt, 'jobs', cfgopt)
		setConfigFromOpt(opts.seed, 'jobs', 'seeds', lambda x: x.replace(',', ' '))
		config.opts = opts
		overlay = ConfigOverlay.open(config.get('global', 'config mode', 'verbatim'), config)

		# Check work dir validity (default work directory is the config file name)
		if not os.path.exists(config.workDir):
			if not opts.init:
				utils.vprint('Will force initialization of %s if continued!' % config.workDir, -1)
				opts.init = True
			if utils.getUserBool('Do you want to create the working directory %s?' % config.workDir, True):
				os.makedirs(config.workDir)
		checkSpace = config.getInt('global', 'workdir space', 10, volatile=True)

		class InitSentinel:
			def __init__(self, config):
				(self.config, self.userInit) = (config, config.opts.init)
				self.log = utils.PersistentDict(os.path.join(config.workDir, 'initlog'))
				self.log.write(update = not self.userInit)

			def checkpoint(self, name):
				self.log.write()
				self.config.opts.init = QM(self.userInit, self.userInit, self.log.get(name) != 'done')
				self.log[name] = 'done'
		initSentinel = InitSentinel(config)

		# Initialise application module
		initSentinel.checkpoint('module')
		module = Module.open(config.get('global', 'module'), config)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(module)
			sys.exit(0)

		# Initialise monitoring module
		initSentinel.checkpoint('monitoring')
		monitor = Monitoring(config, module, map(lambda x: Monitoring.open(x, config, module),
			config.getList('jobs', 'monitor', ['scripts'])))

		# Initialise workload management interface
		initSentinel.checkpoint('backend')
		backend = config.get('global', 'backend', 'grid')
		defaultwms = { 'grid': 'GliteWMS', 'local': 'LocalWMS' }
		if backend == 'grid':
			wms = WMS.open(config.get(backend, 'wms', 'GliteWMS'), config, module, monitor)
		elif backend == 'local':
			wms = WMS.open(defaultwms[backend], config, module, monitor)
		else:
			raise ConfigError('Invalid backend specified!' % config.workDir)

		# Initialise job database
		initSentinel.checkpoint('jobmanager')
		jobManager = JobManager(config, module, monitor)

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			config.prettyPrint(sys.stdout, printDefault = opts.help_cfg, printUnused = False)
			sys.exit(0)

		# If invoked in report mode, just show report and exit
		if Report(jobManager.jobDB).show(opts, module):
			sys.exit(0)

		# Check if jobs have to be deleted / reset and exit
		if opts.delete:
			jobManager.delete(wms, opts.delete)
			sys.exit(0)
		if opts.reset:
			jobManager.reset(wms, opts.reset)
			sys.exit(0)

		actionList = config.getList('jobs', 'action', ['check', 'retrieve', 'submit'], volatile=True)

		# Check for 
		if config.get('global', 'file check', False, volatile=True):
			if utils.fileCheck(config):
				if utils.getUserBool('\nQuit grid-control in order to initialize the task again?', False):
					sys.exit(0)

		initSentinel.checkpoint('config')
		savedConfigPath = os.path.join(config.workDir, 'work.conf')
		if not opts.init:
			# Compare config files
			if config.needInit(savedConfigPath):
				if utils.getUserBool('\nQuit grid-control in order to initialize the task again?', False):
					sys.exit(0)
				if utils.getUserBool('\nOverwrite currently saved configuration to remove warning in the future?', False):
					config.prettyPrint(open(savedConfigPath, 'w'))
		else:
			# Save working config file - no runtime config file changes should happen after this point
			config.prettyPrint(open(savedConfigPath, 'w'))
		config.allowSet = False

		if opts.continuous and not opts.gui:
			utils.vprint(level = -1)
			Report(jobManager.jobDB).summary()
			utils.vprint('Running in continuous mode. Press ^C to exit.', -1)

		# Job submission loop
		initSentinel.checkpoint('loop')
		def jobCycle(wait = utils.wait):
			while True:
				(didWait, lastSpaceMsg) = (False, 0)
				# Check whether wms can submit
				if not wms.canSubmit(module.wallTime, opts.submission):
					opts.submission = False
				# Check free disk space
				freeSpace = lambda x: x.f_bavail * x.f_bsize / 1024**2
				if (checkSpace > 0) and freeSpace(os.statvfs(config.workDir)) < checkSpace:
					if time.time() - lastSpaceMsg > 5 * 60:
						utils.vprint('Not enough space left in working directory', -1, True)
						lastSpaceMsg = time.time()
				else:
					for action in map(str.lower, actionList):
						if action.startswith('c') and not utils.abort():   # check for jobs
							if jobManager.check(wms):
								didWait = wait(wms.getTimings()[1])
						elif action.startswith('r') and not utils.abort(): # retrieve finished jobs
							if jobManager.retrieve(wms):
								didWait = wait(wms.getTimings()[1])
						elif action.startswith('s') and not utils.abort() and opts.submission:
							if jobManager.submit(wms):
								didWait = wait(wms.getTimings()[1])

				# quit if abort flag is set or not in continuous mode
				if utils.abort() or not opts.continuous:
					break
				# idle timeout
				if not didWait:
					wait(wms.getTimings()[0])

		if opts.gui:
			from grid_control import gui
			gui.ANSIGUI(jobManager, jobCycle)
		else:
			jobCycle()

	except GCError:
		sys.stderr.write(GCError.message)
		if utils.verbosity() > 2:
			raise RethrowError('')
		sys.exit(1)

	sys.exit(0)
