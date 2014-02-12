#!/usr/bin/env python
import sys, os, signal, optparse, time, logging

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
	parser.add_option('-i', '--init',          dest='init',       default=False, action='store_true')
	parser.add_option('-q', '--resync',        dest='resync',     default=False, action='store_true')
	parser.add_option('-s', '--no-submission', dest='submission', default=True,  action='store_false')
	parser.add_option('-c', '--continuous',    dest='continuous', default=None,  action='store_true')
	parser.add_option('-o', '--override',      dest='override',   default=[],    action='append')
	parser.add_option('-d', '--delete',        dest='delete',     default=None)
	parser.add_option('',   '--reset',         dest='reset',      default=None)
	parser.add_option('-a', '--action',        dest='action',     default=None)
	parser.add_option('-J', '--job-selector',  dest='selector',   default=None)
	parser.add_option('-m', '--max-retry',     dest='maxRetry',   default=None,  type='int')
	parser.add_option('-v', '--verbose',       dest='verbosity',  default=0,     action='count')
	parser.add_option('-G', '--gui',           dest='gui',        action='store_const', const = 'ANSIConsole')
	parser.add_option('-W', '--webserver',     dest='gui',        action='store_const', const = 'CPWebserver')
	Report.addOptions(parser)
	(opts, args) = parser.parse_args()
	utils.verbosity(opts.verbosity)
	logging.getLogger().setLevel(logging.DEFAULT_VERBOSITY - opts.verbosity)
	# we need exactly one positional argument (config file)
	if len(args) != 1:
		utils.exitWithUsage(usage, 'Config file not specified!')

	# big try... except block to catch exceptions and print error message
	def main():
		config = Config(configFile = args[0], optParser = parser)
		config.opts = opts
		logging_setup(config.getScoped(['logging']))

		# Check work dir validity (default work directory is the config file name)
		if not os.path.exists(config.getWorkPath()):
			if not opts.init:
				utils.vprint('Will force initialization of %s if continued!' % config.getWorkPath(), -1)
				opts.init = True
			if utils.getUserBool('Do you want to create the working directory %s?' % config.getWorkPath(), True):
				utils.ensureDirExists(config.getWorkPath(), 'work directory')
		checkSpace = config.getInt('global', 'workdir space', 10, onChange = None)

		class InitSentinel:
			def __init__(self, config):
				(self.config, self.userInit) = (config, config.opts.init)
				self.log = utils.PersistentDict(config.getWorkPath('initlog'))
				self.log.write(update = not self.userInit)

			def checkpoint(self, name):
				self.log.write()
				self.config.opts.init = QM(self.userInit, self.userInit, self.log.get(name) != 'done')
				self.log[name] = 'done'
		initSentinel = InitSentinel(config)

		# Initialis task module
		initSentinel.checkpoint('module')
		task = TaskModule.open(config.get('global', ['task', 'module']), config, 'task')
		utils.vprint('Current task ID: %s' % task.taskID, -1)
		utils.vprint('Task started on %s' % task.taskDate, -1)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(task)
			sys.exit(0)

		# Initialise monitoring module
		initSentinel.checkpoint('monitoring')
		monitor = Monitoring(config, 'monitor', task, map(lambda x: Monitoring.open(x, config, 'monitor', task),
			config.getList('jobs', 'monitor', ['scripts'])))

		# Initialise workload management interface
		initSentinel.checkpoint('backend')
		wms = WMSFactory(config).getWMS()

		# Initialise job database
		initSentinel.checkpoint('jobmanager')
		jobManager = JobManager.open('SimpleJobManager', config.getScoped(['jobs']), 'jobs', task, monitor)

		# Prepare work package
		initSentinel.checkpoint('deploy')
		wms.deployTask(task, monitor)

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			config.write(sys.stdout, printDefault = opts.help_cfg, printUnused = False)
			sys.exit(0)

		# If invoked in report mode, just show report and exit
		if Report(jobManager.jobDB).show(opts, task):
			sys.exit(0)

		# Check if jobs have to be deleted / reset and exit
		if opts.delete:
			jobManager.delete(wms, opts.delete)
			sys.exit(0)
		if opts.reset:
			jobManager.reset(wms, opts.reset)
			sys.exit(0)

		actionList = config.getList('jobs', 'action', ['check', 'retrieve', 'submit'], onChange = None)
		guiClass = config.get('global', 'gui', 'SimpleConsole', onChange = None)
		runContinuous = config.getBool('jobs', 'continuous', False, onChange = None)

		initSentinel.checkpoint('config')
		config.freezeConfig(writeConfig = opts.init)

		if runContinuous and guiClass == 'SimpleConsole':
			utils.vprint(level = -1)
			Report(jobManager.jobDB).summary()
			utils.vprint('Running in continuous mode. Press ^C to exit.', -1)

		# Job submission loop
		initSentinel.checkpoint('loop')
		def jobCycle(wait = utils.wait):
			while True:
				(didWait, lastSpaceMsg) = (False, 0)
				# Check whether wms can submit
				if not wms.canSubmit(task.wallTime, opts.submission):
					opts.submission = False
				# Check free disk space
				if (checkSpace > 0) and utils.freeSpace(config.getWorkPath()) < checkSpace:
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
				if utils.abort() or not runContinuous:
					break
				# idle timeout
				if not didWait:
					wait(wms.getTimings()[0])

		workflow = GUI.open(guiClass, jobCycle, jobManager, task)
		workflow.run()

	handleException(main)
