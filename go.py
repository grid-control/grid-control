#!/usr/bin/env python
import sys, os, signal, optparse, time, logging

# Load grid-control package
sys.path.insert(1, os.path.join(sys.path[0], 'packages'))
from gcPackage import *

usage = 'Syntax: %s [OPTIONS] <config file>\n' % sys.argv[0]
def print_help(*args):
	utils.eprint('%s\n%s' % (usage, open(utils.pathShare('help.txt'), 'r').read()))
	sys.exit(0)

# Workflow class
class Workflow(NamedObject):
	getConfigSections = NamedObject.createFunction_getConfigSections(['workflow', 'global'])

	def __init__(self, config, name):
		NamedObject.__init__(self, config, name)
		self._workDir = config.getWorkPath()
		# Initialise task module
		self.task = config.getClass(['task', 'module'], cls = TaskModule, tags = [self]).getInstance()
		utils.vprint('Current task ID: %s' % self.task.taskID, -1)
		utils.vprint('Task started on %s' % self.task.taskDate, -1)

		# Initialise monitoring module
		self.monitor = ClassFactory(Monitoring, config, [self.task],
			('monitor', 'scripts'), ('monitor manager', 'Monitoring')).getInstance(self.task)

		# Initialise workload management interface
		self.wms = ClassFactory(WMS, config, [self.task],
			('backend', 'grid'), ('backend manager', 'MultiWMS')).getInstance()

		# Initialise job database
		jobManagerCls = config.getClass('job manager', 'SimpleJobManager', cls = JobManager,
			tags = [self.task, self.wms])
		self.jobManager = jobManagerCls.getInstance(self.task, self.monitor)

		# Prepare work package
		self.wms.deployTask(self.task, self.monitor)

		global_config = config.clone()
		self.actionList = global_config.getList('jobs', 'action', ['check', 'retrieve', 'submit'], onChange = None)
		self.runContinuous = global_config.getBool('jobs', 'continuous', False, onChange = None)

		self.checkSpace = config.getInt('workdir space', 10, onChange = None)
		self.guiClass = config.get('gui', 'SimpleConsole', onChange = None)


	# Job submission loop
	def jobCycle(self, wait = utils.wait):
		while True:
			(didWait, lastSpaceMsg) = (False, 0)
			# Check whether wms can submit
			if not self.wms.canSubmit(self.task.wallTime, opts.submission):
				opts.submission = False
			# Check free disk space
			if (self.checkSpace > 0) and utils.freeSpace(self._workDir) < self.checkSpace:
				if time.time() - lastSpaceMsg > 5 * 60:
					utils.vprint('Not enough space left in working directory', -1, True)
					lastSpaceMsg = time.time()
			else:
				for action in map(str.lower, self.actionList):
					if action.startswith('c') and not utils.abort():   # check for jobs
						if self.jobManager.check(self.wms):
							didWait = wait(self.wms.getTimings()[1])
					elif action.startswith('r') and not utils.abort(): # retrieve finished jobs
						if self.jobManager.retrieve(self.wms):
							didWait = wait(self.wms.getTimings()[1])
					elif action.startswith('s') and not utils.abort() and opts.submission:
						if self.jobManager.submit(self.wms):
							didWait = wait(self.wms.getTimings()[1])

			# quit if abort flag is set or not in continuous mode
			if utils.abort() or not self.runContinuous:
				break
			# idle timeout
			if not didWait:
				wait(self.wms.getTimings()[0])

	def run(self):
		if self.runContinuous and self.guiClass == 'SimpleConsole':
			utils.vprint(level = -1)
			Report(self.jobManager.jobDB).summary()
			utils.vprint('Running in continuous mode. Press ^C to exit.', -1)

		cycler = GUI.open(self.guiClass, self.jobCycle, self.jobManager, self.task)
		cycler.run()
Workflow.registerObject(tagName = 'workflow')


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
		config = CompatConfig(configFile = args[0], optParser = parser)
		config.opts = opts
		logging_setup(config.addSections(['logging']))

		# Check work dir validity (default work directory is the config file name)
		if not os.path.exists(config.getWorkPath()):
			config.set('global', '#init config', 'True')
			if not opts.init:
				utils.vprint('Will force initialization of %s if continued!' % config.getWorkPath(), -1)
				opts.init = True
			if utils.getUserBool('Do you want to create the working directory %s?' % config.getWorkPath(), True):
				utils.ensureDirExists(config.getWorkPath(), 'work directory')

		workflow = config.getClass('global', 'workflow', 'Workflow:global', cls = Workflow).getInstance()
		config.freezeConfig(writeConfig = config.getBool('global', '#init config', False, onChange = None))

#		if not opts.init:
#				if utils.getUserBool('\nQuit grid-control in order to initialize the task again?', False):
#					sys.exit(0)
#				if utils.getUserBool('\nOverwrite currently saved configuration to remove warning in the future?', False):
#					config.freezeConfig(writeConfig = True)

		# Give help about variables
		if opts.help_vars:
			Help().listVars(workflow.task)
			sys.exit(0)

		# Give config help
		if opts.help_cfg or opts.help_scfg:
			config.write(sys.stdout, printDefault = opts.help_cfg, printUnused = False)
			sys.exit(0)

		# If invoked in report mode, just show report and exit
		if Report(workflow.jobManager.jobDB).show(opts, workflow.task):
			sys.exit(0)

		# Check if jobs have to be deleted / reset and exit
		if opts.delete:
			workflow.jobManager.delete(workflow.wms, opts.delete)
			sys.exit(0)
		if opts.reset:
			workflow.jobManager.reset(workflow.wms, opts.reset)
			sys.exit(0)

		workflow.run()

	handleException(main)
