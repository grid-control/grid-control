from grid_control.abstract import NamedObject, ClassFactory
from grid_control.tasks import TaskModule
from grid_control.monitoring import Monitoring
from grid_control.backends import WMS
from grid_control.job_manager import JobManager
from grid_control.gui import GUI
from grid_control import utils
from grid_control.report import Report

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
			('monitor', 'scripts'), ('monitor manager', 'MultiMonitor')).getInstance(self.task)

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
		self.submitFlag = True


	# Job submission loop
	def jobCycle(self, wait = utils.wait):
		while True:
			(didWait, lastSpaceMsg) = (False, 0)
			# Check whether wms can submit
			if not self.wms.canSubmit(self.task.wallTime, self.submitFlag):
				self.submitFlag = False
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
					elif action.startswith('s') and not utils.abort() and self.submitFlag:
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
