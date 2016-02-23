#-#  Copyright 2014-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import logging
from grid_control import utils
from grid_control.backends import WMS
from grid_control.gc_plugin import NamedPlugin
from grid_control.gui import GUI
from grid_control.job_manager import JobManager
from grid_control.logging_setup import LogEveryNsec
from grid_control.monitoring import Monitoring
from grid_control.tasks import TaskModule
from python_compat import imap

# Workflow class
class Workflow(NamedPlugin):
	configSections = NamedPlugin.configSections + ['global', 'workflow']
	tagName = 'workflow'

	def __init__(self, config, name):
		NamedPlugin.__init__(self, config, name)
		self._workDir = config.getWorkPath()
		# Initialise task module
		self.task = config.getPlugin(['task', 'module'], cls = TaskModule, tags = [self])
		utils.vprint('Current task ID: %s' % self.task.taskID, -1)
		utils.vprint('Task started on %s' % self.task.taskDate, -1)

		# Initialise monitoring module
		self.monitor = config.getCompositePlugin('monitor', 'scripts', 'MultiMonitor',
			cls = Monitoring, tags = [self, self.task], pargs = (self.task,))

		# Initialise workload management interface
		self.wms = config.getCompositePlugin('backend', 'grid', 'MultiWMS',
			cls = WMS, tags = [self, self.task])

		# Initialise job database
		self.jobManager = config.getPlugin('job manager', 'SimpleJobManager',
			cls = JobManager, tags = [self, self.task, self.wms], pargs = (self.task, self.monitor))

		# Prepare work package
		self.wms.deployTask(self.task, self.monitor)

		configJobs = config.changeView(viewClass = 'TaggedConfigView', addSections = ['jobs'], addTags = [self])
		self._actionList = configJobs.getList('action', ['check', 'retrieve', 'submit'], onChange = None)
		self.runContinuous = configJobs.getBool('continuous', False, onChange = None)

		self._checkSpace = config.getInt('workdir space', 10, onChange = None)
		self._submitFlag = config.getBool('submission', True, onChange = None)
		self._gui = config.getPlugin('gui', 'SimpleConsole', cls = GUI, onChange = None, pargs = (self,))


	# Job submission loop
	def jobCycle(self, wait = utils.wait):
		wmsTiming = self.wms.getTimings()
		while True:
			didWait = False
			# Check whether wms can submit
			if not self.wms.canSubmit(self.task.wallTime, self._submitFlag):
				self._submitFlag = False
			# Check free disk space
			spaceLogger = logging.getLogger('user.space')
			spaceLogger.addFilter(LogEveryNsec(5 * 60))
			if (self._checkSpace > 0) and utils.freeSpace(self._workDir) < self._checkSpace:
				spaceLogger.warning('Not enough space left in working directory')
			else:
				for action in imap(str.lower, self._actionList):
					if action.startswith('c') and not utils.abort():   # check for jobs
						if self.jobManager.check(self.wms):
							didWait = wait(wmsTiming.waitBetweenSteps)
					elif action.startswith('r') and not utils.abort(): # retrieve finished jobs
						if self.jobManager.retrieve(self.wms):
							didWait = wait(wmsTiming.waitBetweenSteps)
					elif action.startswith('s') and not utils.abort() and self._submitFlag:
						if self.jobManager.submit(self.wms):
							didWait = wait(wmsTiming.waitBetweenSteps)

			# quit if abort flag is set or not in continuous mode
			if utils.abort() or not self.runContinuous:
				break
			# idle timeout
			if not didWait:
				wait(wmsTiming.waitOnIdle)

	def run(self):
		self._gui.displayWorkflow()
