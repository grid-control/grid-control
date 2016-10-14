# | Copyright 2014-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import sys, time, logging
from grid_control import utils
from grid_control.backends import WMS
from grid_control.gc_plugin import NamedPlugin
from grid_control.gui import GUI, SimpleActivityStream
from grid_control.job_manager import JobManager
from grid_control.logging_setup import LogEveryNsec
from grid_control.monitoring import Monitoring
from grid_control.tasks import TaskModule
from python_compat import imap


# Workflow class
class Workflow(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['global', 'workflow']
	config_tag_name = 'workflow'

	def __init__(self, config, name, abort=None):
		NamedPlugin.__init__(self, config, name)

		# Initial activity stream
		sys.stdout = SimpleActivityStream(sys.stdout, register_callback=True)
		sys.stderr = SimpleActivityStream(sys.stderr)

		# Workdir settings
		self._path_work = config.get_work_path()
		self._check_space = config.get_int('workdir space', 10, on_change=None)

		# Initialise task module
		self.task = config.get_plugin(['module', 'task'], cls=TaskModule, tags=[self])
		if abort == 'task':
			return

		self._log.log(logging.INFO, 'Current task ID: %s', self.task.task_id)
		self._log.log(logging.INFO, 'Task started on: %s', self.task.task_date)

		# Initialise workload management interface
		self.wms = config.get_composited_plugin('backend', 'grid', 'MultiWMS',
			cls=WMS, tags=[self, self.task])

		# Subsequent config calls also include section "jobs":
		jobs_config = config.change_view(view_class='TaggedConfigView',
			add_sections=['jobs'], add_tags=[self])

		# Initialise monitoring module
		monitor = jobs_config.get_composited_plugin('monitor', 'scripts', 'MultiMonitor',
			cls=Monitoring, tags=[self, self.task], pargs=(self.task,))

		# Initialise job database
		self.job_manager = jobs_config.get_plugin('job manager', 'SimpleJobManager',
			cls=JobManager, tags=[self, self.task, self.wms], pargs=(self.task, monitor))

		if abort == 'jobmanager':
			return

		# Prepare work package
		self.wms.deploy_task(self.task, monitor,
			transfer_se=config.get_state('init', detail='storage'),
			transfer_sb=config.get_state('init', detail='sandbox'))

		# Configure workflow settings
		self._action_list = jobs_config.get_list('action',
			['check', 'retrieve', 'submit'], on_change=None)
		self.duration = 0
		if jobs_config.get_bool('continuous', False, on_change=None):  # legacy option
			self.duration = -1
		self.duration = jobs_config.get_time('duration', self.duration, on_change=None)
		self._submit_flag = jobs_config.get_bool('submission', True, on_change=None)
		self._submit_time = jobs_config.get_time('submission time requirement',
			self.task.wall_time, on_change=None)

		# Initialise GUI
		(sys.stdout, sys.stderr) = (sys.stdout.finish(), sys.stderr.finish())
		self._gui = config.get_plugin('gui', 'SimpleConsole', cls=GUI, on_change=None, pargs=(self,))

		self._space_logger = logging.getLogger('workflow.space')
		self._space_logger.addFilter(LogEveryNsec(interval=5 * 60))

	def process(self, wait=utils.wait):
		# Job submission loop
		wms_timing_info = self.wms.get_interval_info()
		t_start = time.time()
		while True:
			did_wait = False
			# Check whether wms can submit
			if not self.wms.can_submit(self._submit_time, self._submit_flag):
				self._submit_flag = False
			# Check free disk space
			if (self._check_space > 0) and utils.disk_usage(self._path_work) < self._check_space:
				self._space_logger.warning('Not enough space left in working directory')
			else:
				did_wait = self._run_actions(wait, wms_timing_info)

			# quit if abort flag is set or not in continuous mode
			if utils.abort() or ((self.duration >= 0) and (time.time() - t_start > self.duration)):
				break
			# idle timeout
			if not did_wait:
				wait(wms_timing_info.wait_on_idle)
		self.job_manager.finish()

	def run(self):
		self._gui.display_workflow(workflow=self)

	def _run_actions(self, wait, wms_timing_info):
		did_wait = False
		for action in imap(str.lower, self._action_list):
			if action.startswith('c') and not utils.abort():   # check for jobs
				if self.job_manager.check(self.task, self.wms):
					did_wait = wait(wms_timing_info.wait_between_steps)
			elif action.startswith('r') and not utils.abort():  # retrieve finished jobs
				if self.job_manager.retrieve(self.task, self.wms):
					did_wait = wait(wms_timing_info.wait_between_steps)
			elif action.startswith('s') and not utils.abort() and self._submit_flag:
				if self.job_manager.submit(self.task, self.wms):
					did_wait = wait(wms_timing_info.wait_between_steps)
		return did_wait
