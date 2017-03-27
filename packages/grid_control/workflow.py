# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import time, logging
from grid_control.backends import WMS
from grid_control.gc_plugin import NamedPlugin
from grid_control.job_manager import JobManager
from grid_control.logging_setup import LogEveryNsec
from grid_control.monitoring import Monitoring
from grid_control.tasks import TaskModule
from grid_control.utils import abort, disk_space_avail, wait
from grid_control.utils.parsing import str_time_short
from python_compat import imap


class Workflow(NamedPlugin):
	# Workflow class
	config_section_list = NamedPlugin.config_section_list + ['global', 'workflow']
	config_tag_name = 'workflow'

	def __init__(self, config, name, task=None, wms=None, monitor=None, job_manager=None):
		NamedPlugin.__init__(self, config, name)

		# Work directory settings
		self._path_work = config.get_work_path()
		self._check_space = config.get_int('workdir space', 10, on_change=None)
		self._check_space_timeout = config.get_time('workdir space timeout', 5, on_change=None)

		# Initialise task module
		self.task = config.get_plugin(['module', 'task'], cls=TaskModule,
			bind_kwargs={'tags': [self]}, override=task)
		if abort():
			return

		self._log.info('Current task ID: %s', self.task.task_id)
		self._log.info('Task started on: %s', self.task.task_date)

		# Initialise workload management interface
		self.wms = config.get_composited_plugin('backend', 'grid', 'MultiWMS', cls=WMS,
			bind_kwargs={'tags': [self, self.task]}, override=wms)
		if abort():
			return

		# Subsequent config calls also include section "jobs":
		jobs_config = config.change_view(view_class='TaggedConfigView',
			add_sections=['jobs'], add_tags=[self])

		# Initialise monitoring module
		self._monitor = jobs_config.get_composited_plugin('monitor', 'scripts', 'MultiMonitor',
			cls=Monitoring, bind_kwargs={'tags': [self, self.task]}, pargs=(self.task,), override=monitor)
		if abort():
			return

		# Initialise job database
		self.job_manager = jobs_config.get_plugin('job manager', 'SimpleJobManager', cls=JobManager,
			bind_kwargs={'tags': [self, self.task, self.wms]}, pargs=(self.task, self._monitor),
			override=job_manager)
		if abort():
			return

		# Transfer SE
		self._transfer_se = config.get_state('init', detail='storage')
		self._transfer_sb = config.get_state('init', detail='sandbox')

		# Configure workflow settings
		self._action_list = jobs_config.get_list('action',
			['check', 'retrieve', 'submit'], on_change=None)
		self._duration = 0
		if jobs_config.get_bool('continuous', False, on_change=None):  # legacy option
			self._duration = -1
		self._duration = jobs_config.get_time('duration', self._duration, on_change=None)
		self._submit_flag = jobs_config.get_bool('submission', True, on_change=None)
		self._submit_time = jobs_config.get_time('submission time requirement',
			self.task.wall_time, on_change=None)

		# Initialise space logging
		self._space_logger = logging.getLogger('workflow.space')
		self._space_logger.addFilter(LogEveryNsec(interval=5 * 60))

	def run(self):
		if self._duration < 0:
			self._log.info('Running in continuous mode. Press ^C to exit.')
		elif self._duration > 0:
			self._log.info('Running for %s', str_time_short(self._duration))
		# Prepare work package
		self.wms.deploy_task(self.task, self._monitor,
			transfer_se=self._transfer_se, transfer_sb=self._transfer_sb)
		# Job submission loop
		wms_timing_info = self.wms.get_interval_info()
		t_start = time.time()
		while not abort():
			did_wait = False
			# Check whether wms can submit
			if not self.wms.can_submit(self._submit_time, self._submit_flag):
				self._submit_flag = False
			# Check free disk space
			if self._no_disk_space_left():
				self._space_logger.warning('Not enough space left in working directory')
			else:
				did_wait = self._run_actions(wms_timing_info)
			# quit if abort flag is set or not in continuous mode
			if abort() or ((self._duration >= 0) and (time.time() - t_start > self._duration)):
				break
			# idle timeout
			if not did_wait:
				wait(wms_timing_info.wait_on_idle)
		self.job_manager.finish()

	def _no_disk_space_left(self):
		if self._check_space > 0:
			return disk_space_avail(self._path_work, self._check_space_timeout) <= self._check_space

	def _run_actions(self, wms_timing_info):
		did_wait = False
		for action in imap(str.lower, self._action_list):
			if not abort():
				if action.startswith('c'):   # check for jobs
					if self.job_manager.check(self.task, self.wms):
						did_wait = wait(wms_timing_info.wait_between_steps)
				elif action.startswith('r'):  # retrieve finished jobs
					if self.job_manager.retrieve(self.task, self.wms):
						did_wait = wait(wms_timing_info.wait_between_steps)
				elif action.startswith('s') and self._submit_flag:
					if self.job_manager.submit(self.task, self.wms):
						did_wait = wait(wms_timing_info.wait_between_steps)
		return did_wait
