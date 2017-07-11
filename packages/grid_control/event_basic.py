# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import os, math, shlex, logging
from grid_control.event_base import EventHandlerManager, LocalEventHandler, RemoteEventHandler
from grid_control.job_db import Job
from grid_control.utils.parsing import str_time_long
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.thread_tools import GCThreadPool
from hpfwk import clear_current_exception, ignore_exception


class CompatEventHandlerManager(EventHandlerManager):
	alias_list = ['compat']

	def __init__(self, config):
		# This class allows to specify events handlers as done in the past with a single option
		EventHandlerManager.__init__(self, config)
		for old_monitor in config.get_list(['monitor', 'event handler'], ['scripts']):
			if ignore_exception(Exception, None, LocalEventHandler.get_class, old_monitor):
				config.set('local event handler', old_monitor, '+=', section='jobs')
			if ignore_exception(Exception, None, RemoteEventHandler.get_class, old_monitor):
				config.set('remote event handler', old_monitor, '+=', section='backend')


class BasicLogEventHandler(LocalEventHandler):
	alias_list = ['logmonitor']

	def __init__(self, config, name, task):
		LocalEventHandler.__init__(self, config, name, task)
		self._map_error_code2msg = dict(task.map_error_code2msg)
		self._log_status = logging.getLogger('jobs.status')
		self._show_wms = config.get_bool('event log show wms', False, on_change=None)

	def on_job_state_change(self, job_db_len, jobnum, job_obj, old_state, new_state, reason=None):
		jobnum_len = int(math.log10(max(1, job_db_len)) + 1)
		job_status_str_list = ['Job %s state changed from %s to %s' % (
			str(jobnum).ljust(jobnum_len), Job.enum2str(old_state), Job.enum2str(new_state))]

		if reason:
			job_status_str_list.append('(%s)' % reason)
		if self._show_wms and job_obj.gc_id:
			job_status_str_list.append('(WMS:%s)' % job_obj.gc_id.split('.')[1])
		if (new_state == Job.SUBMITTED) and (job_obj.attempt > 1):
			job_status_str_list.append('(retry #%s)' % (job_obj.attempt - 1))
		elif (new_state == Job.QUEUED) and (job_obj.get_job_location() != 'N/A'):
			job_status_str_list.append('(%s)' % job_obj.get_job_location())
		elif (new_state in [Job.WAITING, Job.ABORTED, Job.DISABLED]) and job_obj.get('reason'):
			job_status_str_list.append('(%s)' % job_obj.get('reason'))
		elif (new_state == Job.SUCCESS) and (job_obj.get('runtime') is not None):
			if (job_obj.get('runtime') or 0) >= 0:
				job_status_str_list.append('(runtime %s)' % str_time_long(job_obj.get('runtime') or 0))
		elif new_state == Job.FAILED:
			fail_msg = self._explain_failure(job_obj)
			if fail_msg:
				job_status_str_list.append('(%s)' % fail_msg)
		self._log_status.log_time(logging.INFO, str.join(' ', job_status_str_list))

	def on_task_finish(self, job_len):
		self._log_status.log_time(logging.INFO, 'Task successfully completed. Quitting grid-control!')

	def _explain_failure(self, job_obj):
		msg_list = []
		exit_code = job_obj.get('retcode')
		if exit_code:
			msg_list.append('error code: %d' % exit_code)
			if self._log_status.isEnabledFor(logging.DEBUG) and (exit_code in self._map_error_code2msg):
				msg_list.append(self._map_error_code2msg[exit_code])
		job_location = job_obj.get_job_location()
		if job_location:
			msg_list.append(job_location)
		if (job_obj.get('runtime') is not None) and ((job_obj.get('runtime') or 0) >= 0):
			msg_list.append('runtime %s' % str_time_long(job_obj.get('runtime') or 0))
		return str.join(' - ', msg_list)


class ScriptEventHandler(LocalEventHandler):
	alias_list = ['scripts']
	config_section_list = LocalEventHandler.config_section_list + ['scripts']

	def __init__(self, config, name, task):
		LocalEventHandler.__init__(self, config, name, task)
		self._silent = config.get_bool('silent', True, on_change=None)
		self._script_submit = config.get_command('on submit', '', on_change=None)
		self._script_status = config.get_command('on status', '', on_change=None)
		self._script_output = config.get_command('on output', '', on_change=None)
		self._script_finish = config.get_command('on finish', '', on_change=None)
		self._script_timeout = config.get_time('script timeout', 20, on_change=None)
		self._path_work = config.get_work_path()
		self._tp = GCThreadPool()

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		# Called on job status update
		self._run_in_background(self._script_output, jobnum, job_obj, {'RETCODE': exit_code})

	def on_job_submit(self, wms, job_obj, jobnum):
		# Called on job submission
		self._run_in_background(self._script_submit, jobnum, job_obj)

	def on_job_update(self, wms, job_obj, jobnum, data):
		# Called on job status update
		self._run_in_background(self._script_status, jobnum, job_obj)

	def on_task_finish(self, job_len):
		# Called at the end of the task
		self._run_in_background(self._script_finish, jobnum=0, additional_var_dict={'NJOBS': job_len})

	def on_workflow_finish(self):
		self._tp.wait_and_drop(self._script_timeout)

	def _run_in_background(self, script, jobnum=None, job_obj=None, additional_var_dict=None):
		if script != '':
			self._tp.start_daemon('Running event handler script %s' % script,
				self._script_thread, script, jobnum, job_obj, additional_var_dict)

	def _script_thread(self, script, jobnum=None, job_obj=None, add_dict=None):
		# Get both task and job config / state dicts
		try:
			tmp = {}
			if job_obj is not None:
				for key, value in job_obj.get_dict().items():
					tmp[key.upper()] = value
			tmp['GC_WORKDIR'] = self._path_work
			if jobnum is not None:
				tmp.update(self._task.get_job_dict(jobnum))
			tmp.update(add_dict or {})
			env = dict(os.environ)
			for key, value in tmp.items():
				if not key.startswith('GC_'):
					key = 'GC_' + key
				env[key] = str(value)

			script = self._task.substitute_variables('monitoring script', script, jobnum, tmp)
			if not self._silent:
				proc = LocalProcess(*shlex.split(script), **{'env_dict': env})
				proc_output = proc.get_output(timeout=self._script_timeout)
				if proc_output.strip():
					self._log.info(proc_output.strip())
			else:
				os.system(script)
		except Exception:
			self._log.exception('Error while running user script')
			clear_current_exception()
