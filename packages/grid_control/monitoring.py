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

import os, shlex
from grid_control import utils
from grid_control.gc_plugin import NamedPlugin
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.thread_tools import GCThreadPool
from hpfwk import clear_current_exception
from python_compat import imap, lchain, lmap


class EventHandler(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['events']
	config_tag_name = 'event'

	def __init__(self, config, name, task):
		NamedPlugin.__init__(self, config, name)
		self._task = task

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		pass

	def on_job_submit(self, wms, job_obj, jobnum):
		pass

	def on_job_update(self, wms, job_obj, jobnum, data):
		pass

	def on_task_finish(self, job_len):
		pass

	def on_workflow_finish(self):
		pass


class Monitoring(EventHandler):
	# Monitoring base class with submodule support
	config_tag_name = 'monitor'

	def get_file_list(self):
		return []

	def get_script(self):  # Script to call later on
		return []

	def get_task_dict(self):
		return {'GC_MONITORING': str.join(' ', imap(os.path.basename, self.get_script()))}


class MultiEventHandler(EventHandler):
	def __init__(self, config, name, handler_list, task):
		EventHandler.__init__(self, config, name, task)
		self._handlers = handler_list

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		for handler in self._handlers:
			handler.on_job_output(wms, job_obj, jobnum, exit_code)

	def on_job_submit(self, wms, job_obj, jobnum):
		for handler in self._handlers:
			handler.on_job_submit(wms, job_obj, jobnum)

	def on_job_update(self, wms, job_obj, jobnum, data):
		for handler in self._handlers:
			handler.on_job_update(wms, job_obj, jobnum, data)

	def on_task_finish(self, job_len):
		for handler in self._handlers:
			handler.on_task_finish(job_len)

	def on_workflow_finish(self):
		for handler in self._handlers:
			handler.on_workflow_finish()


class ScriptMonitoring(Monitoring):
	alias_list = ['scripts']
	config_section_list = EventHandler.config_section_list + ['scripts']

	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		self._silent = config.get_bool('silent', True, on_change=None)
		self._script_submit = config.get_command('on submit', '', on_change=None)
		self._script_status = config.get_command('on status', '', on_change=None)
		self._script_output = config.get_command('on output', '', on_change=None)
		self._script_finish = config.get_command('on finish', '', on_change=None)
		self._script_timeout = config.get_time('script timeout', 5, on_change=None)
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
		self._run_in_background(self._script_finish, additional_var_dict={'NJOBS': job_len})

	def on_workflow_finish(self):
		self._tp.wait_and_drop(self._script_timeout)

	def _run_in_background(self, script, jobnum=None, job_obj=None, additional_var_dict=None):
		if script != '':
			self._tp.start_daemon('Running monitoring script %s' % script,
				self._script_thread, script, jobnum, job_obj, additional_var_dict)

	def _script_thread(self, script, jobnum=None, job_obj=None, add_dict=None):
		# Get both task and job config / state dicts
		try:
			tmp = {}
			if job_obj is not None:
				for key, value in job_obj.get_dict().items():
					tmp[key.upper()] = value
			tmp['WORKDIR'] = self._path_work
			tmp.update(self._task.get_task_dict())
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
				proc = LocalProcess(*shlex.split(script), **{'environment': env})
				proc_output = proc.get_output(timeout=self._script_timeout)
				if proc_output.strip():
					self._log.info(proc_output.strip())
			else:
				os.system(script)
		except Exception:
			self._log.exception('Error while running user script')
			clear_current_exception()


# The multimonitor inherits the multiplexing features from MultiEventHandler and the monitoring API
class MultiMonitor(MultiEventHandler, Monitoring):
	def get_file_list(self):
		return lchain(lmap(lambda h: h.get_file_list(), self._handlers) + [self.get_script()])

	def get_script(self):
		return lchain(imap(lambda h: h.get_script(), self._handlers))

	def get_task_dict(self):
		tmp = Monitoring.get_task_dict(self)
		return utils.merge_dict_list(lmap(lambda m: m.get_task_dict(), self._handlers) + [tmp])
