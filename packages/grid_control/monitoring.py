# | Copyright 2009-2016 Karlsruhe Institute of Technology
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
from python_compat import imap, lchain, lmap


class EventHandler(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['events']
	config_tag_name = 'event'

	def __init__(self, config, name, task):
		NamedPlugin.__init__(self, config, name)
		self._task = task

	def onJobSubmit(self, wms, jobObj, jobnum):
		pass

	def onJobUpdate(self, wms, jobObj, jobnum, data):
		pass

	def onJobOutput(self, wms, jobObj, jobnum, retCode):
		pass

	def onTaskFinish(self, nJobs):
		pass

	def onFinish(self):
		pass


class MultiEventHandler(EventHandler):
	def __init__(self, config, name, handlerList, task):
		EventHandler.__init__(self, config, name, task)
		self._handlers = handlerList

	def onJobSubmit(self, wms, jobObj, jobnum):
		for handler in self._handlers:
			handler.onJobSubmit(wms, jobObj, jobnum)

	def onJobUpdate(self, wms, jobObj, jobnum, data):
		for handler in self._handlers:
			handler.onJobUpdate(wms, jobObj, jobnum, data)

	def onJobOutput(self, wms, jobObj, jobnum, retCode):
		for handler in self._handlers:
			handler.onJobOutput(wms, jobObj, jobnum, retCode)

	def onTaskFinish(self, nJobs):
		for handler in self._handlers:
			handler.onTaskFinish(nJobs)

	def onFinish(self):
		for handler in self._handlers:
			handler.onFinish()


# Monitoring base class with submodule support
class Monitoring(EventHandler):
	config_tag_name = 'monitor'

	# Script to call later on
	def getScript(self):
		return []

	def get_task_dict(self):
		return {'GC_MONITORING': str.join(' ', imap(os.path.basename, self.getScript()))}

	def getFiles(self):
		return []


class MultiMonitor(MultiEventHandler, Monitoring):
	def getScript(self):
		return lchain(imap(lambda h: h.getScript(), self._handlers))

	def get_task_dict(self):
		tmp = Monitoring.get_task_dict(self)
		return utils.merge_dict_list(lmap(lambda m: m.get_task_dict(), self._handlers) + [tmp])

	def getFiles(self):
		return lchain(lmap(lambda h: h.getFiles(), self._handlers) + [self.getScript()])


class ScriptMonitoring(Monitoring):
	alias_list = ['scripts']
	config_section_list = EventHandler.config_section_list + ['scripts']

	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		self._silent = config.get_bool('silent', True, on_change = None)
		self._evtSubmit = config.get_command('on submit', '', on_change = None)
		self._evtStatus = config.get_command('on status', '', on_change = None)
		self._evtOutput = config.get_command('on output', '', on_change = None)
		self._evtFinish = config.get_command('on finish', '', on_change = None)
		self._runningMax = config.get_time('script timeout', 5, on_change = None)
		self._workPath = config.get_work_path()
		self._tp = GCThreadPool()

	# Get both task and job config / state dicts
	def _scriptThread(self, script, jobnum = None, jobObj = None, allDict = None):
		try:
			tmp = {}
			if jobObj is not None:
				for key, value in jobObj.get_dict().items():
					tmp[key.upper()] = value
			tmp['WORKDIR'] = self._workPath
			tmp.update(self._task.get_task_dict())
			if jobnum is not None:
				tmp.update(self._task.get_job_dict(jobnum))
			tmp.update(allDict or {})
			env = dict(os.environ)
			for key, value in tmp.items():
				if not key.startswith('GC_'):
					key = 'GC_' + key
				env[key] = str(value)

			script = self._task.substVars('monitoring script', script, jobnum, tmp)
			if not self._silent:
				proc = LocalProcess(*shlex.split(script), **{'environment': env})
				proc_output = proc.get_output(timeout = self._runningMax)
				if proc_output.strip():
					self._log.info(proc_output.strip())
			else:
				os.system(script)
		except Exception:
			self._log.exception('Error while running user script')

	def _runInBackground(self, script, jobnum = None, jobObj = None, addDict = None):
		if script != '':
			self._tp.start_daemon('Running monitoring script %s' % script,
				self._scriptThread, script, jobnum, jobObj, addDict)

	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobnum):
		self._runInBackground(self._evtSubmit, jobnum, jobObj)

	# Called on job status update
	def onJobUpdate(self, wms, jobObj, jobnum, data):
		self._runInBackground(self._evtStatus, jobnum, jobObj)

	# Called on job status update
	def onJobOutput(self, wms, jobObj, jobnum, retCode):
		self._runInBackground(self._evtOutput, jobnum, jobObj, {'RETCODE': retCode})

	# Called at the end of the task
	def onTaskFinish(self, nJobs):
		self._runInBackground(self._evtFinish, addDict = {'NJOBS': nJobs})

	def onFinish(self):
		self._tp.wait_and_drop(self._runningMax)
