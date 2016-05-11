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
from grid_control.job_db import Job
from grid_control.utils.gc_itertools import lchain
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.thread_tools import GCThreadPool
from python_compat import imap, lmap

class EventHandler(NamedPlugin):
	configSections = NamedPlugin.configSections + ['events']
	tagName = 'event'

	def __init__(self, config, name, task):
		NamedPlugin.__init__(self, config, name)
		self._task = task

	def onJobSubmit(self, wms, jobObj, jobNum):
		pass

	def onJobUpdate(self, wms, jobObj, jobNum, data):
		pass

	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		pass

	def onTaskFinish(self, nJobs):
		pass

	def onFinish(self):
		pass


class MultiEventHandler(EventHandler):
	def __init__(self, config, name, handlerList, task):
		EventHandler.__init__(self, config, name, task)
		self._handlers = handlerList

	def onJobSubmit(self, wms, jobObj, jobNum):
		for handler in self._handlers:
			handler.onJobSubmit(wms, jobObj, jobNum)

	def onJobUpdate(self, wms, jobObj, jobNum, data):
		for handler in self._handlers:
			handler.onJobUpdate(wms, jobObj, jobNum, data)

	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		for handler in self._handlers:
			handler.onJobOutput(wms, jobObj, jobNum, retCode)

	def onTaskFinish(self, nJobs):
		for handler in self._handlers:
			handler.onTaskFinish(nJobs)

	def onFinish(self):
		for handler in self._handlers:
			handler.onFinish()


# Monitoring base class with submodule support
class Monitoring(EventHandler):
	tagName = 'monitor'

	# Script to call later on
	def getScript(self):
		return []

	def getTaskConfig(self):
		return {}

	def getFiles(self):
		return []


class MultiMonitor(MultiEventHandler, Monitoring):
	def getScript(self):
		return lchain(imap(lambda h: h.getScript(), self._handlers))

	def getTaskConfig(self):
		tmp = {'GC_MONITORING': str.join(' ', imap(os.path.basename, self.getScript()))}
		return utils.mergeDicts(lmap(lambda m: m.getTaskConfig(), self._handlers) + [tmp])

	def getFiles(self):
		return lchain(lmap(lambda h: h.getFiles(), self._handlers) + [self.getScript()])


class ScriptMonitoring(Monitoring):
	alias = ['scripts']
	configSections = EventHandler.configSections + ['scripts']

	def __init__(self, config, name, task):
		Monitoring.__init__(self, config, name, task)
		self._silent = config.getBool('silent', True, onChange = None)
		self._evtSubmit = config.getCommand('on submit', '', onChange = None)
		self._evtStatus = config.getCommand('on status', '', onChange = None)
		self._evtOutput = config.getCommand('on output', '', onChange = None)
		self._evtFinish = config.getCommand('on finish', '', onChange = None)
		self._runningMax = config.getTime('script timeout', 5, onChange = None)
		self._workPath = config.getWorkPath()
		self._tp = GCThreadPool()

	# Get both task and job config / state dicts
	def _scriptThread(self, script, jobNum = None, jobObj = None, allDict = None):
		try:
			tmp = {}
			if jobNum is not None:
				tmp.update(self._task.getSubmitInfo(jobNum))
			if jobObj is not None:
				tmp.update(jobObj.getAll())
			tmp['WORKDIR'] = self._workPath
			tmp.update(self._task.getTaskConfig())
			if jobNum is not None:
				tmp.update(self._task.getJobConfig(jobNum))
				tmp.update(self._task.getSubmitInfo(jobNum))
			tmp.update(allDict or {})
			for key, value in tmp.items():
				if not key.startswith('GC_'):
					key = 'GC_' + key
				os.environ[key] = str(value)

			script = self._task.substVars('monitoring script', script, jobNum, tmp)
			if not self._silent:
				proc = LocalProcess(*shlex.split(script))
				self._log.info(proc.get_output(timeout = self._runningMax))
			else:
				os.system(script)
		except Exception:
			self._log.exception('Error while running user script!')

	def _runInBackground(self, script, jobNum = None, jobObj = None, addDict = None):
		if script != '':
			self._tp.start_thread('Running monitoring script %s' % script,
				self._scriptThread, script, jobNum, jobObj, addDict)

	# Called on job submission
	def onJobSubmit(self, wms, jobObj, jobNum):
		self._runInBackground(self._evtSubmit, jobNum, jobObj)

	# Called on job status update
	def onJobUpdate(self, wms, jobObj, jobNum, data):
		self._runInBackground(self._evtStatus, jobNum, jobObj, {'STATUS': Job.enum2str(jobObj.state)})

	# Called on job status update
	def onJobOutput(self, wms, jobObj, jobNum, retCode):
		self._runInBackground(self._evtOutput, jobNum, jobObj, {'RETCODE': retCode})

	# Called at the end of the task
	def onTaskFinish(self, nJobs):
		self._runInBackground(self._evtFinish, addDict = {'NJOBS': nJobs})

	def onFinish(self):
		self._tp.wait_and_drop(self._runningMax)
