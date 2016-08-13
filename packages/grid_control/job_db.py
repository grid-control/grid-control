# | Copyright 2007-2016 Karlsruhe Institute of Technology
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
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, NestedException
from python_compat import irange

class JobError(NestedException):
	pass

class Job(object):
	def __init__(self):
		self.state = Job.INIT
		self.attempt = 0
		self.history = {}
		self.wmsId = None
		self.submitted = 0
		self.changed = 0
		self.dict = {}


	def get_dict(self):
		return {'id': self.wmsId, 'status': Job.enum2str(self.state),
			'attempt': self.attempt, 'submitted': self.submitted, 'changed': self.changed}


	def set(self, key, value):
		self.dict[key] = value


	def get(self, key, default = None):
		return self.dict.get(key, default)


	def update(self, state):
		self.state = state
		self.changed = time.time()
		self.history[self.attempt] = self.dict.get('dest', 'N/A')


	def assignId(self, wmsId):
		self.dict['legacy'] = None # Legacy support
		self.wmsId = wmsId
		self.attempt = self.attempt + 1
		self.submitted = time.time()

makeEnum(['INIT', 'SUBMITTED', 'DISABLED', 'READY', 'WAITING', 'QUEUED', 'ABORTED',
		'RUNNING', 'CANCEL', 'UNKNOWN', 'CANCELLED', 'DONE', 'FAILED', 'SUCCESS'], Job)


class JobClassHolder(object):
	def __init__(self, *states):
		self.states = []
		for state in states:
			if isinstance(state, JobClassHolder):
				self.states.extend(state.states)
			else:
				self.states.append(state)


class JobClass(object):
	ATWMS = JobClassHolder(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.UNKNOWN)
	PROCESSING = JobClassHolder(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.UNKNOWN, Job.RUNNING)
	CANCEL = JobClassHolder(Job.CANCEL)
	DONE = JobClassHolder(Job.DONE)
	DISABLED = JobClassHolder(Job.DISABLED)
	ENDSTATE = JobClassHolder(Job.SUCCESS, Job.DISABLED)
	PROCESSED = JobClassHolder(Job.SUCCESS, Job.FAILED, Job.CANCELLED, Job.ABORTED)
	SUBMIT_CANDIDATES = JobClassHolder(Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED)
	SUCCESS = JobClassHolder(Job.SUCCESS)


class JobDB(ConfigurablePlugin):
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('jobs')
		(self._jobLimit, self._alwaysSelector) = (jobLimit, jobSelector)
		(self._defaultJob, self._workPath) = (Job(), config.getWorkPath())

	def setJobLimit(self, value):
		self._jobLimit = value

	def __len__(self):
		return self._jobLimit

	def getWorkPath(self): # TODO: only used by report class
		return self._workPath

	def getJobsIter(self, jobSelector = None, subset = None):
		if subset is None:
			subset = irange(self._jobLimit)
		if jobSelector and self._alwaysSelector:
			select = lambda *args: jobSelector(*args) and self._alwaysSelector(*args)
		elif jobSelector or self._alwaysSelector:
			select = jobSelector or self._alwaysSelector
		else:
			for jobNum in subset:
				yield jobNum
			raise StopIteration
		for jobNum in subset:
			if select(jobNum, self.getJobTransient(jobNum)):
				yield jobNum

	def getJobs(self, jobSelector = None, subset = None):
		return list(self.getJobsIter(jobSelector, subset))

	def getJobsN(self, jobSelector = None, subset = None):
		return len(self.getJobs(jobSelector, subset)) # fastest method! (iter->list written in C)

	def getJob(self, jobNum):
		raise AbstractError

	def getJobTransient(self, jobNum):
		raise AbstractError

	def getJobPersistent(self, jobNum):
		raise AbstractError

	def commit(self, jobNum, jobObj):
		raise AbstractError
