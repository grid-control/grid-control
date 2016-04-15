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

import os, time, fnmatch, logging, operator
from grid_control import utils
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.file_objects import SafeFile
from hpfwk import NestedException
from python_compat import imap, irange, reduce, sorted

class JobError(NestedException):
	pass

class Job(object):
	__internals = ('wmsId', 'status')

	def __init__(self):
		self.state = Job.INIT
		self.nextstate = None
		self.attempt = 0
		self.history = {}
		self.wmsId = None
		self.submitted = 0
		self.changed = 0
		self.dict = {}


	def loadData(cls, name, data):
		try:
			job = Job()
			job.state = Job.str2enum(data.get('status'), Job.FAILED)

			if 'id' in data:
				if not data['id'].startswith('WMSID'): # Legacy support
					data['legacy'] = data['id']
					if data['id'].startswith('https'):
						data['id'] = 'WMSID.GLITEWMS.%s' % data['id']
					else:
						wmsId, backend = tuple(data['id'].split('.', 1))
						data['id'] = 'WMSID.%s.%s' % (backend, wmsId)
				job.wmsId = data['id']
			for key in ['attempt', 'submitted', 'changed']:
				if key in data:
					setattr(job, key, data[key])
			if 'runtime' not in data:
				if 'submitted' in data:
					data['runtime'] = time.time() - float(job.submitted)
				else:
					data['runtime'] = 0
			for key in irange(1, job.attempt + 1):
				if ('history_' + str(key)).strip() in data:
					job.history[key] = data['history_' + str(key)]

			for i in cls.__internals:
				try:
					del data[i]
				except Exception:
					pass
			job.dict = data
		except Exception:
			raise JobError('Unable to parse data in %s:\n%r' % (name, data))
		return job
	loadData = classmethod(loadData)


	def load(cls, name):
		try:
			data = utils.DictFormat(escapeString = True).parse(open(name))
		except Exception:
			raise JobError('Invalid format in %s' % name)
		return Job.loadData(name, data)
	load = classmethod(load)


	def getAll(self):
		data = dict(self.dict)
		data['status'] = Job.enum2str(self.state)
		data['attempt'] = self.attempt
		data['submitted'] = self.submitted
		data['changed'] = self.changed
		for key, value in self.history.items():
			data['history_' + str(key)] = value
		if self.wmsId is not None:
			data['id'] = self.wmsId
			if self.dict.get('legacy', None): # Legacy support
				data['id'] = self.dict.pop('legacy')
		return data


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
		'RUNNING', 'CANCELLED', 'DONE', 'FAILED', 'SUCCESS'], Job, useHash = False)


class JobClass(object):
	mkJobClass = lambda *fList: (reduce(operator.add, imap(lambda f: 1 << f, fList)), fList)
	ATWMS = mkJobClass(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED)
	RUNNING = mkJobClass(Job.RUNNING)
	PROCESSING = mkJobClass(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)
	READY = mkJobClass(Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED)
	DONE = mkJobClass(Job.DONE)
	SUCCESS = mkJobClass(Job.SUCCESS)
	DISABLED = mkJobClass(Job.DISABLED)
	ENDSTATE = mkJobClass(Job.SUCCESS, Job.DISABLED)
	PROCESSED = mkJobClass(Job.SUCCESS, Job.FAILED, Job.CANCELLED, Job.ABORTED)


class JobDB(ConfigurablePlugin):
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('jobs')
		self._dbPath = config.getWorkPath('jobs')
		self._jobMap = self.readJobs(jobLimit)
		if jobLimit < 0 and len(self._jobMap) > 0:
			jobLimit = max(self._jobMap) + 1
		(self.jobLimit, self.alwaysSelector) = (jobLimit, jobSelector)

	def getWorkPath(self): # TODO: only used by report class
		return os.path.abspath(os.path.join(self._dbPath, '..'))

	def readJobs(self, jobLimit):
		try:
			if not os.path.exists(self._dbPath):
				os.mkdir(self._dbPath)
		except Exception:
			raise JobError("Problem creating work directory '%s'" % self._dbPath)

		candidates = []
		for jobFile in fnmatch.filter(os.listdir(self._dbPath), 'job_*.txt'):
			try: # 2xsplit is faster than regex
				jobNum = int(jobFile.split(".")[0].split("_")[1])
			except Exception:
				continue
			candidates.append((jobNum, jobFile))

		(jobMap, maxJobs) = ({}, len(candidates))
		activity = utils.ActivityLog('Reading job infos ...')
		idx = 0
		for (jobNum, jobFile) in sorted(candidates):
			idx += 1
			if (jobLimit >= 0) and (jobNum >= jobLimit):
				self._log.info('Stopped reading job infos at job #%d out of %d available job files', jobNum, len(candidates))
				break
			jobObj = Job.load(os.path.join(self._dbPath, jobFile))
			jobMap[jobNum] = jobObj
			if idx % 100 == 0:
				activity.finish()
				activity = utils.ActivityLog('Reading job infos ... %d [%d%%]' % (idx, (100.0 * idx) / maxJobs))
		activity.finish()
		return jobMap


	def get(self, jobNum, default = None, create = False):
		if create:
			self._jobMap[jobNum] = self._jobMap.get(jobNum, Job())
		return self._jobMap.get(jobNum, default)


	def getJobsIter(self, jobSelector = None, subset = None):
		if subset is None:
			subset = irange(self.jobLimit)
		if jobSelector and self.alwaysSelector:
			select = lambda *args: jobSelector(*args) and self.alwaysSelector(*args)
		elif jobSelector or self.alwaysSelector:
			select = jobSelector or self.alwaysSelector
		else:
			for jobNum in subset:
				yield jobNum
			raise StopIteration
		for jobNum in subset:
			if select(jobNum, self.get(jobNum, Job())):
				yield jobNum


	def getJobs(self, jobSelector = None, subset = None):
		return list(self.getJobsIter(jobSelector, subset))


	def getJobsN(self, jobSelector = None, subset = None):
		return len(self.getJobs(jobSelector, subset)) # fastest method! (iter->list written in C)


	def commit(self, jobNum, jobObj):
		fp = SafeFile(os.path.join(self._dbPath, 'job_%d.txt' % jobNum), 'w')
		fp.writelines(utils.DictFormat(escapeString = True).format(jobObj.getAll()))
		fp.close()


	def __len__(self):
		return self.jobLimit
