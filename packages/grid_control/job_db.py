import sys, os, time, fnmatch, operator, utils
from exceptions import ConfigError, RuntimeError, RethrowError
from utils import QM

class Job:
	states = ('INIT', 'SUBMITTED', 'DISABLED', 'READY', 'WAITING', 'QUEUED', 'ABORTED',
		'RUNNING', 'CANCELLED', 'DONE', 'FAILED', 'SUCCESS')
	_stateDict = {}
	for idx, state in enumerate(states):
		_stateDict[state] = idx
		locals()[state] = idx
	__internals = ('wmsId', 'status')


	def __init__(self, state = INIT):
		self.state = state
		self.nextstate = None
		self.attempt = 0
		self.history = {}
		self.wmsId = None
		self.submitted = 0
		self.changed = 0
		self.dict = {}


	def load(cls, name):
		try:
			data = utils.DictFormat(escapeString = True).parse(open(name))
		except:
			raise RuntimeError('Invalid format in %s' % name)
		try:
			job = Job(cls._stateDict[data.get('status', 'FAILED')])

			if 'id' in data:
				if not data['id'].startswith('WMSID'): # Legacy support
					data['legacy'] = data['id']
					if data['id'].startswith('https'):
						data['id'] = 'WMSID.GLITEWMS.%s' % data['id']
					else:
						wmsId, backend = tuple(data['id'].split('.', 1))
						data['id'] = 'WMSID.%s.%s' % (backend, wmsId)
				job.wmsId = data['id']
			if 'attempt' in data:
				job.attempt = data['attempt']
			if 'submitted' in data:
				job.submitted = data['submitted']
			if 'runtime' not in data:
				if 'submitted' in data:
					data['runtime'] = time.time() - float(job.submitted)
				else:
					data['runtime'] = 0
			if 'changed' in data:
				job.changed = data['changed']

			for key in range(1, job.attempt + 1):
				if ('history_' + str(key)).strip() in data:
					job.history[key] = data['history_' + str(key)]

			for i in cls.__internals:
				try:
					del data[i]
				except:
					pass
			job.dict = data
		except:
			raise RuntimeError('Unable to parse data in %s:\n%r' % (name, data))
		return job
	load = classmethod(load)


	def getAll(self):
		data = self.dict
		data['status'] = self.states[self.state]
		data['attempt'] = self.attempt
		data['submitted'] = self.submitted
		data['changed'] = self.changed
		for key, value in self.history.items():
			data['history_' + str(key)] = value
		if self.wmsId != None:
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


class JobClass:
	mkJobClass = lambda *fList: (reduce(operator.add, map(lambda f: 1 << f, fList)), fList)
	ATWMS = mkJobClass(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED)
	RUNNING = mkJobClass(Job.RUNNING)
	PROCESSING = mkJobClass(Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING)
	READY = mkJobClass(Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED)
	DONE = mkJobClass(Job.DONE)
	SUCCESS = mkJobClass(Job.SUCCESS)
	DISABLED = mkJobClass(Job.DISABLED)
	ENDSTATE = mkJobClass(Job.SUCCESS, Job.DISABLED)


class JobDB:
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		(self.jobLimit, self.alwaysSelector) = (jobLimit, jobSelector)
		self.dbPath = os.path.join(config.workDir, 'jobs')
		try:
			if not os.path.exists(self.dbPath):
				if config.opts.init:
					os.mkdir(self.dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % config.workDir)
		except IOError:
			raise RethrowError("Problem creating work directory '%s'" % self.dbPath)

		candidates = fnmatch.filter(os.listdir(self.dbPath), 'job_*.txt')
		(self._jobs, log, maxJobs) = ({}, None, len(candidates))
		for idx, jobFile in enumerate(candidates):
			if (jobLimit >= 0) and (len(self._jobs) >= jobLimit):
				utils.eprint('Stopped reading job infos! The number of job infos in the work directory (%d)' % len(self._jobs), newline = False)
				utils.eprint('is larger than the maximum number of jobs (%d)' % jobLimit)
				break
			try: # 2xsplit is faster than regex
				jobNum = int(jobFile.split(".")[0].split("_")[1])
			except:
				continue
			jobObj = Job.load(os.path.join(self.dbPath, jobFile))
			self._jobs[jobNum] = jobObj
			if idx % 100 == 0:
				del log
				log = utils.ActivityLog('Reading job infos ... %d [%d%%]' % (idx, (100.0 * idx) / maxJobs))
		if jobLimit < 0 and len(self._jobs) > 0:
			self.jobLimit = max(self._jobs) + 1


	def get(self, jobNum, default = None, create = False):
		if create:
			self._jobs[jobNum] = self._jobs.get(jobNum, Job())
		return self._jobs.get(jobNum, default)


	def getJobsIter(self, jobSelector = None, subset = None):
		if subset == None:
			subset = range(self.jobLimit)
		if jobSelector and self.alwaysSelector:
			select = lambda *args: jobSelector(*args) and self.alwaysSelector(*args)
		elif jobSelector or self.alwaysSelector:
			select = QM(jobSelector, jobSelector, self.alwaysSelector)
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
		counter = 0
		for jobNum in self.getJobsIter(jobSelector, subset):
			counter += 1
		return counter


	def commit(self, jobNum, jobObj):
		fp = open(os.path.join(self.dbPath, 'job_%d.txt' % jobNum), 'w')
		utils.safeWrite(fp, utils.DictFormat(escapeString = True).format(jobObj.getAll()))
#		if jobObj.state == Job.DISABLED:


	def __len__(self):
		return self.jobLimit
