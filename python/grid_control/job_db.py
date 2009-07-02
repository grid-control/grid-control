import sys, os, re, fnmatch, random, utils, math
from time import time, localtime, strftime
from grid_control import ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, config, opts, module):
		self.opts = opts
		self.module = module
		self._dbPath = os.path.join(opts.workDir, 'jobs')
		try:
			if not os.path.exists(self._dbPath):
				if opts.init:
					os.mkdir(self._dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % opts.workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self._dbPath, e))

		self.nJobs = config.getInt('jobs', 'jobs', -1)
		if self.nJobs < 0:
			# No valid number of jobs given in config file - module has to provide number of jobs
			self.nJobs = module.getMaxJobs()
			if self.nJobs == None:
				raise ConfigError("Module doesn't provide max number of Jobs!")
		else:
			# Module doesn't have to provide number of jobs
			try:
				maxJobs = module.getMaxJobs()
				if maxJobs and (self.nJobs > maxJobs):
					print "Maximum number of jobs given as %d was truncated to %d" % (self.nJobs, maxJobs)
					self.nJobs = maxJobs
			except:
				pass

		self.all = []
		self.ready = []
		self.running = []
		self.done = []
		self.ok = []

		for jobNum, jobObj in self._readJobs():
			self._jobs[jobNum] = jobObj
			self.all.append(jobNum)
			self._findQueue(jobObj).append(jobNum)
		self.ready.extend(filter(lambda x: not self._jobs.has_key(x), xrange(self.nJobs)))

		for list in (self.all, self.ready, self.running, self.done, self.ok):
			list.sort()

		self.timeout = utils.parseTime(config.get('jobs', 'queue timeout', ''))
		self.inFlight = config.getInt('jobs', 'in flight', self.nJobs)
		self.doShuffle = config.getBool('jobs', 'shuffle', False)


	# Return appropriate queue for given job
	def _findQueue(self, jobObj):
		if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED, Job.RUNNING):
			return self.running
		elif jobObj.state in (Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED):
			return self.ready	# resubmit?
		elif jobObj.state == Job.DONE:
			return self.done
		elif jobObj.state == Job.SUCCESS:
			return self.ok
		raise Exception("Internal error: Unexpected job state %s" % Job.states[jobObj.state])


	def _readJobs(self):
		regexfilter = re.compile(r'^job_([0-9]+)\.txt$')
		self._jobs = {}
		for jobFile in fnmatch.filter(os.listdir(self._dbPath), 'job_*.txt'):
			match = regexfilter.match(jobFile)
			try:
				jobNum = int(match.group(1))
			except:
				continue
			yield (jobNum, Job.load(os.path.join(self._dbPath, jobFile)))


	def get(self, jobNum):
		return self._jobs[jobNum]


	def _update(self, jobNum, job, state):
		if job.state == state:
			return

		oldState = job.state
		old = self._findQueue(job)
		old.remove(jobNum)

		job.update(state)
		job.save(os.path.join(self._dbPath, "job_%d.txt" % jobNum))

		new = self._findQueue(job)
		new.append(jobNum)
		new.sort()

		jobNumLen = int(math.log10(max(1, self.nJobs)) + 1)
		utils.vprint("Job %s state changed from %s to %s" % (str(jobNum).ljust(jobNumLen), Job.states[oldState], Job.states[state]), -1, True, False)
		if (state == Job.SUBMITTED) and (job.attempt > 1):
			print "(attempt #%s)" % job.attempt
		elif (state == Job.FAILED) and job.get('retcode') and job.get('dest'):
			print "(error code: %d - %s)" % (job.get('retcode'), job.get('dest'))
		elif (state == Job.QUEUED) and job.get('dest'):
			print "(%s)" % job.get('dest')
		elif (state in [Job.WAITING, Job.ABORTED]) and job.get('reason'):
			print '(%s)' % job.get('reason')
		elif (state == Job.SUCCESS) and job.get('runtime'):
			print "(runtime %s)" % utils.strTime(job.get('runtime'))
		else:
			print


	def getSubmissionJobs(self, maxsample):
		submit = max(0, self.inFlight - len(self.running))
		if self.opts.continuous:
			submit = min(maxsample, submit)
		if self.opts.maxRetry != None:
			list = filter(lambda x: self._jobs.get(x, Job()).attempt < self.opts.maxRetry, self.ready)
		else:
			list = self.ready[:]
		if self.doShuffle:
			list = self.sample(list, submit)
		else:
			list = list[:submit]
		list.sort()
		return list


	def submit(self, wms, maxsample = 100):
		ids = self.getSubmissionJobs(maxsample)
		if (len(ids) == 0) or not self.opts.submission:
			return False

		if not wms.bulkSubmissionBegin(len(ids)):
			return False
		try:
			for jobNum, wmsId, data in wms.submitJobs(ids):
				try:
					job = self._jobs[jobNum]
				except:
					job = Job()
					self._jobs[jobNum] = job

				job.assignId(wmsId)
				for key, value in data.iteritems():
					job.set(key, value)

				self._update(jobNum, job, Job.SUBMITTED)
				self.module.onJobSubmit(job, jobNum)
				if self.opts.abort:
					return False
			return True
		finally:
			wms.bulkSubmissionEnd()


	def getWmsMap(self, idlist):
		map = {}
		for id in idlist:
			job = self._jobs[id]
			map[job.id] = (id, job)
		return map


	def check(self, wms, maxsample = 100):
		change = False
		timeoutlist = []

		if self.opts.continuous:
			wmsMap = self.getWmsMap(self.sample(self.running, maxsample))
		else:
			wmsMap = self.getWmsMap(self.running)

		# Update states of jobs
		for wmsId, state, info in wms.checkJobs(wmsMap.keys()):
			id, job = wmsMap[wmsId]
			if state != job.state:
				change = True
				for key, value in info.items():
					job.set(key, value)
				self._update(id, job, state)
				self.module.onJobUpdate(job, id, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if job.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self.timeout > 0 and time() - job.submitted > self.timeout:
						timeoutlist.append(id)
			if self.opts.abort:
				return False

		# Cancel jobs who took too long
		if len(timeoutlist):
			change = True
			print "\nTimeout for the following jobs:"
			Report(timeoutlist, self._jobs).details()
			wms.cancelJobs(map(lambda jobNum: self._jobs[jobNum].id, timeoutlist))
			self.mark_cancelled(timeoutlist)
			# Fixme: Error handling

		# Quit when all jobs are finished
		if len(self.ok) == self.nJobs:
			utils.vprint("All jobs are finished. Quitting grid-control!", -1, True, False)
			sys.exit(0)

		return change


	def sample(self, list, size):
		list = random.sample(list, min(size, len(list)))
		list.sort()
		return list


	def retrieve(self, wms, maxsample = 10):
		change = False

		if self.opts.continuous:
			wmsMap = self.getWmsMap(self.sample(self.done, maxsample))
		else:
			wmsMap = self.getWmsMap(self.done)

		retrievedJobs = False
		for jobNum, retCode, data in wms.retrieveJobs(wmsMap.keys()):
			try:
				job = self._jobs[jobNum]
			except:
				continue

			if retCode == 0:
				state = Job.SUCCESS
			else:
				state = Job.FAILED

			if state != job.state:
				change = True
				job.set('retcode', retCode)
				job.set('runtime', data.get("TIME", -1))
				self._update(jobNum, job, state)
				self.module.onJobOutput(job, jobNum, retCode)

			if self.opts.abort:
				return False

		return change


	def mark_cancelled(self, jobs):
		for jobNum in jobs:
			try:
				jobObj = self._jobs[jobNum]
			except:
				continue
			self._update(jobNum, jobObj, Job.CANCELLED)


	def delete(self, wms, opts):
		predefined = { 'TODO': 'SUBMITTED,WAITING,READY,QUEUED', 'ALL': 'SUBMITTED,WAITING,READY,QUEUED,RUNNING'}
		jobfilter = predefined.get(opts.delete.upper(), opts.delete.upper())

		if len(jobfilter) and jobfilter[0].isdigit():
			try:
				jobs = map(int, jobfilter.split(","))
			except:
				raise UserError("Job identifiers must be integers.")
		else:
			jobs = filter(lambda x: self._jobs[x].statefilter(jobfilter), self._jobs)

		print "\nDeleting the following jobs:"
		Report(jobs, self._jobs).details()

		if not len(jobs) == 0:
			if utils.boolUserInput('Do you really want to delete these jobs?', True):
				wmsIds = map(lambda jobNum: self._jobs[jobNum].id, jobs)
				if wms.cancelJobs(wmsIds):
					self.mark_cancelled(jobs)
				else:
					print "\nThere was a problem with deleting your jobs!"
					if utils.boolUserInput('Do you want to mark them as deleted?', True):
						self.mark_cancelled(jobs)
