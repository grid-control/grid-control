import sys, os, re, fnmatch, random, utils, math, threading
from time import time, localtime, strftime
from grid_control import ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, config, module):
		self.config = config
		self.module = module
		self._dbPath = os.path.join(config.workDir, 'jobs')
		try:
			if not os.path.exists(self._dbPath):
				if config.opts.init:
					os.mkdir(self._dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % config.workDir)
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

		self.ready = []
		self.running = []
		self.done = []
		self.ok = []
		self.disabled = []

		for jobNum, jobObj in self._readJobs():
			self._jobs[jobNum] = jobObj
			self._findQueue(jobObj).append(jobNum)
		self.ready.extend(filter(lambda x: not self._jobs.has_key(x), xrange(self.nJobs)))

		for list in (self.ready, self.running, self.done, self.ok):
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
		elif jobObj.state == Job.DISABLED:
			return self.disabled
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
		return self._jobs.get(jobNum, Job())


	def _update(self, jobObj, jobNum, state):
		if jobObj.state == state:
			return

		oldState = jobObj.state
		old = self._findQueue(jobObj)
		old.remove(jobNum)

		jobObj.update(state)
		jobObj.save(os.path.join(self._dbPath, "job_%d.txt" % jobNum))

		new = self._findQueue(jobObj)
		new.append(jobNum)
		new.sort()

		jobNumLen = int(math.log10(max(1, self.nJobs)) + 1)
		utils.vprint("Job %s state changed from %s to %s" % (str(jobNum).ljust(jobNumLen), Job.states[oldState], Job.states[state]), -1, True, False)
		if (state == Job.SUBMITTED) and (jobObj.attempt > 1):
			print "(attempt #%s)" % jobObj.attempt
		elif (state == Job.FAILED) and jobObj.get('retcode') and jobObj.get('dest'):
			print "(error code: %d - %s)" % (jobObj.get('retcode'), jobObj.get('dest'))
		elif (state == Job.QUEUED) and jobObj.get('dest') != 'N/A':
			print "(%s)" % jobObj.get('dest')
		elif (state in [Job.WAITING, Job.ABORTED, Job.DISABLED]) and jobObj.get('reason'):
			print '(%s)' % jobObj.get('reason')
		elif (state == Job.SUCCESS) and jobObj.get('runtime'):
			print "(runtime %s)" % utils.strTime(jobObj.get('runtime'))
		else:
			print


	def sample(self, list, size):
		list = random.sample(list, min(size, len(list)))
		list.sort()
		return list


	def getSubmissionJobs(self, maxsample):
		submit = max(0, self.inFlight - len(self.running))
		if self.config.opts.continuous:
			submit = min(maxsample, submit)
		if self.config.opts.maxRetry != None:
			list = filter(lambda x: self._jobs.get(x, Job()).attempt < self.config.opts.maxRetry, self.ready)
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
		if (len(ids) == 0) or not self.config.opts.submission:
			return False

		if not wms.bulkSubmissionBegin(len(ids)):
			return False
		try:
			for jobNum, wmsId, data in wms.submitJobs(ids):
				try:
					jobObj = self._jobs[jobNum]
				except:
					jobObj = Job()
					self._jobs[jobNum] = jobObj

				jobObj.assignId(wmsId)
				for key, value in data.iteritems():
					jobObj.set(key, value)

				self._update(jobObj, jobNum, Job.SUBMITTED)
				self.module.onJobSubmit(jobObj, jobNum)
				if self.config.opts.abort:
					return False
			return True
		finally:
			wms.bulkSubmissionEnd()


	def wmsArgs(self, ids):
		return map(lambda jobNum: (self._jobs[jobNum].wmsId, jobNum), ids)


	def check(self, wms, maxsample = 100):
		change = False
		timeoutlist = []

		if self.config.opts.continuous:
			jobList = self.sample(self.running, maxsample)
		else:
			jobList = self.running

		# Update states of jobs
		for jobNum, wmsId, state, info in wms.checkJobs(self.wmsArgs(jobList)):
			jobObj = self._jobs[jobNum]
			if state != jobObj.state:
				change = True
				for key, value in info.items():
					jobObj.set(key, value)
				self._update(jobObj, jobNum, state)
				self.module.onJobUpdate(jobObj, jobNum, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self.timeout > 0 and time() - jobObj.submitted > self.timeout:
						timeoutlist.append(jobNum)
			if self.config.opts.abort:
				return False

		# Cancel jobs who took too long
		if len(timeoutlist):
			change = True
			print "\nTimeout for the following jobs:"
			Report(timeoutlist, self._jobs).details()
			wms.cancelJobs(self.wmsArgs(timeoutlist))
			self.mark_cancelled(timeoutlist)
			# Fixme: Error handling

		# Quit when all jobs are finished
		if len(self.ok) == self.nJobs:
			eventCmd = self.config.getPath('events', 'on finish', '')
			if eventCmd != '':
				params = "%s %d" % (eventCmd, self.nJobs)
				threading.Thread(target = os.system, args = (params,)).start()
				utils.vprint("All jobs are finished. Quitting grid-control!", -1, True, False)
			sys.exit(0)

		return change


	def retrieve(self, wms, maxsample = 10):
		change = False

		if self.config.opts.continuous:
			jobList = self.sample(self.done, maxsample)
		else:
			jobList = self.done

		retrievedJobs = False
		for jobNum, retCode, data in wms.retrieveJobs(self.wmsArgs(jobList)):
			try:
				jobObj = self._jobs[jobNum]
			except:
				continue

			if retCode == 0:
				state = Job.SUCCESS
			else:
				state = Job.FAILED

			if state != jobObj.state:
				change = True
				jobObj.set('retcode', retCode)
				jobObj.set('runtime', data.get("TIME", -1))
				self._update(jobObj, jobNum, state)
				self.module.onJobOutput(jobObj, jobNum, retCode)

			if self.config.opts.abort:
				return False

		return change


	def mark_cancelled(self, jobs):
		for jobNum in jobs:
			try:
				jobObj = self._jobs[jobNum]
			except:
				continue
			self._update(jobObj, jobNum, Job.CANCELLED)


	def delete(self, wms, selector):
		predefined = { 'TODO': 'SUBMITTED,WAITING,READY,QUEUED', 'ALL': 'SUBMITTED,WAITING,READY,QUEUED,RUNNING'}
		jobfilter = predefined.get(selector.upper(), selector.upper())

		if len(jobfilter) and jobfilter[0].isdigit():
			try:
				jobs = map(int, jobfilter.split(","))
			except:
				raise UserError("Job identifiers must be integers.")
		else:
			jobs = filter(lambda x: self._jobs[x].statefilter(jobfilter), self._jobs)

		print "\nDeleting the following jobs:"
		Report(jobs, self._jobs).details()

		if len(jobs) == 0:
			return
		if not utils.boolUserInput('Do you really want to delete these jobs?', True):
			return
		if wms.cancelJobs(self.wmsArgs(jobs)):
			self.mark_cancelled(jobs)
		else:
			print "\nThere was a problem with deleting your jobs!"
			if utils.boolUserInput('Do you want to mark them as deleted?', True):
				self.mark_cancelled(jobs)
