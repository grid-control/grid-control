import sys, os, re, fnmatch, random, utils, math, threading
from time import time, localtime, strftime
from grid_control import ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, config, module, monitor):
		self.config = config
		self.monitor = monitor
		self.errorDict = module.errorDict
		self._dbPath = os.path.join(config.workDir, 'jobs')
		try:
			if not os.path.exists(self._dbPath):
				if config.opts.init:
					os.mkdir(self._dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % config.workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self._dbPath, e))

		self.nJobs = config.getInt('jobs', 'jobs', -1, volatile=True)
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
		self.queued = []
		self.done = []
		self.ok = []
		self.disabled = []

		for jobNum, jobObj in self._readJobs():
			if len(self._jobs) >= self.nJobs:
				print "Stopped reading job infos! The number of job infos in the work directory",
				print "is larger than the maximum number of jobs (%d)" % self.nJobs
				break
			self._jobs[jobNum] = jobObj
			self._findQueue(jobObj).append(jobNum)
		if len(self._jobs) < self.nJobs:
			self.ready.extend(filter(lambda x: x not in self._jobs, range(self.nJobs)))

		for list in (self.ready, self.queued, self.running, self.done, self.ok):
			list.sort()

		self.timeout = utils.parseTime(config.get('jobs', 'queue timeout', '', volatile=True))
		self.inFlight = config.getInt('jobs', 'in flight', -1, volatile=True)
		self.inQueue = config.getInt('jobs', 'in queue', -1, volatile=True)
		self.doShuffle = config.getBool('jobs', 'shuffle', False, volatile=True)
		self.maxRetry = config.getInt('jobs', 'max retry', -1, volatile=True)
		if self.config.opts.maxRetry != None:
			self.maxRetry = self.config.opts.maxRetry


	# Return appropriate queue for given job
	def _findQueue(self, jobObj):
		if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
			return self.queued
		elif jobObj.state == Job.RUNNING:
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
		elif (state == Job.QUEUED) and jobObj.get('dest') != 'N/A':
			print "(%s)" % jobObj.get('dest')
		elif (state in [Job.WAITING, Job.ABORTED, Job.DISABLED]) and jobObj.get('reason'):
			print '(%s)' % jobObj.get('reason')
		elif (state == Job.SUCCESS) and jobObj.get('runtime'):
			print "(runtime %s)" % utils.strTime(jobObj.get('runtime'))
		elif (state == Job.FAILED):
			msg = []
			if jobObj.get('retcode'):
				msg.append("error code: %d" % jobObj.get('retcode'))
				try:
					if utils.verbosity() > 0:
						msg.append(self.errorDict[jobObj.get('retcode')])
				except:
					pass
			if jobObj.get('dest'):
				msg.append(jobObj.get('dest'))
			if len(msg):
				print "(%s)" % str.join(" - ", msg),
			print
		else:
			print


	def sample(self, list, size):
		list = random.sample(list, min(size, len(list)))
		list.sort()
		return list


	def getSubmissionJobs(self, maxsample):
		# Determine number of jobs to submit
		submit = self.nJobs
		nQueued = len(self.queued)
		if self.inQueue > 0:
			submit = min(submit, self.inQueue - nQueued)
		if self.inFlight > 0:
			submit = min(submit, self.inFlight - nQueued - len(self.running))
		if self.config.opts.continuous:
			submit = min(submit, maxsample)
		submit = max(submit, 0)

		# Get list of submittable jobs
		if self.maxRetry >= 0:
			list = filter(lambda x: self._jobs.get(x, Job()).attempt < self.maxRetry, self.ready)
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

				if wmsId == None:
					# Could not register at WMS
					self._update(jobObj, jobNum, Job.FAILED)
					continue

				jobObj.assignId(wmsId)
				for key, value in data.iteritems():
					jobObj.set(key, value)

				self._update(jobObj, jobNum, Job.SUBMITTED)
				self.monitor.onJobSubmit(wms, jobObj, jobNum)
				if self.config.opts.abort:
					return False
			return True
		finally:
			wms.bulkSubmissionEnd()


	def wmsArgs(self, ids):
		return map(lambda jobNum: (self._jobs[jobNum].wmsId, jobNum), ids)


	def check(self, wms, maxsample = 100):
		change = False
		timeoutList = []

		if self.config.opts.continuous:
			jobList = self.sample(self.running + self.queued, maxsample)
		else:
			jobList = self.running + self.queued

		# Update states of jobs
		for jobNum, wmsId, state, info in wms.checkJobs(self.wmsArgs(jobList)):
			jobObj = self._jobs[jobNum]
			if state != jobObj.state:
				change = True
				for key, value in info.items():
					jobObj.set(key, value)
				self._update(jobObj, jobNum, state)
				self.monitor.onJobUpdate(wms, jobObj, jobNum, info)
			else:
				# If a job stays too long in an inital state, cancel it
				if jobObj.state in (Job.SUBMITTED, Job.WAITING, Job.READY, Job.QUEUED):
					if self.timeout > 0 and time() - jobObj.submitted > self.timeout:
						timeoutList.append(jobNum)
			if self.config.opts.abort:
				return False

		# Cancel jobs who took too long
		if len(timeoutList):
			change = True
			print "\nTimeout for the following jobs:"
			self.cancel(wms, timeoutList)

		# Quit when all jobs are finished
		if len(self.ok) == self.nJobs:
			self.monitor.onTaskFinish(self.nJobs)
			utils.vprint("Task successfully completed. Quitting grid-control!", -1, True, False)
			sys.exit(0)

		return change


	def retrieve(self, wms, maxsample = 10):
		change = False

		if self.config.opts.continuous:
			jobList = self.sample(self.done, maxsample)
		else:
			jobList = self.done

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
				self.monitor.onJobOutput(wms, jobObj, jobNum, retCode)

			if self.config.opts.abort:
				return False

		return change


	def cancel(self, wms, jobs, interactive = False):
		if len(jobs) == 0:
			return
		Report(jobs, self._jobs).details()
		if interactive and not utils.boolUserInput('Do you really want to delete these jobs?', True):
			return

		def mark_cancelled(jobNum):
			try:
				jobObj = self._jobs[jobNum]
			except:
				return
			self._update(jobObj, jobNum, Job.CANCELLED)

		for (wmsId, jobNum) in wms.cancelJobs(self.wmsArgs(jobs)):
			# Remove deleted job from todo list and mark as cancelled
			jobs.remove(jobNum)
			mark_cancelled(jobNum)

		if len(jobs) > 0:
			print "\nThere was a problem with deleting the following jobs:"
			Report(jobs, self._jobs).details()
			if interactive and utils.boolUserInput('Do you want to mark them as deleted?', True):
				map(mark_cancelled, jobs)


	def delete(self, wms, selector):
		predefined = { 'TODO': 'SUBMITTED,WAITING,READY,QUEUED', 'ALL': 'SUBMITTED,WAITING,READY,QUEUED,RUNNING'}
		jobFilter = predefined.get(selector.upper(), selector.upper())

		if len(jobFilter) and jobFilter[0].isdigit():
			try:
				jobs = map(int, jobFilter.split(","))
			except:
				raise UserError("Job identifiers must be integers.")
		else:
			def stateFilter(jobObj):
				for state in jobFilter.split(','):
					regex = re.compile('^%s.*' % state)
					for key in filter(regex.match, Job.states):
						if key == Job.states[jobObj.state]:
							return True
				return False
			def siteFilter(jobObj):
				dest = jobObj.get("dest").upper()
				if not dest:
					return False
				dest = str.join("/", map(lambda x: x.split(":")[0], dest.split("/")))
				for site in jobFilter.split(','):
					regex = re.compile(site)
					if regex.search(dest) and jobObj.state not in (Job.SUCCESS, Job.FAILED):
						return True
				return False
			# First try matching states, then try to match destinations
			jobs = filter(lambda x: stateFilter(self._jobs[x]), self._jobs.keys())
			if jobs == []:
				jobs = filter(lambda x: siteFilter(self._jobs[x]), self._jobs.keys())

		print "\nDeleting the following jobs:"
		self.cancel(wms, jobs, True)
