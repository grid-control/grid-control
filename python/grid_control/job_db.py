from __future__ import generators
import sys, os, re, fnmatch, random, utils
from time import time, localtime, strftime
from grid_control import SortedList, ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, workDir, config, opts, module):
		self._dbPath = os.path.join(workDir, 'jobs')
		try:
			if not os.path.exists(self._dbPath):
				if opts.init:
					os.mkdir(self._dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self._dbPath, e))

		nJobs = config.getInt('jobs', 'jobs', -1)
		if nJobs < 0:
			# No valid number of jobs given in config file - module has to provide number of jobs
			nJobs = module.getMaxJobs()
			if nJobs == None:
				raise ConfigError("Module doesn't provide max number of Jobs!")
		else:
			# Module doesn't have to provide number of jobs
			try:
				maxJobs = module.getMaxJobs()
				if maxJobs and (nJobs > maxJobs):
					print "Maximum number of jobs given as %d was truncated to %d" % (nJobs, maxJobs)
					nJobs = maxJobs
			except:
				pass

		self.all = SortedList()
		self.ready = SortedList()
		self.running = SortedList()
		self.done = SortedList()
		self.ok = SortedList()
		self.disabled = SortedList()

		for jobNum, jobObj in self._scan():
			self._jobs[jobNum] = jobObj
			self.all.add(jobNum)
			self._findQueue(jobObj).append(jobNum)
		self.ready.extend(filter(lambda x: not (x in self.all), xrange(nJobs)))

		self.timeout = utils.parseTime(config.get('jobs', 'queue timeout', ''))
		self.inFlight = config.getInt('jobs', 'in flight')
		self.doShuffle = config.getBool('jobs', 'shuffle', False)
		self.module = module
		self.opts = opts


	def _findQueue(self, job):
		state = job.state

		if state in (Job.SUBMITTED, Job.WAITING, Job.READY,
		             Job.QUEUED, Job.RUNNING):
			queue = self.running
		elif state in (Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED):
			queue = self.ready	# resubmit?
		elif state == Job.DONE:
			queue = self.done
		elif state == Job.SUCCESS:
			queue = self.ok
		else:
			raise Exception("Internal error: Unexpected job state %s" % Job.states[state])

		return queue


	def _scan(self):
		regexfilter = re.compile(r'^job_([0-9]+)\.txt$')
		self._jobs = {}
		for jobFile in fnmatch.filter(os.listdir(self._dbPath), 'job_*.txt'):
			match = regexfilter.match(jobFile)
			try:
				jobNum = int(match.group(1))
			except:
				continue

			fp = open(os.path.join(self._dbPath, jobFile))
			yield (jobNum, Job.load(fp))
			fp.close()


	def list(self, types = None):
		for id, job in self._jobs.items():
			if types == None or job.state in types:
				yield id


	def get(self, id):
		return self._jobs[id]


	def _saveJob(self, id):
		job = self._jobs[id]
		fp = open(os.path.join(self._dbPath, "job_%d.txt" % id), 'w')
		job.save(fp)
		fp.truncate()
		fp.close()
		# FIXME: Error handling?


	def _update(self, jobNum, job, state):
		if job.state == state:
			return

		old = self._findQueue(job)
		job.update(state)
		new = self._findQueue(job)

		if old != new:
			old.remove(jobNum)
			new.add(jobNum)

		utils.vprint("Job %d state changed to %s" % (jobNum, Job.states[state]), -1, True, False)
		if (state == Job.SUBMITTED) and (job.attempt > 1):
			print "(attempt #%s)" % job.attempt
		elif (state == Job.FAILED) and job.get('retcode') and job.get('dest'):
			print "(error code: %d - %s)" % (job.get('retcode'), job.get('dest'))
		elif (state == Job.QUEUED) and job.get('dest'):
			print "(%s)" % job.get('dest')
		elif (state == Job.WAITING) and job.get('reason'):
			print '(%s)' % job.get('reason')
		elif (state == Job.SUCCESS) and job.get('runtime'):
			print "(runtime %s)" % utils.strTime(job.get('runtime'))
		else:
			print

		self._saveJob(jobNum)


	def getSubmissionJobs(self):
		submit = max(0, self.inFlight - len(self.running))
		if self.opts.continuous:
			submit = min(100, submit)
		if self.opts.maxRetry != None:
			list = filter(lambda x: self._jobs.get(x, Job()).attempt < self.opts.maxRetry, self.ready)
		else:
			list = self.ready[:]
		if self.doShuffle:
			return SortedList(random.sample(list, submit))
		return SortedList(list[:submit])


	def submit(self, wms):
		ids = self.getSubmissionJobs()
		if (len(ids) == 0) or not self.opts.submission:
			return False

		wms.bulkSubmissionBegin(len(ids))
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
				self.bulkSubmissionEnd()
				return False

		wms.bulkSubmissionEnd()
		return True


	def getWmsMap(self, idlist):
		map = {}
		for id in idlist:
			job = self._jobs[id]
			map[job.id] = (id, job)
		return map


	def check(self, wms):
		change = False
		timeoutlist = []

		# TODO: just check running?
		if self.opts.continuous:
			wmsMap = self.getWmsMap(random.sample(self.running, min(100, len(self.running))))
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
				return change

		# Cancel jobs who took too long
		if len(timeoutlist):
			change = True
			print "\nTimeout for the following jobs:"
			Report(timeoutlist, self._jobs).details()
			wms.cancelJobs(map(lambda jobNum: self._jobs[jobNum].id, timeoutlist))
			self.mark_cancelled(timeoutlist)
			# Fixme: Error handling

		# Quit when all jobs are finished
		if (len(self.ready) == 0) and (len(self.running) == 0) and (len(self.done) == 0):
			print "%s - All jobs are finished. Quitting grid-control!" % strftime("%Y-%m-%d %H:%M:%S", localtime())
			sys.exit(0)

		return change


	def retrieve(self, wms):
		change = False
		wmsIds = map(lambda jobNum: self._jobs[jobNum].id, self.done)

		for id, retCode, data in wms.retrieveJobs(wmsIds):
			try:
				job = self._jobs[id]
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
				self._update(id, job, state)
				self.module.onJobOutput(job, id, retCode)

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
