from __future__ import generators
import os, re, fnmatch, random, utils
from time import time, localtime, strftime
from grid_control import SortedList, ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, workDir, nJobs, timeout, module, init = False):
		self._dbPath = os.path.join(workDir, 'jobs')
		try:
			if not os.path.exists(self._dbPath):
				if init:
					os.mkdir(self._dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self._dbPath, e))

		self.all = SortedList()
		for id, job in self._scan():
			self._jobs[id] = job
			self.all.add(id)

		self.ready = SortedList()
		self.running = SortedList()
		self.done = SortedList()
		self.ok = SortedList()

		if nJobs < 0:
			nJobs = module.getMaxJobs()
		else:
			try:
				maxJobs = module.getMaxJobs()
				if nJobs > maxJobs:
					print "Maximum number of jobs given as %d was truncated to %d" % (nJobs, maxJobs)
					nJobs = maxJobs
			except:
				pass

		if nJobs == None:
			raise

		i = 0
		for j in self.all:
			self.ready.extend(xrange(i, min(j, nJobs)))
			i = j + 1
			queue = self._findQueue(self._jobs[j])
			queue.append(j)
		self.ready.extend(xrange(i, nJobs))
		self.timeout = timeout
		self.module = module


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
		for job in fnmatch.filter(os.listdir(self._dbPath), 'job_*.txt'):
			match = regexfilter.match(job)
			try:
				id = int(match.group(1))
			except:
				continue

			fp = open(os.path.join(self._dbPath, job))
			yield (id, Job.load(fp))
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


	def _update(self, id, job, state):
		if job.state == state:
			return

		old = self._findQueue(job)
		job.update(state)
		new = self._findQueue(job)

		if old != new:
			old.remove(id)
			new.add(id)

		print "%s - Job %d state changed to %s" % (strftime("%Y-%m-%d %H:%M:%S", localtime()), id, Job.states[state]),
		if state == Job.SUBMITTED and job.attempt > 1:
			print "(attempt #%s)" % job.attempt
		elif (job.get('retcode') != None) and (state == Job.FAILED):
			print "(error code: %d - %s)" % (job.get('retcode'), job.get('dest'))
		elif state == Job.QUEUED:
			print "(%s)" % job.get('dest')
		elif state == Job.WAITING:
			print '(%s)' % job.get('reason')
		elif state == Job.SUCCESS:
			runtime = job.get('runtime')
			print "(runtime %dh %0.2dmin %0.2dsec)" % (runtime / 60 / 60, (runtime / 60) % 60, runtime % 60)
		else:
			print

		self._saveJob(id)


	def getSubmissionJobs(self, maxInFlight, maxRetry, shuffle):
		curInFlight = len(self.running)
		submit = max(0, maxInFlight - curInFlight)
		if maxRetry:
			list = filter(lambda x: self._jobs.get(x, Job()).attempt < maxRetry, self.ready)
		else:
			list = self.ready[:]
		if shuffle:
			random.shuffle(list)
		return SortedList(list[:submit])


	def getWmsMap(self, idlist):
		map = {}
		for id in idlist:
			job = self._jobs[id]
			map[job.id] = (id, job)
		return map


	def check(self, wms):
		change = False
		timeoutlist = []
		wmsMap = self.getWmsMap(self.running)

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
		if len(timeoutlist):
			change = True
			print "\nTimeout for the following jobs:"
			Report(timeoutlist, self._jobs).details()
			wms.cancelJobs(self.getWmsMap(timeoutlist).keys())
			self.mark_cancelled(timeoutlist)
			# Fixme: Error handling

		return change


	def submit(self, wms, ids):
		if len(ids) == 0:
			return

		try:
			wms.bulkSubmissionBegin()
#			self.module.onJobSubmit(job, id)
			for id in ids:
				try:
					job = self._jobs[id]
				except:
					job = Job()
					self._jobs[id] = job

				wmsId = wms.submitJob(id, job)
				if wmsId == None:
					# FIXME
					continue

				job.assignId(wmsId)
				self._update(id, job, Job.SUBMITTED)
				self.module.onJobSubmit(job, id)
		finally:
			wms.bulkSubmissionEnd()


	def retrieve(self, wms):
		change = False
		wmsIds = self.getWmsMap(self.done).keys()

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

		return change


	def mark_cancelled(self, jobs):
		for id in jobs:
			try:
				job = self._jobs[id]
			except:
				continue
			self._update(id, job, Job.CANCELLED)


	def delete(self, wms, jobfilter):
		jobs = []
		if jobfilter == "TODO":
			jobfilter = "SUBMITTED,WAITING,READY,QUEUED"
		if jobfilter == "ALL":
			jobfilter = "SUBMITTED,WAITING,READY,QUEUED,RUNNING"
		if len(jobfilter) and jobfilter[0].isdigit():
			for jobId in jobfilter.split(","):
				try:
					jobs.append(int(jobId))
				except:
					raise UserError("Job identifiers must be integers.")
		else:
			jobs = filter(lambda x: self._jobs[x].statefilter(jobfilter), self._jobs)

		wmsIds = self.getWmsMap(jobs).keys()

		print "\nDeleting the following jobs:"
		Report(jobs, self._jobs).details()
		
		if not len(jobs) == 0:
			if not utils.boolUserInput('Do you really want to delete these jobs?', True):
				return 0

			if wms.cancelJobs(wmsIds):
				self.mark_cancelled(jobs)
			else:
				print "\nThere was a problem with deleting your jobs!"
				if not utils.boolUserInput('Do you want to do a forced delete?', True):
					return 0
				self.mark_cancelled(jobs)
