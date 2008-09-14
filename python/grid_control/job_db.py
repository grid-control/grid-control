from __future__ import generators
import os, re, fnmatch
from time import localtime, strftime
from grid_control import SortedList, ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, workDir, nJobs, module, init = False):
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

		i = 0
		for j in self.all:
			self.ready.extend(xrange(i, min(j, nJobs)))
			i = j + 1
			queue = self._findQueue(self._jobs[j])
			queue.append(j)
		self.ready.extend(xrange(i, nJobs))
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
		else:
			print

		self._saveJob(id)


	def getSubmissionJobs(self, maxInFlight, maxRetry):
		curInFlight = len(self.running)
		submit = maxInFlight - curInFlight
		if submit < 0:
			submit = 0
		if maxRetry:
			list = filter(lambda x: self._jobs.get(x, Job()).attempt < maxRetry, self.ready)
		else:
			list = self.ready
		return list[:submit]


	def getWmsMap(self, idlist):
		map = {}
		for id in idlist:
			job = self._jobs[id]
			map[job.id] = (id, job)
		return map


	def check(self, wms):
		change = False
		map = {}
		wmsIds = []
		for id in self.running:
			job = self._jobs[id]
			map[job.id] = (id, job)
			wmsIds.append(job.id)

		for id, state, info in wms.checkJobs(wmsIds):
			id, job = map[id]
			if state != job.state:
				change = True
				for key, value in info.items():
					job.set(key, value)
				self._update(id, job, state)
				self.module.onJobUpdate(job, id, info)

		return change


	def submit(self, wms, wmsIds):
		if len(wmsIds) == 0:
			return

		try:
			wms.bulkSubmissionBegin()
			for id in wmsIds:
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
		wmsIds = []
		for id in self.done:
			job = self._jobs[id]
			wmsIds.append(job.id)

		for id, retCode in wms.retrieveJobs(wmsIds):
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

		wmsIds = []
		for id in jobs:
			job = self._jobs[id]
			wmsIds.append(job.id)

		print "\nDeleting the following jobs:"
		Report(jobs,self._jobs).details()
		
		if not len(jobs) == 0:
			userinput = raw_input('Do you really want to delete these jobs? [yes]:')
			if userinput == 'yes' or userinput == '':
				if wms.cancel(wmsIds):
					for id in jobs:
						try:
							job = self._jobs[id]
						except:
							continue

						self._update(id, job, Job.CANCELLED)
				else:
					print "\nThere was a problem with deleting your jobs!"
					if raw_input('Do you want to do a forced delete? [yes]:') == 'yes':
						for id in jobs:
							try:
								job = self._jobs[id]
							except:
								continue
							self._update(id, job, Job.CANCELLED)

			else:
				return 0
