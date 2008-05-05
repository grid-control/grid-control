from __future__ import generators
import os, re, fnmatch
from grid_control import SortedList, ConfigError, Job, UserError, Report

class JobDB:
	def __init__(self, workDir, nJobs, init = False):
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


	def _findQueue(self, job):
		state = job.state

		if state in (Job.SUBMITTED, Job.WAITING, Job.READY,
		             Job.QUEUED, Job.RUNNING):
			queue = self.running
		elif state in (Job.INIT, Job.FAILED, Job.ABORTED, Job.CANCELLED):
			queue = self.ready	# resubmit?
		elif state == Job.DONE:
			queue = self.done
		elif state == Job.OK:
			queue = self.ok
		else:
			raise Exception("Internal error: Unexpected job state %s" % Job.states[state])

		return queue		


	def _scan(self):
		filter = re.compile(r'^job_([0-9]+)\.txt$')
		self._jobs = {}
		for job in fnmatch.filter(os.listdir(self._dbPath), 'job_*.txt'):
			match = filter.match(job)
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

		print "Job %d state changed to %s" % (id, Job.states[state])

		self._saveJob(id)


	def check(self, wms):
		change = False
		map = {}
		ids = []
		for id in self.running:
			job = self._jobs[id]
			map[job.id] = (id, job)
			ids.append(job.id)

		for id, state, info in wms.checkJobs(ids):
			id, job = map[id]
			if state != job.state:
				change = True
				for key, value in info.items():
					job.set(key, value)
				self._update(id, job, state)

		return change


	def submit(self, wms, ids):
		if len(ids) == 0:
			return

		try:
			wms.bulkSubmissionBegin()
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
		finally:
			wms.bulkSubmissionEnd()

	def retrieve(self, wms):
		change = False
		ids = []
		for id in self.done:
			job = self._jobs[id]
			ids.append(job.id)

		for id, retCode in wms.retrieveJobs(ids):
			try:
				job = self._jobs[id]
			except:
				continue

			if retCode == 0:
				state = Job.OK
			else:
				state = Job.FAILED
				print "Errorcode: %d" % retCode

			if state != job.state:
				change = True
				job.set('retcode', retCode)
				self._update(id, job, state)

		return change


	def delete(self, wms, filter):
		jobs = []
		if filter == "TODO":
			filter = "SUBMITTED,WAITING,READY,QUEUED"
		if filter == "ALL":
			filter = "SUBMITTED,WAITING,READY,QUEUED,RUNNING"
		if len(filter) and filter[0].isdigit():
			for jobId in filter.split(","):
				try:
					jobs.append(int(jobId))
				except:
					raise UserError("Job identifiers must be integers.")
		else:
			for jobId in self.all:
				if self._jobs[jobId].filter(filter):
					jobs.append(jobId)

		ids = []
		for id in jobs:
			job = self._jobs[id]
			ids.append(job.id)

		print "\nDeleting the following jobs:"

		Report(jobs,self._jobs).details()
		
		if not len(jobs) == 0:
			if raw_input('Do you really want to delete these jobs? [yes]:') == 'yes':
				if wms.cancel(ids):
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
