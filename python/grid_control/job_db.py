from __future__ import generators
import os, re, fnmatch
from grid_control import SortedList, ConfigError, Job

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
		elif state in (Job.INIT, Job.FAILED, Job.ABORTED):
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


	def submit(self, wms, id):
		try:
			job = self._jobs[id]
		except:
			job = Job()
			self._jobs[id] = job

		wmsId = wms.submitJob(id, job)
		if wmsId == None:
			# FIXME
			return

		job.assignId(wmsId)
		self._update(id, job, Job.SUBMITTED)
