from __future__ import generators
import os, re, fnmatch
from grid_control import SortedList, ConfigError, Job

class JobDB:
	def __init__(self, workDir, nJobs, inFlight, init = False):
		self._dbPath = os.path.join(workDir, 'jobs')
		try:
			if not os.path.exists(self._dbPath):
				if init:
					os.mkdir(self._dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self._dbPath, e))

		self._n = nJobs
		self._max = inFlight

		self._all = SortedList()
		self._states = {}
		for state in Job.states:
			self._states[state] = SortedList()

		for id, job in self._scan():
			self._jobs[id] = job
			self._all = id
			self._states[job.state] = id

		


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
			yield (id, Job(fp))
			fp.close()


	def list(self, types = None):
		for id, job in self._jobs.items():
			if types == None or job.state in types:
				yield id
