import os, re, fnmatch
from grid_control import ConfigError, Job

class JobDB:
	def __init__(self, workDir, init = False):
		self.dbPath = os.path.join(workDir, 'jobs')
		try:
			if not os.path.exists(self.dbPath):
				if init:
					os.mkdir(self.dbPath)
				else:
					raise ConfigError("Not a properly initialized work directory '%s'." % workDir)
		except IOError, e:
			raise ConfigError("Problem creating work directory '%s': %s" % (self.dbPath, e))

		self.scan()

		for id, job in self._jobs.items():
			print "Job %d is %s." % (id, job.states[job.state])


	def scan(self):
		filter = re.compile(r'^job_([0-9]+)\.txt$')
		self._jobs = {}
		for job in fnmatch.filter(os.listdir(self.dbPath), 'job_*.txt'):
			match = filter.match(job)
			try:
				id = int(match.group(1))
			except:
				continue

			fp = open(os.path.join(self.dbPath, job))
			self._jobs[id] = Job(fp)
			fp.close()
