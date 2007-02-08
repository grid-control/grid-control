import os
from grid_control import ConfigError

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

	def scan(self):
		pass
