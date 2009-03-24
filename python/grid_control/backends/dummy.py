from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO
from grid_control import ConfigError, Job, utils
from wms import WMS

try:
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

class DummyWMS(WMS):
	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, 'grid', init)


	def submitJob(self, id, job):
		print "EVENT [SUBMIT]: ", id, job
		self.module.getJobArguments(id)
		print self.module.getEnvironment(id)


	def checkJobs(self, ids):
		print "EVENT [CHECK]: ", ids
#		result.append((id, status, data))
		return []


	def getJobsOutput(self, ids):
		print "EVENT [OUTPUT]: ", ids
#		result.append(os.path.join(tmpPath, file))
		return []


	def cancelJobs(self, ids):
		print "EVENT [CANCEL]: ", ids
		return True
