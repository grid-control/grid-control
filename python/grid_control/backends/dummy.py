from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO, random
from grid_control import ConfigError, Job, utils
from wms import WMS

class DummyWMS(WMS):
	def __init__(self, workDir, config, opts, module):
		WMS.__init__(self, workDir, config, opts, module, 'grid')
		self.jobmap = {}

	def submitJob(self, jobNum):
		print "EVENT [SUBMIT]: ", jobNum
		self.jobmap[jobNum] = random.randint(0, 10000000)
		return (jobNum, self.jobmap[jobNum], {})


	def checkJobs(self, ids):
		print "EVENT [CHECK]: ", ids
#		result.append((id, status, data))
		return map(lambda id: (id, 0, {}), ids)


	def getJobsOutput(self, ids):
		print "EVENT [OUTPUT]: ", ids
#		result.append(os.path.join(tmpPath, file))
		return []


	def cancelJobs(self, ids):
		print "EVENT [CANCEL]: ", ids
		return True
