import sys, os, random
from grid_control import Job, utils
from wms import WMS

class DummyWMS(WMS):
	def __init__(self, config, module, monitor):
		WMS.__init__(self, config, module, monitor, 'grid')
		self.mapNum2ID = {}

	def submitJob(self, jobNum):
		utils.eprint('EVENT [SUBMIT]: %d' % jobNum)
		self.mapNum2ID[jobNum] = random.randint(0, 10000000)
		self.writeJobConfig(jobNum, os.path.join(self.config.workDir, '%s.info' % jobNum))
		return (jobNum, self.mapNum2ID[jobNum], {})


	def checkJobs(self, ids):
		utils.eprint('EVENT [CHECK]: %s' % ids)
		return map(lambda (wmsId, jobNum): (jobNum, wmsId, Job.QUEUED, {}), ids)


	def getJobsOutput(self, ids):
		utils.eprint('EVENT [OUTPUT]: %s' % ids)
		return []


	def cancelJobs(self, ids):
		utils.eprint('EVENT [CANCEL]: %s' % ids)
		return ids
