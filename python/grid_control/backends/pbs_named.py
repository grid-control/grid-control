from pbs import PBS

class PBS_NAMED(PBS):
	def __init__(self, config, module, init):
		PBS.__init__(self, config, module, init)
#		self._nameFile = config.getPath('pbs', 'name source')
		self._source = ["Nero", "Caesar", "Augustus"]


	def getJobName(self, taskId, jobId):
		return self._source[jobId % len(self._source)]
