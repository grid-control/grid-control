from pbs import PBS

class PBS_NAMED(PBS):
	def __init__(self, config, module, init):
		PBS.__init__(self, config, module, init)
		self._nameFile = config.getPath('local', 'name source', '')
		self._source = open(self._nameFile, 'r').readlines()


	def getJobName(self, taskId, jobId):
		return self._source[jobId % len(self._source)]
