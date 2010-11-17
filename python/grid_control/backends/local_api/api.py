from grid_control import AbstractObject, AbstractError

class LocalWMSApi(AbstractObject):
	def __init__(self, config, localWMS):
		self.wms = localWMS

	def getQueues(self):
		return None

	def getNodes(self):
		return None

	def getJobArguments(self, jobNum, sandbox):
		raise AbstractError

	def getSubmitArguments(self, jobNum, reqs, sandbox, stdout, stderr, addAttr):
		raise AbstractError

	def parseSubmitOutput(self, data):
		raise AbstractError

	def unknownID(self):
		raise AbstractError

	def parseStatus(self, status):
		raise AbstractError

	def getCheckArguments(self, wmsIds):
		raise AbstractError

	def parseJobState(self, state):
		return self._statusMap[state]

	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)

	def checkReq(self, reqs, req, test = lambda x: x > 0):
		if req in reqs:
			return test(reqs[req])
		return False

LocalWMSApi.dynamicLoaderPath()
