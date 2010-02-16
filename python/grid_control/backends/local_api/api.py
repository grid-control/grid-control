from grid_control import AbstractObject, AbstractError

class LocalWMSApi(AbstractObject):
	def __init__(self, config, localWMS):
		self.config = config
		self.wms = localWMS

	def getQueues(self):
		raise AbstractError

	def getArguments(self, jobNum, sandbox):
		raise AbstractError

	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr):
		raise AbstractError

	def parseSubmitOutput(self, data):
		raise AbstractError

	def unknownID(self):
		raise AbstracError

	def parseStatus(self, status):
		raise AbstracError

	def getCheckArgument(self, wmsIds):
		raise AbstracError

	def parseJobState(self, state):
		return self._statusMap[state]

	def getCancelArgument(self, wmsIds):
		return str.join(" ", wmsIds)
