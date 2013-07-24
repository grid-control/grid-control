class JobDef(object):
	def __init__(self, jobNum):
		self.variables = {}
		self.active = True
		self.jobNum = jobNum
		(self.files, self.software, self.storage) = ([], None, None)
		(self.memory, self.time_wall, self.time_cpu, self.cores) = (None, None, None, None)

	def _combineReq(self, fun, oldValue, newValue):
		if oldValue != None:
			return fun(oldValue, newValue)
		return newValue

	def requireMemory(self, value):
		self.memory = self._combineReq(max, self.memory, value)

	def requireWalltime(self, value):
		self.time_wall = self._combineReq(max, self.time_wall, value)

	def requireCPUtime(self, value):
		self.time_cpu = self._combineReq(max, self.time_cpu, value)

	def requireCores(self, value):
		self.cores = self._combineReq(max, self.cores, value)

	def requireSoftware(self, value):
		self.software = self._combineReq(lambda l, i: l + i, self.software, [value])

	def requireStorage(self, listvalue):
		self.storage = self._combineReq(lambda l, i: l + i, self.storage, listvalue)
