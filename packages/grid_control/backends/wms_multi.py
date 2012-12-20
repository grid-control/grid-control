from grid_control import RuntimeError
from wms import WMS
from broker import Broker
# Distribute to WMS according to job id prefix

class MultiWMS(WMS):
	def __init__(self, config, defaultWMS, wmsList):
		self.defaultWMS = defaultWMS
		self.wmsMap = dict(map(lambda wmsObj: (wmsObj.wmsName, wmsObj), wmsList))
		WMS.__init__(self, config, None, None)

		# Determine WMS timings
		waitIdle, waitDefault = self.defaultWMS.getTimings()
		for wmsPrefix, wmsObj in self.wmsMap.items():
			wi, wd = wmsObj.getTimings()
			waitIdle = max(waitIdle, wi)
			waitDefault = max(waitDefault, wd)
		self.timing = (waitIdle, waitDefault)
		self.brokerWMS = self._createBroker('wms broker', 'RandomBroker', 'wms', 'wms', self.wmsMap.keys)


	def getTimings(self):
		return self.timing


	def canSubmit(self, neededTime, canCurrentlySubmit):
		canCurrentlySubmit = self.defaultWMS.canSubmit(neededTime, canCurrentlySubmit)
		for wmsPrefix, wmsObj in self.wmsMap.items():
			canCurrentlySubmit = wmsObj.canSubmit(neededTime, canCurrentlySubmit)
		return canCurrentlySubmit


	def getProxy(self, wmsId):
		return self.wmsMap.get(self._splitId(wmsId)[0], self.defaultWMS).getProxy(wmsId)


	def deployTask(self, module, monitor):
		self.defaultWMS.deployTask(module, monitor)
		for wmsPrefix, wmsObj in self.wmsMap.items():
			wmsObj.deployTask(module, monitor)


	def submitJobs(self, jobNumList, module):
		def brokerJobs(jobNum):
			jobReq = self.brokerWMS.brokerAdd(module.getRequirements(jobNum), WMS.BACKEND)
			return dict(jobReq).get(WMS.BACKEND)[0]
		return self._forwardCall(jobNumList, brokerJobs, lambda wmsObj, args: wmsObj.submitJobs(args, module))


	def checkJobs(self, ids):
		return self._forwardCall(ids, lambda (wmsId, jobNum): self._splitId(wmsId)[0], lambda wmsObj, args: wmsObj.checkJobs(args))


	def retrieveJobs(self, ids):
		return self._forwardCall(ids, lambda (wmsId, jobNum): self._splitId(wmsId)[0], lambda wmsObj, args: wmsObj.retrieveJobs(args))


	def cancelJobs(self, ids):
		return self._forwardCall(ids, lambda (wmsId, jobNum): self._splitId(wmsId)[0], lambda wmsObj, args: wmsObj.cancelJobs(args))


	def _assignArgs(self, args, assignFun):
		argMap = {}
		for arg in args: # Assign args to backends
			backend = assignFun(arg)
			if not backend:
				backend = self.defaultWMS.wmsName
			argMap.setdefault(backend, []).append(arg)
		return argMap


	def _forwardCall(self, args, assignFun, callFun):
		argMap = self._assignArgs(args, assignFun)
		for wmsPrefix in filter(lambda wmsPrefix: wmsPrefix in argMap, self.wmsMap):
			for result in callFun(self.wmsMap[wmsPrefix], argMap[wmsPrefix]):
				yield result
