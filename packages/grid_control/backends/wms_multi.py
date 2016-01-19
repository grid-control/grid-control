#-#  Copyright 2012-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from grid_control.backends.broker import Broker
from grid_control.backends.wms import WMS
from grid_control.utils import Result

# Distribute to WMS according to job id prefix

class MultiWMS(WMS):
	def __init__(self, config, wmsName, wmsList):
		WMS.__init__(self, config, wmsName)
		self._defaultWMS = wmsList[0].getInstance()
		defaultT = self._defaultWMS.getTimings()
		self._timing = Result(waitOnIdle = defaultT.waitOnIdle, waitBetweenSteps = defaultT.waitBetweenSteps)
		self._wmsMap = {self._defaultWMS.getObjectName().lower(): self._defaultWMS}
		for wmsEntry in wmsList[1:]:
			wmsObj = wmsEntry.getInstance()
			self._wmsMap[wmsEntry.getObjectName().lower()] = wmsObj
			wmsT = wmsObj.getTimings()
			self._timing.waitOnIdle = max(self._timing.waitOnIdle, wmsT.waitOnIdle)
			self._timing.waitBetweenSteps = max(self._timing.waitBetweenSteps, wmsT.waitBetweenSteps)

		self._brokerWMS = config.getPlugin('wms broker', 'RandomBroker',
			cls = Broker, tags = [self]).getInstance('wms', 'wms', self._wmsMap.keys)


	def getTimings(self):
		return self._timing


	def canSubmit(self, neededTime, canCurrentlySubmit):
		canCurrentlySubmit = self._defaultWMS.canSubmit(neededTime, canCurrentlySubmit)
		for wmsPrefix, wmsObj in self._wmsMap.items():
			canCurrentlySubmit = wmsObj.canSubmit(neededTime, canCurrentlySubmit)
		return canCurrentlySubmit


	def getAccessToken(self, gcID):
		return self._wmsMap.get(self._splitId(gcID)[0], self._defaultWMS).getAccessToken(gcID)


	def deployTask(self, task, monitor):
		self._defaultWMS.deployTask(task, monitor)
		for wmsPrefix, wmsObj in self._wmsMap.items():
			wmsObj.deployTask(task, monitor)


	def submitJobs(self, jobNumList, task):
		def brokerJobs(jobNum):
			jobReq = self._brokerWMS.brokerAdd(task.getRequirements(jobNum), WMS.BACKEND)
			return dict(jobReq).get(WMS.BACKEND)[0]
		return self._forwardCall(jobNumList, brokerJobs, lambda wmsObj, args: wmsObj.submitJobs(args, task))


	def checkJobs(self, ids):
		return self._forwardCall(ids, lambda (wmsId, jobNum): self._splitId(wmsId)[0], lambda wmsObj, args: wmsObj.checkJobs(args))


	def retrieveJobs(self, ids):
		return self._forwardCall(ids, lambda (wmsId, jobNum): self._splitId(wmsId)[0], lambda wmsObj, args: wmsObj.retrieveJobs(args))


	def cancelJobs(self, ids):
		return self._forwardCall(ids, lambda (wmsId, jobNum): self._splitId(wmsId)[0], lambda wmsObj, args: wmsObj.cancelJobs(args))


	def _getMapID2Backend(self, args, assignFun):
		argMap = {}
		for arg in args: # Assign args to backends
			backend = assignFun(arg)
			if not backend:
				backend = self._defaultWMS.wmsName
			argMap.setdefault(backend.lower(), []).append(arg)
		return argMap


	def _forwardCall(self, args, assignFun, callFun):
		argMap = self._getMapID2Backend(args, assignFun)
		for wmsPrefix in filter(lambda wmsPrefix: wmsPrefix in argMap, self._wmsMap):
			wms = self._wmsMap[wmsPrefix]
			for result in callFun(wms, argMap[wmsPrefix]):
				yield result
