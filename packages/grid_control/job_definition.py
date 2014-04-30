#-#  Copyright 2013 Karlsruhe Institute of Technology
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

	def requireStorage(self, selist):
		self.storage = self._combineReq(lambda l, i: l + i, self.storage, selist)

	def applyTo(self, other):
		other.variables.update(self.variables)
		other.active = other.active and self.active
		other.files.extend(self.files)
		# FIX requirements!

	def getHash(self):
		return None
