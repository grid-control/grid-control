#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
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

import os, time
from grid_control import utils
from grid_control.datasets import DataProvider
from grid_control.gc_exceptions import UserError
from grid_control.parameters.psource_base import ParameterMetadata, ParameterSource
from python_compat import md5_hex

class DataParameterSource(ParameterSource):
	def __init__(self, dataDir, srcName, dataProvider, dataSplitter, dataProc):
		ParameterSource.__init__(self)
		(self.dataDir, self.srcName, self.dataProvider, self.dataSplitter, self.dataProc) = \
			(dataDir, srcName, dataProvider, dataSplitter, dataProc)

		if not dataProvider:
			pass # debug mode - used by scripts - disables resync
		elif os.path.exists(self.getDataPath('cache.dat') and self.getDataPath('map.tar')):
			self.dataSplitter.importState(self.getDataPath('map.tar'))
		else:
			DataProvider.saveToFile(self.getDataPath('cache.dat'), self.dataProvider.getBlocks())
			self.dataSplitter.splitDataset(self.getDataPath('map.tar'), self.dataProvider.getBlocks())

		self.maxN = self.dataSplitter.getMaxJobs()
		self.keepOld = True

	def getMaxParameters(self):
		return self.maxN

	def fillParameterKeys(self, result):
		result.append(ParameterMetadata('DATASETSPLIT'))
		result.extend(self.dataProc.getKeys())

	def fillParameterInfo(self, pNum, result):
		splitInfo = self.dataSplitter.getSplitInfo(pNum)
		self.dataProc.process(pNum, splitInfo, result)

	def getHash(self):
		return md5_hex(str(self.srcName) + str(self.dataSplitter.getMaxJobs()))

	def __repr__(self):
		return 'data(%s)' % utils.QM(self.srcName == 'data', '', self.srcName)

	def getDataPath(self, postfix):
		return os.path.join(self.dataDir, self.srcName + postfix)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = ParameterSource.resync(self)
		if self.resyncEnabled() and self.dataProvider:
			# Get old and new dataset information
			old = DataProvider.loadFromFile(self.getDataPath('cache.dat')).getBlocks()
			self.dataProvider.clearCache()
			new = self.dataProvider.getBlocks()
			self.dataProvider.saveToFile(self.getDataPath('cache-new.dat'), new)

			# Use old splitting information to synchronize with new dataset infos
			jobChanges = self.dataSplitter.resyncMapping(self.getDataPath('map-new.tar'), old, new)
			if jobChanges:
				# Move current splitting to backup and use the new splitting from now on
				def backupRename(old, cur, new):
					if self.keepOld:
						os.rename(self.getDataPath(cur), self.getDataPath(old))
					os.rename(self.getDataPath(new), self.getDataPath(cur))
				backupRename(  'map-old-%d.tar' % time.time(),   'map.tar',   'map-new.tar')
				backupRename('cache-old-%d.dat' % time.time(), 'cache.dat', 'cache-new.dat')
				old_maxN = self.dataSplitter.getMaxJobs()
				self.dataSplitter.importState(self.getDataPath('map.tar'))
				self.maxN = self.dataSplitter.getMaxJobs()
				self.dataSplitter.getMaxJobs()
				result_redo.update(jobChanges[0])
				result_disable.update(jobChanges[1])
				result_sizeChange = result_sizeChange or (old_maxN != self.maxN)
			self.resyncFinished()
		return (result_redo, result_disable, result_sizeChange)

	def create(cls, pconfig = None, src = 'data'):
		if src not in DataParameterSource.datasetsAvailable:
			raise UserError('Dataset parameter source "%s" not setup!' % src)
		result = DataParameterSource.datasetsAvailable[src]
		DataParameterSource.datasetsUsed.append(result)
		return result
	create = classmethod(create)

DataParameterSource.datasetsAvailable = {}
DataParameterSource.datasetsUsed = []
ParameterSource.managerMap['data'] = 'DataParameterSource'
