# | Copyright 2009-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, time
from grid_control import utils
from grid_control.datasets import DataProvider
from grid_control.gc_exceptions import UserError
from grid_control.parameters.psource_base import ParameterSource
from python_compat import md5_hex

class DataParameterSource(ParameterSource):
	def __init__(self, dataDir, srcName, dataProvider, dataSplitter, dataProc, keepOld = True):
		ParameterSource.__init__(self)
		(self._dataDir, self._srcName, self._dataProvider, self._dataSplitter, self._part_proc) = \
			(dataDir, srcName, dataProvider, dataSplitter, dataProc)

		if not dataProvider:
			pass # debug mode - used by scripts - disables resync
		elif os.path.exists(self.getDataPath('cache.dat') and self.getDataPath('map.tar')):
			self._dataSplitter.importPartitions(self.getDataPath('map.tar'))
		else:
			DataProvider.saveToFile(self.getDataPath('cache.dat'), self._dataProvider.getBlocks(silent = False))
			self._dataSplitter.splitDataset(self.getDataPath('map.tar'), self._dataProvider.getBlocks())

		self._maxN = self._dataSplitter.getMaxJobs()
		self._keepOld = keepOld

	def getNeededDataKeys(self):
		return self._part_proc.getNeededKeys(self._dataSplitter)

	def getMaxParameters(self):
		return self._maxN

	def fillParameterKeys(self, result):
		result.extend(self._part_proc.getKeys())

	def fillParameterInfo(self, pNum, result):
		splitInfo = self._dataSplitter.getSplitInfo(pNum)
		self._part_proc.process(pNum, splitInfo, result)

	def getHash(self):
		return md5_hex(str(self._srcName) + str(self._dataSplitter.getMaxJobs()) + str(self.resyncEnabled()))

	def show(self):
		return ['%s: src = %s' % (self.__class__.__name__, self._srcName)]

	def __repr__(self):
		return 'data(%s)' % utils.QM(self._srcName == 'data', '', self._srcName)

	def getDataPath(self, postfix):
		return os.path.join(self._dataDir, self._srcName + postfix)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = ParameterSource.resync(self)
		if self.resyncEnabled() and self._dataProvider:
			# Get old and new dataset information
			old = DataProvider.loadFromFile(self.getDataPath('cache.dat')).getBlocks()
			self._dataProvider.clearCache()
			new = self._dataProvider.getBlocks()
			self._dataProvider.saveToFile(self.getDataPath('cache-new.dat'), new)

			# Use old splitting information to synchronize with new dataset infos
			jobChanges = self._dataSplitter.resyncMapping(self.getDataPath('map-new.tar'), old, new)
			if jobChanges:
				# Move current splitting to backup and use the new splitting from now on
				def backupRename(old, cur, new):
					if self._keepOld:
						os.rename(self.getDataPath(cur), self.getDataPath(old))
					os.rename(self.getDataPath(new), self.getDataPath(cur))
				backupRename(  'map-old-%d.tar' % time.time(),   'map.tar',   'map-new.tar')
				backupRename('cache-old-%d.dat' % time.time(), 'cache.dat', 'cache-new.dat')
				old_maxN = self._dataSplitter.getMaxJobs()
				self._dataSplitter.importPartitions(self.getDataPath('map.tar'))
				self._maxN = self._dataSplitter.getMaxJobs()
				result_redo.update(jobChanges[0])
				result_disable.update(jobChanges[1])
				result_sizeChange = result_sizeChange or (old_maxN != self._maxN)
			self.resyncFinished()
		return (result_redo, result_disable, result_sizeChange)

	def create(cls, pconfig = None, src = 'data'): # pylint:disable=arguments-differ
		if src not in DataParameterSource.datasetsAvailable:
			raise UserError('Dataset parameter source "%s" not setup!' % src)
		result = DataParameterSource.datasetsAvailable[src]
		DataParameterSource.datasetsUsed.append(result)
		return result
	create = classmethod(create)

DataParameterSource.datasetsAvailable = {}
DataParameterSource.datasetsUsed = []
ParameterSource.managerMap['data'] = 'DataParameterSource'
