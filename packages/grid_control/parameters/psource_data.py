#-#  Copyright 2012-2014 Karlsruhe Institute of Technology
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

import time, os
from psource_base import ParameterSource, ParameterMetadata, ParameterInfo
from grid_control import utils, WMS, QM, UserError
from grid_control.datasets import DataSplitter, DataProvider

class DataSplitProcessor:
	def __init__(self, checkSE = True):
		self.checkSE = checkSE

	def formatFileList(self, fl):
		return str.join(' ', fl)

	def getKeys(self):
		return map(lambda k: ParameterMetadata(k, untracked=True), ['FILE_NAMES', 'MAX_EVENTS',
			'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])

	def process(self, pNum, splitInfo, result):
		result.update({
			'FILE_NAMES': self.formatFileList(splitInfo[DataSplitter.FileList]),
			'MAX_EVENTS': splitInfo[DataSplitter.NEntries],
			'SKIP_EVENTS': splitInfo.get(DataSplitter.Skipped, 0),
			'DATASETID': splitInfo.get(DataSplitter.DatasetID, None),
			'DATASETPATH': splitInfo.get(DataSplitter.Dataset, None),
			'DATASETBLOCK': splitInfo.get(DataSplitter.BlockName, None),
			'DATASETNICK': splitInfo.get(DataSplitter.Nickname, None),
			'DATASETSPLIT': pNum,
		})
		result[ParameterInfo.REQS].append((WMS.STORAGE, splitInfo.get(DataSplitter.Locations)))
		result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and not splitInfo.get(DataSplitter.Invalid, False)
		if self.checkSE:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (splitInfo.get(DataSplitter.Locations) != [])


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
			self.dataProvider.saveState(self.getDataPath('cache.dat'))
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
		if utils.verbosity() > 2:
			utils.vprint('Dataset task number: %d' % pNum)
			DataSplitter.printInfoForJob(splitInfo)
		self.dataProc.process(pNum, splitInfo, result)

	def getHash(self):
		return utils.md5(str(self.srcName) + str(self.dataSplitter.getMaxJobs())).hexdigest()

	def __repr__(self):
		return 'data(%s)' % QM(self.srcName == 'data', '', self.srcName)

	def getDataPath(self, postfix):
		return os.path.join(self.dataDir, self.srcName + postfix)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = ParameterSource.resync(self)
		if self.resyncEnabled() and self.dataProvider:
			# Get old and new dataset information
			old = DataProvider.loadState(self.getDataPath('cache.dat')).getBlocks()
			self.dataProvider.clearCache()
			new = self.dataProvider.getBlocks()
			self.dataProvider.saveState(self.getDataPath('cache-new.dat'))

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
ParameterSource.managerMap['data'] = DataParameterSource
