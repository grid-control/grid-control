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
from grid_control.datasets import DataProvider, DatasetError
from grid_control.gc_exceptions import UserError
from grid_control.parameters.psource_base import LimitedResyncParameterSource
from grid_control.utils.activity import Activity
from python_compat import md5_hex, set

class DataParameterSource(LimitedResyncParameterSource):
	alias = ['data']

	def __init__(self, dataDir, srcName, dataProvider, dataSplitter, dataProc, repository, keepOld = True):
		LimitedResyncParameterSource.__init__(self)
		(self._dn, self._name, self._data_provider, self._data_splitter, self._part_proc, self._keepOld) = \
			(dataDir, srcName, dataProvider, dataSplitter, dataProc, keepOld)
		repository['dataset:%s' % srcName] = self
		self.resyncSetup(interval = -1)

		if not dataProvider: # debug mode - used by scripts - disables resync
			self._maxN = self._data_splitter.getMaxJobs()
			return

		# look for aborted resyncs - and try to restore old state if possible
		if self._existsDataPath('cache.dat.resync') and self._existsDataPath('map.tar.resync'):
			utils.renameFile(self._getDataPath('cache.dat.resync'), self._getDataPath('cache.dat'))
			utils.renameFile(self._getDataPath('map.tar.resync'), self._getDataPath('map.tar'))
		elif self._existsDataPath('cache.dat.resync') or self._existsDataPath('map.tar.resync'):
			raise DatasetError('Found broken resync state')

		if self._existsDataPath('cache.dat') and self._existsDataPath('map.tar'):
			self._data_splitter.importPartitions(self._getDataPath('map.tar'))
		else:
			DataProvider.saveToFile(self._getDataPath('cache.dat'), self._data_provider.getBlocks(show_stats = False))
			self._data_splitter.splitDataset(self._getDataPath('map.tar'), self._data_provider.getBlocks(show_stats = False))

		self._maxN = self._data_splitter.getMaxJobs()

	def canFinish(self):
		return self._resyncInterval < 0

	def getMaxParameters(self):
		return self._maxN

	def fillParameterKeys(self, result):
		result.extend(self._part_proc.getKeys() or [])

	def fillParameterInfo(self, pNum, result):
		splitInfo = self._data_splitter.getSplitInfo(pNum)
		self._part_proc.process(pNum, splitInfo, result)

	def getHash(self):
		if self._resync_enabled():
			return md5_hex(repr(time.time()))
		return md5_hex(repr([self._name, self._data_splitter.getMaxJobs()]))

	def show(self):
		return ['%s: src = %s' % (self.__class__.__name__, self._name)]

	def __repr__(self):
		return 'data(%s)' % utils.QM(self._name == 'data', '', self._name)

	def _getDataPath(self, postfix):
		return os.path.join(self._dn, self._name + postfix)

	def _existsDataPath(self, postfix):
		return os.path.exists(os.path.join(self._dn, self._name + postfix))

	def _resync(self):
		if self._data_provider:
			activity = Activity('Performing resync of datasource %r' % self._name)
			# Get old and new dataset information
			ds_old = DataProvider.loadFromFile(self._getDataPath('cache.dat')).getBlocks(show_stats = False)
			self._data_provider.clearCache()
			ds_new = self._data_provider.getBlocks(show_stats = False)
			self._data_provider.saveToFile(self._getDataPath('cache-new.dat'), ds_new)

			# Use old splitting information to synchronize with new dataset infos
			old_maxN = self._data_splitter.getMaxJobs()
			jobChanges = self._data_splitter.resyncMapping(self._getDataPath('map-new.tar'), ds_old, ds_new)
			activity.finish()
			if jobChanges is not None:
				# Move current splitting to backup and use the new splitting from now on
				def backupRename(old, cur, new):
					if self._keepOld:
						os.rename(self._getDataPath(cur), self._getDataPath(old))
					os.rename(self._getDataPath(new), self._getDataPath(cur))
				backupRename(  'map-old-%d.tar' % time.time(),   'map.tar',   'map-new.tar')
				backupRename('cache-old-%d.dat' % time.time(), 'cache.dat', 'cache-new.dat')
				self._data_splitter.importPartitions(self._getDataPath('map.tar'))
				self._maxN = self._data_splitter.getMaxJobs()
				self._log.debug('Dataset resync finished: %d -> %d partitions', old_maxN, self._maxN)
				return (set(jobChanges[0]), set(jobChanges[1]), old_maxN != self._maxN)

	def create(cls, pconfig, repository, src = 'data'): # pylint:disable=arguments-differ
		src_key = 'dataset:%s' % src
		if src_key not in repository:
			raise UserError('Dataset parameter source "%s" not setup!' % src)
		return repository[src_key]
	create = classmethod(create)
