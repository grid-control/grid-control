# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

from grid_control.config import triggerResync
from grid_control.datasets import DataProvider, DataSplitter, DatasetError
from grid_control.datasets.splitter_basic import HybridSplitter
from grid_control.utils import optSplit
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.thread_tools import start_thread
from grid_control.utils.webservice import JSONRestClient
from grid_control_cms.lumi_tools import parseLumiFilter, strLumi
from grid_control_cms.sitedb import SiteDB
from python_compat import itemgetter, lfilter, sorted

CMSLocationFormat = makeEnum(['hostname', 'siteDB', 'both'])

# required format: <dataset path>[@<instance>][#<block>]
class CMSBaseProvider(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick = None):
		self._changeTrigger = triggerResync(['datasets', 'parameters'])
		self._lumi_filter = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi, onChange = self._changeTrigger)
		if not self._lumi_filter.empty():
			config.set('dataset processor', 'LumiDataProcessor', '+=')
		DataProvider.__init__(self, config, datasetExpr, datasetNick)
		# LumiDataProcessor instantiated in DataProcessor.__ini__ will set lumi metadata as well
		self._lumi_query = config.getBool('lumi metadata', not self._lumi_filter.empty(), onChange = self._changeTrigger)
		config.set('phedex sites matcher mode', 'shell', '?=')
		# PhEDex blacklist: 'T1_*_Disk nodes allow user jobs - other T1's dont!
		self._phedexFilter = config.getFilter('phedex sites', '-* T1_*_Disk T2_* T3_*',
			defaultMatcher = 'blackwhite', defaultFilter = 'strict', onChange = self._changeTrigger)
		self._onlyComplete = config.getBool('only complete sites', True, onChange = self._changeTrigger)
		self._locationFormat = config.getEnum('location format', CMSLocationFormat, CMSLocationFormat.hostname, onChange = self._changeTrigger)
		self._pjrc = JSONRestClient(url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas')
		self._sitedb = SiteDB()

		(self._datasetPath, self._datasetInstance, self._datasetBlock) = optSplit(datasetExpr, '@#')
		instance_default = config.get('dbs instance', '', onChange = self._changeTrigger)
		self._datasetInstance = self._datasetInstance or instance_default
		if not self._datasetInstance:
			self._datasetInstance = 'prod/global'
		elif '/' not in self._datasetInstance:
			self._datasetInstance = 'prod/%s' % self._datasetInstance
		self._datasetBlock = self._datasetBlock or 'all'
		self.onlyValid = config.getBool('only valid', True, onChange = self._changeTrigger)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 2 * 60 * 60 # 2 hour delay minimum


	# Check if splitterClass is valid
	def checkSplitter(self, splitterClass):
		if (DataSplitter.Skipped in splitterClass.neededEnums()) and not self._lumi_filter.empty():
			self._log.debug('Selected splitter %s is not compatible with active lumi filter!', splitterClass.__name__)
			self._log.warning('Active lumi section filter forced selection of HybridSplitter')
			return HybridSplitter
		return splitterClass


	def _replicaLocation(self, replica_info):
		(name_node, name_hostname, _) = replica_info
		if self._locationFormat == CMSLocationFormat.siteDB:
			yield name_node
		else:
			if name_hostname is not None:
				name_hostnames = [name_hostname]
			else:
				name_hostnames = self._sitedb.cms_name_to_se(name_node)
			for name_hostname in name_hostnames:
				if self._locationFormat == CMSLocationFormat.hostname:
					yield name_hostname
				else:
					yield '%s/%s' % (name_node, name_hostname)


	def _fmtLocations(self, replica_infos):
		for replica_info in replica_infos:
			(_, _, completed) = replica_info
			if completed:
				for entry in self._replicaLocation(replica_info):
					yield entry
			else:
				for entry in self._replicaLocation(replica_info):
					yield '(%s)' % entry


	def _processReplicas(self, blockPath, replica_infos):
		def empty_with_warning(*args):
			self._log.warning(*args)
			return []
		def expanded_replica_locations(replica_infos):
			for replica_info in replica_infos:
				for entry in self._replicaLocation(replica_info):
					yield entry

		if not replica_infos:
			return empty_with_warning('Dataset block %r has no replica information!', blockPath)
		replica_infos_selected = self._phedexFilter.filterList(replica_infos, key = itemgetter(0))
		if not replica_infos_selected:
			return empty_with_warning('Dataset block %r is not available at the selected locations!\nAvailable locations: %s', blockPath,
				str.join(', ', self._fmtLocations(replica_infos)))
		if not self._onlyComplete:
			return list(expanded_replica_locations(replica_infos_selected))
		replica_infos_complete = lfilter(lambda nn_nh_c: nn_nh_c[2], replica_infos_selected)
		if not replica_infos_complete:
			return empty_with_warning('Dataset block %r is not completely available at the selected locations!\nAvailable locations: %s', blockPath,
				str.join(', ', self._fmtLocations(replica_infos)))
		return list(expanded_replica_locations(replica_infos_complete))


	# Get dataset se list from PhEDex (perhaps concurrent with listFiles)
	def _getPhedexReplicas(self, blockPath, dictReplicas):
		dictReplicas[blockPath] = []
		for phedexBlock in self._pjrc.get(params = {'block': blockPath})['phedex']['block']:
			for replica in phedexBlock['replica']:
				dictReplicas[blockPath].append((replica['node'], replica.get('se'), replica['complete'] == 'y'))


	def getDatasets(self):
		if self._cache_dataset is None:
			self._cache_dataset = [self._datasetPath]
			if '*' in self._datasetPath:
				self._cache_dataset = list(self._getCMSDatasets(self._datasetPath))
				if not self._cache_dataset:
					raise DatasetError('No datasets selected by DBS wildcard %s !' % self._datasetPath)
		return self._cache_dataset


	def _getCMSBlocks(self, datasetPath, getSites):
		iter_blockname_selist = self._getCMSBlocksImpl(datasetPath, getSites)
		n_blocks = 0
		selected_blocks = False
		for (blockname, selist) in iter_blockname_selist:
			n_blocks += 1
			if (self._datasetBlock != 'all') and (str.split(blockname, '#')[1] != self._datasetBlock):
				continue
			selected_blocks = True
			yield (blockname, selist)
		if (n_blocks > 0) and not selected_blocks:
			raise DatasetError('Dataset %r contains %d blocks, but none were selected by %r' % (datasetPath, n_blocks, self._datasetBlock))


	def _fillCMSFiles(self, block, blockPath):
		lumi_used = False
		lumiDict = {}
		if self._lumi_query: # central lumi query
			lumiDict = self._getCMSLumisImpl(blockPath)
		fileList = []
		for (fileInfo, listLumi) in self._getCMSFilesImpl(blockPath, self.onlyValid, self._lumi_query):
			if lumiDict and not listLumi:
				listLumi = lumiDict.get(fileInfo[DataProvider.URL], [])
			if listLumi:
				(listLumiExt_Run, listLumiExt_Lumi) = ([], [])
				for (run, lumi_list) in sorted(listLumi):
					listLumiExt_Run.extend([run] * len(lumi_list))
					listLumiExt_Lumi.extend(lumi_list)
				fileInfo[DataProvider.Metadata] = [listLumiExt_Run, listLumiExt_Lumi]
				lumi_used = True
			fileList.append(fileInfo)
		if lumi_used:
			block.setdefault(DataProvider.Metadata, []).extend(['Runs', 'Lumi'])
		block[DataProvider.FileList] = fileList


	def _getCMSLumisImpl(self, blockPath):
		return None


	def _getGCBlocks(self, usePhedex):
		for datasetPath in self.getDatasets():
			counter = 0
			for (blockPath, replica_infos) in self._getCMSBlocks(datasetPath, getSites = not usePhedex):
				result = {}
				result[DataProvider.Dataset] = blockPath.split('#')[0]
				result[DataProvider.BlockName] = blockPath.split('#')[1]

				if usePhedex: # Start parallel phedex query
					dictReplicas = {}
					tPhedex = start_thread('Query phedex site info for %s' % blockPath, self._getPhedexReplicas, blockPath, dictReplicas)
					self._fillCMSFiles(result, blockPath)
					tPhedex.join()
					replica_infos = dictReplicas.get(blockPath)
				else:
					self._fillCMSFiles(result, blockPath)
				result[DataProvider.Locations] = self._processReplicas(blockPath, replica_infos)

				if len(result[DataProvider.FileList]):
					counter += 1
					yield result

			if counter == 0:
				raise DatasetError('Dataset %s does not contain any valid blocks!' % datasetPath)


class DBS2Provider(CMSBaseProvider):
	alias = ['dbs2']

	def __init__(self, config, datasetExpr, datasetNick = None):
		raise DatasetError('CMS deprecated all DBS2 Services in April 2014! Please use DBS3Provider instead.')
