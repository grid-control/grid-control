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

from grid_control import utils
from grid_control.config import triggerResync
from grid_control.datasets import DataProvider, DataSplitter, DatasetError
from grid_control.datasets.splitter_basic import HybridSplitter
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.thread_tools import start_thread
from grid_control.utils.webservice import JSONRestClient
from grid_control_cms.lumi_tools import parseLumiFilter, strLumi
from python_compat import sorted

CMSLocationFormat = makeEnum(['hostname', 'siteDB', 'both'])
PhedexT1Mode = makeEnum(['accept', 'disk', 'none'])

# required format: <dataset path>[@<instance>][#<block>]
class CMSBaseProvider(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		changeTrigger = triggerResync(['datasets', 'parameters'])
		self._lumi_filter = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi, onChange = changeTrigger)
		if not self._lumi_filter.empty():
			config.set('dataset processor', 'LumiDataProcessor', '+=')
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		# LumiDataProcessor instantiated in DataProcessor.__ini__ will set lumi metadata as well
		self._lumi_query = config.getBool('lumi metadata', not self._lumi_filter.empty(), onChange = changeTrigger)
		# PhEDex blacklist: 'T1_DE_KIT', 'T1_US_FNAL' and '*_Disk' allow user jobs - other T1's dont!
		self._phedexFilter = config.getFilter('phedex sites', '-T3_US_FNALLPC',
			defaultMatcher = 'blackwhite', defaultFilter = 'weak', onChange = changeTrigger)
		self._phedexT1Filter = config.getFilter('phedex t1 accept', 'T1_DE_KIT T1_US_FNAL',
			defaultMatcher = 'blackwhite', defaultFilter = 'weak', onChange = changeTrigger)
		self._phedexT1Mode = config.getEnum('phedex t1 mode', PhedexT1Mode, PhedexT1Mode.disk, onChange = changeTrigger)
		self.onlyComplete = config.getBool('only complete sites', True, onChange = changeTrigger)
		self._locationFormat = config.getEnum('location format', CMSLocationFormat, CMSLocationFormat.hostname, onChange = changeTrigger)
		self._pjrc = JSONRestClient(url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas')

		(self._datasetPath, self._url, self._datasetBlock) = utils.optSplit(datasetExpr, '@#')
		self._url = self._url or config.get('dbs instance', '')
		self._datasetBlock = self._datasetBlock or 'all'
		self.onlyValid = config.getBool('only valid', True, onChange = changeTrigger)


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


	def _nodeFilter(self, nameSiteDB, complete):
		# Remove T0 and T1 by default
		result = not (nameSiteDB.startswith('T0_') or nameSiteDB.startswith('T1_'))
		# check if listed on the accepted list
		if self._phedexT1Mode in [PhedexT1Mode.disk, PhedexT1Mode.accept]:
			result = result or (self._phedexT1Filter.filterList([nameSiteDB]) == [nameSiteDB])
		if self._phedexT1Mode == PhedexT1Mode.disk:
			result = result or nameSiteDB.lower().endswith('_disk')
		# apply phedex blacklist
		result = result and (self._phedexFilter.filterList([nameSiteDB]) == [nameSiteDB])
		# check for completeness at the site
		result = result and (complete or not self.onlyComplete)
		return result


	# Get dataset se list from PhEDex (perhaps concurrent with listFiles)
	def _getPhedexSEList(self, blockPath, dictSE):
		dictSE[blockPath] = []
		for phedexBlock in self._pjrc.get(params = {'block': blockPath})['phedex']['block']:
			for replica in phedexBlock['replica']:
				if self._nodeFilter(replica['node'], replica['complete'] == 'y'):
					location = None
					if self._locationFormat == CMSLocationFormat.hostname:
						location = replica.get('se')
					elif self._locationFormat == CMSLocationFormat.siteDB:
						location = replica.get('node')
					elif (self._locationFormat == CMSLocationFormat.both) and (replica.get('node') or replica.get('se')):
						location = '%s/%s' % (replica.get('node'), replica.get('se'))
					if location:
						dictSE[blockPath].append(location)
					else:
						utils.vprint('Warning: Dataset block %s replica at %s / %s is skipped!' %
							(blockPath, replica.get('node'), replica.get('se')), -1)


	def getDatasets(self):
		if self._cache_dataset is None:
			self._cache_dataset = [self._datasetPath]
			if '*' in self._datasetPath:
				self._cache_dataset = list(self.getCMSDatasets(self._datasetPath))
				if not self._cache_dataset:
					raise DatasetError('No datasets selected by DBS wildcard %s !' % self._datasetPath)
		return self._cache_dataset


	def getCMSBlocks(self, datasetPath, getSites):
		iter_blockname_selist = self.getCMSBlocksImpl(datasetPath, getSites)
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


	def fillCMSFiles(self, block, blockPath):
		lumi_used = False
		lumiDict = {}
		if self._lumi_query: # central lumi query
			lumiDict = self.getCMSLumisImpl(blockPath)
		fileList = []
		for (fileInfo, listLumi) in self.getCMSFilesImpl(blockPath, self.onlyValid, self._lumi_query):
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


	def getCMSLumisImpl(self, blockPath):
		return None


	def getGCBlocks(self, usePhedex):
		for datasetPath in self.getDatasets():
			counter = 0
			for (blockPath, listSE) in self.getCMSBlocks(datasetPath, getSites = not usePhedex):
				result = {}
				result[DataProvider.Dataset] = blockPath.split('#')[0]
				result[DataProvider.BlockName] = blockPath.split('#')[1]

				if usePhedex: # Start parallel phedex query
					dictSE = {}
					tPhedex = start_thread('Query phedex site info for %s' % blockPath, self._getPhedexSEList, blockPath, dictSE)
					self.fillCMSFiles(result, blockPath)
					tPhedex.join()
					listSE = dictSE.get(blockPath)
				else:
					self.fillCMSFiles(result, blockPath)
				result[DataProvider.Locations] = listSE

				if len(result[DataProvider.FileList]):
					counter += 1
					yield result

			if counter == 0:
				raise DatasetError('Dataset %s does not contain any valid blocks!' % datasetPath)


class DBS2Provider(CMSBaseProvider):
	alias = ['dbs2']

	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		raise DatasetError('CMS deprecated all DBS2 Services in April 2014! Please use DBS3Provider instead.')
