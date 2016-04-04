# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control.datasets import DataProcessor, DataProvider, DataSplitter, DatasetError, PartitionProcessor
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.gc_itertools import ichain
from grid_control_cms.lumi_tools import filterLumiFilter, formatLumi, parseLumiFilter, selectLumi, strLumi
from python_compat import imap, izip, set

LumiKeep = makeEnum(['RunLumi', 'Run', 'none'])

def removeRunLumi(value, idxRuns, idxLumi):
	if (idxRuns is not None) and (idxLumi is not None):
		value.pop(max(idxRuns, idxLumi))
		value.pop(min(idxRuns, idxLumi))
	elif idxLumi is not None:
		value.pop(idxLumi)
	elif idxRuns is not None:
		value.pop(idxRuns)


class LumiDataProcessor(DataProcessor):
	def __init__(self, config):
		DataProcessor.__init__(self, config)
		self._lumi_filter = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi)
		if self._lumi_filter.empty():
			lumi_keep_default = LumiKeep.none
		else:
			lumi_keep_default = LumiKeep.Run
			config.setBool('lumi metadata', True)
			self._log.warning('Runs/lumi section filter enabled!')
		self._lumi_keep = config.getEnum('lumi keep', LumiKeep, lumi_keep_default)
		self._lumi_strict = config.getBool('strict lumi filter', True)

	def _acceptLumi(self, block, fi, idxRuns, idxLumi):
		if (idxRuns is None) or (idxLumi is None):
			return True
		fi_meta = fi[DataProvider.Metadata]
		for (run, lumi) in izip(fi_meta[idxRuns], fi_meta[idxLumi]):
			if selectLumi((run, lumi), self._lumi_filter.lookup(block[DataProvider.Nickname])):
				return True

	def _processFI(self, block, idxRuns, idxLumi):
		for fi in block[DataProvider.FileList]:
			if (not self._lumi_filter.empty()) and not self._acceptLumi(block, fi, idxRuns, idxLumi):
				continue
			if (self._lumi_keep == LumiKeep.Run) and (idxLumi is not None):
				if idxRuns is not None:
					fi[DataProvider.Metadata][idxRuns] = list(set(fi[DataProvider.Metadata][idxRuns]))
				fi[DataProvider.Metadata].pop(idxLumi)
			elif self._lumi_keep == LumiKeep.none:
				removeRunLumi(fi[DataProvider.Metadata], idxRuns, idxLumi)
			yield fi

	def processBlock(self, block):
		if (not self._lumi_filter) and (self._lumi_keep == LumiKeep.RunLumi):
			return block
		def getMetadataIdx(key):
			if key in block[DataProvider.Metadata]:
				return block[DataProvider.Metadata].index(key)
		idxRuns = getMetadataIdx('Runs')
		idxLumi = getMetadataIdx('Lumi')
		if self._lumi_filter and ((idxRuns is None) or (idxLumi is None)) and self._lumi_strict:
			raise DatasetError('Lumi filter active but data source does not provide lumi information!')

		block[DataProvider.FileList] = list(self._processFI(block, idxRuns, idxLumi))
		if not block[DataProvider.FileList]:
			return
		block[DataProvider.NEntries] = sum(imap(lambda fi: fi[DataProvider.NEntries], block[DataProvider.FileList]))
		if self._lumi_keep == LumiKeep.RunLumi:
			return block
		elif self._lumi_keep == LumiKeep.Run:
			if idxLumi is not None:
				block[DataProvider.Metadata].pop(idxLumi)
			return block
		removeRunLumi(block[DataProvider.Metadata], idxRuns, idxLumi)
		return block


class LumiPartitionProcessor(PartitionProcessor):
	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		self._lumi_filter = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi)

	def getNeededKeys(self, splitter):
		if self._lumi_filter.empty():
			return []
		return ['LUMI_RANGE']

	def process(self, pNum, splitInfo, result):
		if not self._lumi_filter.empty():
			idxRuns = splitInfo[DataSplitter.MetadataHeader].index("Runs")
			iterRuns = ichain(imap(lambda m: m[idxRuns], splitInfo[DataSplitter.Metadata]))
			lumi_filter = filterLumiFilter(list(iterRuns), self._lumi_filter.lookup(splitInfo[DataSplitter.Nickname]))
			result['LUMI_RANGE'] = str.join(',', imap(lambda lr: '"%s"' % lr, formatLumi(lumi_filter)))
