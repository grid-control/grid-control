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

from grid_control.config import triggerResync
from grid_control.datasets import DataProcessor, DataProvider, DataSplitter, DatasetError, PartitionProcessor
from grid_control.parameters import ParameterMetadata
from grid_control.utils.data_structures import makeEnum
from grid_control_cms.lumi_tools import filterLumiFilter, formatLumi, parseLumiFilter, selectLumi, selectRun, strLumi
from python_compat import any, ichain, imap, izip, set

LumiKeep = makeEnum(['RunLumi', 'Run', 'none'])
LumiMode = makeEnum(['strict', 'weak'])

def removeRunLumi(value, idxRuns, idxLumi):
	if (idxRuns is not None) and (idxLumi is not None):
		value.pop(max(idxRuns, idxLumi))
		value.pop(min(idxRuns, idxLumi))
	elif idxLumi is not None:
		value.pop(idxLumi)
	elif idxRuns is not None:
		value.pop(idxRuns)


class LumiDataProcessor(DataProcessor):
	alias = ['lumi']

	def __init__(self, config, onChange):
		DataProcessor.__init__(self, config, onChange)
		self._lumi_filter = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi, onChange = onChange)
		if self._lumi_filter.empty():
			lumi_keep_default = LumiKeep.none
		else:
			lumi_keep_default = LumiKeep.Run
			config.setBool('lumi metadata', True)
			self._log.info('Runs/lumi section filter enabled!')
		self._lumi_keep = config.getEnum('lumi keep', LumiKeep, lumi_keep_default, onChange = onChange)
		self._lumi_strict = config.getEnum('lumi filter strictness', LumiMode, LumiMode.strict, onChange = onChange)

	def _acceptRun(self, block, fi, idxRuns, lumi_filter):
		if idxRuns is None:
			return True
		return any(imap(lambda run: selectRun(run, lumi_filter), fi[DataProvider.Metadata][idxRuns]))

	def _acceptLumi(self, block, fi, idxRuns, idxLumi, lumi_filter):
		if (idxRuns is None) or (idxLumi is None):
			return True
		return any(imap(lambda run_lumi: selectLumi(run_lumi, lumi_filter),
			izip(fi[DataProvider.Metadata][idxRuns], fi[DataProvider.Metadata][idxLumi])))

	def _processFI(self, block, idxRuns, idxLumi):
		for fi in block[DataProvider.FileList]:
			if not self._lumi_filter.empty(): # Filter files by run / lumi
				lumi_filter = self._lumi_filter.lookup(block[DataProvider.Nickname], is_selector = False)
				if (self._lumi_strict == LumiMode.strict) and not self._acceptLumi(block, fi, idxRuns, idxLumi, lumi_filter):
					continue
				elif (self._lumi_strict == LumiMode.weak) and not self._acceptRun(block, fi, idxRuns, lumi_filter):
					continue
			# Prune metadata
			if (self._lumi_keep == LumiKeep.Run) and (idxLumi is not None):
				if idxRuns is not None:
					fi[DataProvider.Metadata][idxRuns] = list(set(fi[DataProvider.Metadata][idxRuns]))
				fi[DataProvider.Metadata].pop(idxLumi)
			elif self._lumi_keep == LumiKeep.none:
				removeRunLumi(fi[DataProvider.Metadata], idxRuns, idxLumi)
			yield fi

	def processBlock(self, block):
		if self._lumi_filter.empty() and ((self._lumi_keep == LumiKeep.RunLumi) or (DataProvider.Metadata not in block)):
			return block
		def getMetadataIdx(key):
			if key in block.get(DataProvider.Metadata, []):
				return block[DataProvider.Metadata].index(key)
		idxRuns = getMetadataIdx('Runs')
		idxLumi = getMetadataIdx('Lumi')
		if not self._lumi_filter.empty():
			lumi_filter = self._lumi_filter.lookup(block[DataProvider.Nickname], is_selector = False)
			if lumi_filter and (self._lumi_strict == LumiMode.strict) and ((idxRuns is None) or (idxLumi is None)):
				raise DatasetError('Strict lumi filter active but dataset %s does not provide lumi information!' % DataProvider.bName(block))
			elif lumi_filter and (self._lumi_strict == LumiMode.weak) and (idxRuns is None):
				raise DatasetError('Weak lumi filter active but dataset %s does not provide run information!' % DataProvider.bName(block))

		block[DataProvider.FileList] = list(self._processFI(block, idxRuns, idxLumi))
		if not block[DataProvider.FileList]:
			return
		block[DataProvider.NEntries] = sum(imap(lambda fi: fi[DataProvider.NEntries], block[DataProvider.FileList]))
		# Prune metadata
		if self._lumi_keep == LumiKeep.RunLumi:
			return block
		elif self._lumi_keep == LumiKeep.Run:
			idxRuns = None
		removeRunLumi(block[DataProvider.Metadata], idxRuns, idxLumi)
		return block


class LumiPartitionProcessor(PartitionProcessor):
	def __init__(self, config):
		PartitionProcessor.__init__(self, config)
		changeTrigger = triggerResync(['datasets', 'parameters'])
		self._lumi_filter = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi, onChange = changeTrigger)

	def getKeys(self):
		if self.enabled():
			return [ParameterMetadata('LUMI_RANGE', untracked = True)]

	def enabled(self):
		return not self._lumi_filter.empty()

	def getNeededKeys(self, splitter):
		if self.enabled():
			return ['LUMI_RANGE']

	def process(self, pNum, splitInfo, result):
		if self.enabled():
			lumi_filter = self._lumi_filter.lookup(splitInfo[DataSplitter.Nickname], is_selector = False)
			if lumi_filter:
				idxRuns = splitInfo[DataSplitter.MetadataHeader].index("Runs")
				iterRuns = ichain(imap(lambda m: m[idxRuns], splitInfo[DataSplitter.Metadata]))
				short_lumi_filter = filterLumiFilter(list(iterRuns), lumi_filter)
				result['LUMI_RANGE'] = str.join(',', imap(lambda lr: '"%s"' % lr, formatLumi(short_lumi_filter)))
