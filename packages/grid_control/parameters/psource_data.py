from psource_base import ParameterSource, ParameterMetadata, ParameterInfo
from grid_control import utils, WMS
from grid_control.datasets import DataSplitter

class DataParameterSource(ParameterSource):
	def __init__(self, dataSplitter, fnFormat, checkSE = True):
		ParameterSource.__init__(self)
		(self.dataSplitter, self.fnFormat, self.checkSE) = (dataSplitter, fnFormat, checkSE)

	def getMaxJobs(self):
		return self.dataSplitter.getMaxJobs()

	def fillParameterKeys(self, result):
		result.extend(
			[ParameterMetadata('DATASETSPLIT')] + map(lambda k: ParameterMetadata(k, untracked=True),
			['FILE_NAMES', 'MAX_EVENTS', 'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK']))

	def fillParameterInfo(self, pNum, result):
		splitInfo = self.dataSplitter.getSplitInfo(pNum)
		if utils.verbosity() > 0:
			utils.vprint('Dataset task number: %d' % pNum)
			DataSplitter.printInfoForJob(splitInfo)
		result.update({
			'FILE_NAMES': self.fnFormat(splitInfo[DataSplitter.FileList]),
			'MAX_EVENTS': splitInfo[DataSplitter.NEvents],
			'SKIP_EVENTS': splitInfo.get(DataSplitter.Skipped, 0),
			'DATASETID': splitInfo.get(DataSplitter.DatasetID, None),
			'DATASETPATH': splitInfo.get(DataSplitter.Dataset, None),
			'DATASETBLOCK': splitInfo.get(DataSplitter.BlockName, None),
			'DATASETNICK': splitInfo.get(DataSplitter.Nickname, None),
			'DATASETSPLIT': pNum,
		})
		result[ParameterInfo.REQS].append((WMS.STORAGE, splitInfo.get(DataSplitter.SEList)))
		result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and not splitInfo.get(DataSplitter.Invalid, False)
		if self.checkSE:
			result[ParameterInfo.ACTIVE] = result[ParameterInfo.ACTIVE] and (splitInfo.get(DataSplitter.SEList) != [])

	def create(cls, pconfig = None, src = 0):
		if src not in DataParameterSource.datasets:
			DataParameterSource.datasets[src] = {'obj': DataSplitter.loadState(src), 'fun': lambda fl: str.join(', ', fl)}
		dsCfg = DataParameterSource.datasets.get(src)
		DataParameterSource.datasetSources.append(DataParameterSource(dsCfg['obj'], dsCfg['fun']))
		return DataParameterSource.datasetSources[-1]
	create = classmethod(create)
DataParameterSource.datasets = {}
DataParameterSource.datasetSources = []
ParameterSource.managerMap['data'] = DataParameterSource
