from plugin_base import ParameterPlugin, ParameterMetadata, ParameterInfo
from grid_control import utils, WMS
from grid_control.datasets import DataSplitter

class DataParaPlugin(ParameterPlugin):
	def __init__(self, dataSplitter, fnFormat, checkSE = True):
		ParameterPlugin.__init__(self)
		(self.dataSplitter, self.fnFormat, self.checkSE) = (dataSplitter, fnFormat, checkSE)

	def getMaxJobs(self):
		return self.dataSplitter.getMaxJobs()

	def getParameterNames(self, result):
		result.update([ParameterMetadata('DATASETSPLIT')] + map(lambda k: ParameterMetadata(k, transient=True),
			['FILE_NAMES', 'MAX_EVENTS', 'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK']))

	def getParameters(self, pNum, result):
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
		if self.checkSE:
			result[ParameterInfo.ACTIVE] &= splitInfo.get(DataSplitter.SEList) != []
