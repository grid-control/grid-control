from plugin_base import *
from grid_control import utils, WMS

class DataParaPlugin(ParameterPlugin):
	def __init__(self, dataSplitter, fnFormat):
		(self.dataSplitter, self.fnFormat) = (dataSplitter, fnFormat)

	def getMaxJobs(self):
		return self.dataSplitter.getMaxJobs()

	def getParameterNames(self):
		return (['DATASETSPLIT'], ['FILE_NAMES', 'MAX_EVENTS', 'SKIP_EVENTS', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])

	def getParameters(self, pNum, result):
		import grid_control.datasets.splitter_base
		DataSplitter = grid_control.datasets.splitter_base.DataSplitter
		splitInfo = self.dataSplitter.getSplitInfo(pNum)
		if utils.verbosity() > 0:
			utils.vprint('Dataset task number: %d' % pNum)
			DataSplitter.printInfoForJob(splitInfo)
		result.store['DATASETSPLIT'] = pNum
		result.transient.update({
			'FILE_NAMES': self.fnFormat(splitInfo[DataSplitter.FileList]),
			'MAX_EVENTS': splitInfo[DataSplitter.NEvents],
			'SKIP_EVENTS': splitInfo.get(DataSplitter.Skipped, 0),
			'DATASETID': splitInfo.get(DataSplitter.DatasetID, None),
			'DATASETPATH': splitInfo.get(DataSplitter.Dataset, None),
			'DATASETBLOCK': splitInfo.get(DataSplitter.BlockName, None),
			'DATASETNICK': splitInfo.get(DataSplitter.Nickname, None),
		})
		result.reqs.append((WMS.STORAGE, splitInfo.get(DataSplitter.SEList)))
