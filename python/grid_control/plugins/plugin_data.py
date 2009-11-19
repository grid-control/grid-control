from plugin_base import *
from grid_control import utils
import tarfile, grid_control.datasets.splitter_base
DataSplitter = grid_control.datasets.splitter_base.DataSplitter

# Dataset splitting plugin with delayed and cached info access
class DatasetPlugin(IndexedParameter):
	def __init__(self, fileName):
		IndexedParameter.__init__(self, fileName)
		self.cacheKey = None
		self.fileName = fileName
		self._fmt = utils.DictFormat()
		self._tar = tarfile.open(fileName, 'r:')
		metadata = self._tar.extractfile('Metadata').readlines()
		self._metadata = self._fmt.parse(metadata, lowerCaseKey = False)

		self.setTransform(self.infoLoader)
		self.setDataTransform(varTransform(DataSplitter.Dataset, "DATASETPATH"))
		self.setDataTransform(varTransform(DataSplitter.DatasetID, "DATASETID"))
		self.setDataTransform(varTransform(DataSplitter.Nickname, "DATASETNICK"))

	# Loading dataset info files is expensive - cache last opened subfile
	def infoLoader(self, meta):
		(plugin, key, dummy, reqs) = meta
		if self.cacheKey != key / 100:
			self.cacheKey = key / 100
			subTarFileObj = self._tar.extractfile('%03dXX.tgz' % (key / 100))
			self._cacheTar = tarfile.open(mode = 'r:gz', fileobj = subTarFileObj)
		data = self._fmt.parse(self._cacheTar.extractfile('%05d/info' % key).readlines())
		list = self._cacheTar.extractfile('%05d/list' % key).readlines()
		data[DataSplitter.FileList] = map(str.strip, list)

		# Dataset requirements
		seList = data.get(DataSplitter.SEList)
		if seList != None:
			data.pop(DataSplitter.SEList)
			reqs.append((WMS.STORAGE, seList))
		return (plugin, key, data, reqs)

	# Speed up lookup:
	def getParameterNames(self):
		return []
	def getProcessedNames(self):
		return ["DATASETPATH", "DATASETID", "DATASETNICK"]

	# Access to data is delayed
	def getByIndex(self, key):
		if key >= int(self._metadata["MaxJobs"]):
			raise StopIteration
		return {}


class DatasetEventPlugin(DatasetPlugin):
	def __init__(self, fileName):
		DatasetPlugin.__init__(self, fileName)
		self.setDataTransform(varTransform(DataSplitter.NEvents, "MAX_EVENTS"))
		self.setDataTransform(varTransform(DataSplitter.Skipped, "SKIP_EVENTS"))

	def getProcessedNames(self):
		return DatasetPlugin.getProcessedNames(self) + ["MAX_EVENTS", "SKIP_EVENTS"]


# TODO: Use run / lumi section in file
class DatasetRunLumiPlugin(DatasetPlugin):
	pass
