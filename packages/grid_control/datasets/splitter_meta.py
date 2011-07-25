from grid_control import AbstractError
from splitter_base import DataSplitter
from provider_base import DataProvider

# Split dataset along block and metadata boundaries - using equivalence classes of metadata
class MetadataSplitter(DataSplitter):
	def metaCmp(self, md, fiA, fiB):
		raise AbstractError

	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			files = block[DataProvider.FileList]
			files.sort(lambda a, b: self.metaCmp(block[DataProvider.Metadata], a, b))
			(fileStack, reprElement) = ([], None)
			for fi in files:
				if reprElement == None:
					reprElement = fi
				if self.metaCmp(block[DataProvider.Metadata], fi, reprElement) != 0:
					yield self.finaliseJobSplitting(block, dict(), fileStack)
					(fileStack, reprElement) = ([], fi)
				fileStack.append(fi)
			yield self.finaliseJobSplitting(block, dict(), fileStack)


class RunSplitter(MetadataSplitter):
	def metaCmp(self, md, fiA, fiB):
		mdIdx = md.index('Runs')
		return cmp(fiA[DataProvider.Metadata][mdIdx], fiB[DataProvider.Metadata][mdIdx])
