#-#  Copyright 2011 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

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
