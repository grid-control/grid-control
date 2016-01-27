#-#  Copyright 2015-2016 Karlsruhe Institute of Technology
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

import logging
from hpfwk import InstanceFactory, Plugin

class DataProcessor(Plugin):
	def __init__(self, config):
		self._log = logging.getLogger('dataproc')

	def process(self, blockIter):
		for block in blockIter:
			yield self.processBlock(block)

	def processBlock(self, block):
		raise AbstractError

	def bind(cls, value, modulePaths = [], config = None, **kwargs):
		for entry in value.split():
			yield InstanceFactory(entry, cls.getClass(entry, modulePaths), config)
	bind = classmethod(bind)


class MultiDataProcessor(DataProcessor):
	def __init__(self, config, processorProxyList):
		DataProcessor.__init__(self, config)
		self._processorList = map(lambda p: p.getInstance(), processorProxyList)

	def process(self, blockIter):
		for processor in self._processorList:
			blockIter = processor.process(blockIter)
		return blockIter

	def processBlock(self, block):
		for processor in self._processorList:
			block = processor.processBlock(block)
			if not block:
				break
		return block
