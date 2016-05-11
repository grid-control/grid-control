# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import logging
from grid_control.gc_plugin import ConfigurablePlugin
from hpfwk import AbstractError
from python_compat import lfilter

class DataProcessor(ConfigurablePlugin):
	def __init__(self, config):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('dataproc')

	def enabled(self):
		return True

	def process(self, blockIter):
		for block in blockIter:
			if block is not None:
				yield self.processBlock(block)

	def processBlock(self, block):
		raise AbstractError


class MultiDataProcessor(DataProcessor):
	def __init__(self, config, processorList):
		DataProcessor.__init__(self, config)
		self._processorList = lfilter(lambda proc: proc.enabled(), processorList)
		if len(self._processorList) != len(processorList):
			self._log.log(logging.DEBUG, 'Removed %d disabled dataset processors!' % (len(processorList) - len(self._processorList)))
			for processor in processorList:
				self._log.log(logging.DEBUG1, ' %s %s' % ({True: '*', False: ' '}[processor.enabled()], processor.__class__.__name__))

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


class NullDataProcessor(DataProcessor):
	alias = ['null']

	def processBlock(self, block):
		return block
