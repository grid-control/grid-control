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
from grid_control.config import triggerResync
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import display_selection
from hpfwk import AbstractError

class DataProcessor(ConfigurablePlugin):
	triggerDataResync = triggerResync(['datasets', 'parameters'])

	def __init__(self, config):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('dataproc')

	def enabled(self):
		return True

	def process(self, blockIter):
		for block in blockIter:
			result = self.processBlock(block)
			if result is not None:
				yield result

	def processBlock(self, block):
		raise AbstractError


class MultiDataProcessor(DataProcessor):
	def __init__(self, config, processorList):
		DataProcessor.__init__(self, config)
		(self._processorList, processorNames) = ([], [])
		for proc in processorList:
			if proc.enabled() and proc.__class__.__name__ not in processorNames:
				self._processorList.append(proc)
				processorNames.append(proc.__class__.__name__)
		display_selection(self._log, processorList, self._processorList,
			'Removed %d inactive dataset processors!', lambda item: item.__class__.__name__)

	def process(self, blockIter):
		for processor in self._processorList:
			blockIter = processor.process(blockIter)
		return blockIter


class NullDataProcessor(DataProcessor):
	alias = ['null']

	def processBlock(self, block):
		return block
