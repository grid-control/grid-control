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
from grid_control.utils import prune_processors
from hpfwk import AbstractError

class DataProcessor(ConfigurablePlugin):
	def __init__(self, config, onChange):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('dataset.provider.processor')
		self._log_debug = None
		if self._log.isEnabledFor(logging.DEBUG):
			self._log_debug = self._log

	def enabled(self):
		return True

	def process(self, blockIter):
		for block in blockIter:
			if self._log_debug:
				self._log_debug.debug('%s is processing block %s...' % (self, repr(block)))
			result = self.processBlock(block)
			if result is not None:
				yield result
			if self._log_debug:
				self._log_debug.debug('%s process result: %s' % (self, repr(result)))
		self._finished()

	def processBlock(self, block):
		raise AbstractError

	def _finished(self):
		pass


class MultiDataProcessor(DataProcessor):
	def __init__(self, config, processorList, onChange):
		DataProcessor.__init__(self, config, onChange)
		do_prune = config.getBool('dataset processor prune', True, onChange = onChange)
		self._processorList = prune_processors(do_prune, processorList, self._log, 'Removed %d inactive dataset processors!')

	def process(self, blockIter):
		for processor in self._processorList:
			blockIter = processor.process(blockIter)
		return blockIter


class NullDataProcessor(DataProcessor):
	alias = ['null']

	def processBlock(self, block):
		return block
