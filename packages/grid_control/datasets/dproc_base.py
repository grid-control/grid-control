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
from hpfwk import AbstractError, NestedException


class DataProcessorError(NestedException):
	pass


class DataProcessor(ConfigurablePlugin):
	def __init__(self, config, datasource_name):
		ConfigurablePlugin.__init__(self, config)
		self._datasource_name = datasource_name
		self._log = logging.getLogger('%s.provider.processor' % datasource_name)
		self._log_debug = None
		if self._log.isEnabledFor(logging.DEBUG):
			self._log_debug = self._log

	def enabled(self):
		return True

	def process(self, block_iter):
		for block in block_iter:
			try:
				if self._log_debug:
					self._log_debug.debug('%s is processing block %s...' % (self, repr(block)))
				result = self.process_block(block)
				if result is not None:
					yield result
				if self._log_debug:
					self._log_debug.debug('%s process result: %s' % (self, repr(result)))
			except Exception:
				from grid_control.datasets.provider_base import DataProvider
				block_id = DataProvider.get_block_id(block)
				raise DataProcessorError('Error while processing dataset block %s' % block_id)
		self._finished()

	def process_block(self, block):
		raise AbstractError

	def _finished(self):
		pass


class MultiDataProcessor(DataProcessor):
	def __init__(self, config, processor_list, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		do_prune = config.get_bool('%s processor prune' % datasource_name, True)
		self._processor_list = prune_processors(do_prune, processor_list,
			self._log, 'Removed %d inactive dataset processors!')

	def process(self, block_iter):
		for processor in self._processor_list:
			block_iter = processor.process(block_iter)
		return block_iter


class NullDataProcessor(DataProcessor):
	alias_list = ['null']

	def __init__(self, config=None, datasource_name=None):
		DataProcessor.__init__(self, config, datasource_name)

	def __repr__(self):
		return '%s()' % self.__class__.__name__

	def process_block(self, block):
		return block
