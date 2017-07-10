# | Copyright 2015-2017 Karlsruhe Institute of Technology
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
from grid_control.config import join_config_locations
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import prune_processors
from hpfwk import AbstractError, NestedException
from python_compat import imap


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
		self._disabled = False

	def __repr__(self):
		return self._repr_base()

	def disable_stream_singletons(self):
		pass

	def enabled(self):
		return self._enabled() and not self._disabled

	def must_complete_for_partition(self):
		return False

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
				error_msg = 'Error while processing dataset block in datasource %s'
				raise DataProcessorError(error_msg % repr(self._datasource_name))
		self._finished()

	def process_block(self, block):
		raise AbstractError

	def _enabled(self):
		return True

	def _finished(self):
		pass

	def _get_dproc_opt(self, *args):
		return join_config_locations(self._datasource_name, *args)


class MultiDataProcessor(DataProcessor):
	alias_list = ['multi']

	def __init__(self, config, processor_list, datasource_name):
		DataProcessor.__init__(self, config, datasource_name)
		self._do_prune = config.get_bool(self._get_dproc_opt('processor prune'), True)
		self._processor_list = prune_processors(self._do_prune, processor_list,
			self._log, 'Removed %d inactive dataset processors!')

	def __repr__(self):
		return str.join(' => ', imap(repr, self._processor_list))

	def disable_stream_singletons(self):
		for processor in self._processor_list:
			processor.disable_stream_singletons()
		self._processor_list = prune_processors(self._do_prune, self._processor_list,
			self._log, 'Removed %d singleton dataset processors!')

	def must_complete_for_partition(self):
		return True in imap(lambda dp: dp.must_complete_for_partition(), self._processor_list)

	def process(self, block_iter):
		for processor in self._processor_list:
			block_iter = processor.process(block_iter)
		return block_iter


class NullDataProcessor(DataProcessor):
	alias_list = ['null']

	def __init__(self, config=None, datasource_name=None):
		DataProcessor.__init__(self, config, datasource_name)

	def process_block(self, block):
		return block
