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
from hpfwk import AbstractError
from python_compat import imap, lchain


class PartitionProcessor(ConfigurablePlugin):
	# Class used by DataParameterSource to convert dataset splittings into parameter data
	def __init__(self, config, datasource_name):
		ConfigurablePlugin.__init__(self, config)
		self._datasource_name = datasource_name
		self._log = logging.getLogger('%s.partition.processor' % datasource_name)

	def __repr__(self):
		return self._repr_base()

	def enabled(self):
		return True

	def get_needed_vn_list(self, splitter):
		return []

	def get_partition_metadata(self):
		return []

	def process(self, pnum, partition_info, result):
		raise AbstractError

	def _get_pproc_opt(self, opt):
		return join_config_locations(['', self._datasource_name], 'partition', opt)


class MultiPartitionProcessor(PartitionProcessor):
	alias_list = ['multi']

	def __init__(self, config, processor_list, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		do_prune = config.get_bool(self._get_pproc_opt('processor prune'), True)
		self._processor_list = prune_processors(do_prune, processor_list,
			self._log, 'Removed %d inactive partition processors!')

	def __repr__(self):
		return str.join(' => ', imap(repr, self._processor_list))

	def get_needed_vn_list(self, splitter):
		return lchain(imap(lambda p: p.get_needed_vn_list(splitter) or [], self._processor_list))

	def get_partition_metadata(self):
		return lchain(imap(lambda p: p.get_partition_metadata() or [], self._processor_list))

	def process(self, pnum, partition_info, result):
		for processor in self._processor_list:
			processor.process(pnum, partition_info, result)
