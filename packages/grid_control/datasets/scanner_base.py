# | Copyright 2012-2017 Karlsruhe Institute of Technology
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


class InfoScanner(ConfigurablePlugin):
	def __init__(self, config, datasource_name):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('%s.provider.infoscanner' % datasource_name)
		self._datasource_name = datasource_name

	def get_guard_keysets(self):
		return ([], [])

	def iter_datasource_items(self, depth, item, metadata_dict, entries, location_list, obj_dict):
		if self._log.isEnabledFor(logging.DEBUG):
			self._log.log(logging.DEBUG, '    ' * depth +
				'Collecting information with %s...', self.__class__.__name__)
			logging_info_list = [
				(logging.DEBUG, item, 'Item'),
				(logging.DEBUG1, metadata_dict, 'Metadata'),
				(logging.DEBUG, entries, 'entries'),
				(logging.DEBUG1, location_list, 'SE list'),
				(logging.DEBUG1, obj_dict, 'Objects')
			]
			for (level, content, name) in logging_info_list:
				self._log.log(level, '    ' * depth + '  %s: %s', name, content)
		return self._iter_datasource_items(item, metadata_dict, entries, location_list, obj_dict)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		raise AbstractError


class NullScanner(InfoScanner):
	alias_list = ['null']

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		yield (item, metadata_dict, entries, location_list, obj_dict)
