# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import random
from grid_control.gc_plugin import NamedPlugin


class Broker(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['broker']
	config_tag_name = 'broker'

	def __init__(self, config, name, broker_prefix, item_name, discover_fun):
		NamedPlugin.__init__(self, config, name)
		(self._item_list_start, self._item_list_discovered, self._item_name) = (None, False, item_name)
		self._num_entries = config.get_int('%s entries' % broker_prefix, 0, on_change=None)
		self._randomize = config.get_bool('%s randomize' % broker_prefix, False, on_change=None)

	def broker(self, reqs, req_enum):
		result = self._broker(reqs, self._item_list_start)
		if result is not None:
			reqs.append((req_enum, result))
		return reqs

	def _broker(self, reqs, items):
		if items and self._randomize:
			return random.sample(items, self._num_entries or len(items))
		elif items and self._num_entries:
			return items[:self._num_entries]
		return items

	def _discover(self, discover_fun, cached=True):
		if not cached or (self._item_list_discovered is False):
			self._item_list_discovered = discover_fun()
			msg = 'an unknown number of'
			if self._item_list_discovered is not None:
				msg = str(len(self._item_list_discovered))
			self._log.info('Broker discovered %s %s', msg, self._item_name)
		return self._item_list_discovered
