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

from grid_control.gc_plugin import NamedPlugin
from grid_control.utils import prune_processors
from hpfwk import AbstractError
from python_compat import imap


class Broker(NamedPlugin):
	config_section_list = NamedPlugin.config_section_list + ['broker']
	config_tag_name = 'broker'

	def __init__(self, config, name, broker_prefix, **kwargs):
		NamedPlugin.__init__(self, config, name)

	def __repr__(self):
		return self._repr_base()

	def enabled(self):
		return True

	def process(self, req_list):
		raise AbstractError


class MultiBroker(Broker):
	alias_list = ['multi']

	def __init__(self, config, name, broker_list, broker_prefix, **kwargs):
		Broker.__init__(self, config, name, broker_prefix)
		tmp_broker_list = []
		for broker in broker_list:
			if isinstance(broker, MultiBroker):
				tmp_broker_list.extend(broker.get_broker_list())
			else:
				tmp_broker_list.append(broker)
		self._do_prune = config.get_bool('%s broker prune' % broker_prefix, True)
		self._broker_list = prune_processors(self._do_prune, tmp_broker_list,
			self._log, 'Removed %d inactive ' + broker_prefix + ' brokers!')

	def __repr__(self):
		return str.join(' => ', imap(repr, self._broker_list))

	def enabled(self):
		return True

	def get_broker_list(self):
		return self._broker_list

	def process(self, req_list):
		for broker in self._broker_list:
			req_list = broker.process(req_list)
		return list(req_list)


class NullBroker(Broker):
	alias_list = ['null']

	def enabled(self):
		return False

	def process(self, req_list):
		return req_list
