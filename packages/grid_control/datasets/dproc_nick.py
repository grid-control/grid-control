# | Copyright 2014-2016 Karlsruhe Institute of Technology
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

from grid_control.config import join_config_locations
from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider
from hpfwk import AbstractError


class NickNameProducer(DataProcessor):
	def get_name(self, current_nickname, dataset, block):
		raise AbstractError  # Overwritten by users / other implementations

	def process_block(self, block):  # Get nickname and check for collisions
		dataset = block[DataProvider.Dataset]
		current_nickname = block.get(DataProvider.Nickname, '')
		# legacy API
		for api in ['getName', 'get_name']:
			if hasattr(self, api):
				block[DataProvider.Nickname] = getattr(self, api)(current_nickname, dataset, block)
				break
		return block


class InlineNickNameProducer(NickNameProducer):
	alias_list = ['inline']

	def __init__(self, config, datasource_name):
		NickNameProducer.__init__(self, config, datasource_name)
		self._expr = config.get(
			join_config_locations(['', datasource_name], 'nickname expr'), 'current_nickname')

	def get_name(self, current_nickname, dataset, block):
		return eval(self._expr, {  # pylint:disable=eval-used
			'oldnick': current_nickname, 'current_nickname': current_nickname,
			'dataset': dataset, 'block': block, 'DataProvider': DataProvider})


class SimpleNickNameProducer(NickNameProducer):
	alias_list = ['simple']

	def __init__(self, config, datasource_name):
		NickNameProducer.__init__(self, config, datasource_name)
		self._full_name = config.get_bool(
			join_config_locations(['', datasource_name], 'nickname full name'), True)

	def get_name(self, current_nickname, dataset_name, block):
		if current_nickname == '':
			dataset_name = dataset_name.replace('/PRIVATE/', '').lstrip('/').split('#')[0]
			if self._full_name:
				return dataset_name.replace(' ', '').replace('/', '_').replace('__', '_')
			return dataset_name.split('/')[0]
		return current_nickname
