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

from grid_control.datasets.dproc_base import DataProcessor
from grid_control.datasets.provider_base import DataProvider
from hpfwk import AbstractError

class NickNameProducer(DataProcessor):
	# Get nickname and check for collisions
	def processBlock(self, block):
		blockDS = block[DataProvider.Dataset]
		oldNick = block.get(DataProvider.Nickname, '')
		block[DataProvider.Nickname] = self.getName(oldNick, blockDS, block)
		return block

	# Overwritten by users / other implementations
	def getName(self, oldnick, dataset, block):
		raise AbstractError


class SimpleNickNameProducer(NickNameProducer):
	alias = ['simple']

	def __init__(self, config, onChange):
		NickNameProducer.__init__(self, config, onChange)
		self._full_name = config.getBool('nickname full name', True, onChange = onChange)

	def getName(self, oldnick, dataset, block):
		if oldnick == '':
			ds = dataset.replace('/PRIVATE/', '').lstrip('/').split('#')[0]
			if self._full_name:
				return ds.replace(' ', '').replace('/', '_').replace('__', '_')
			return ds.split('/')[0]
		return oldnick


class InlineNickNameProducer(NickNameProducer):
	alias = ['inline']

	def __init__(self, config, onChange):
		NickNameProducer.__init__(self, config, onChange)
		self._expr = config.get('nickname expr', 'oldnick', onChange = onChange)

	def getName(self, oldnick, dataset, block):
		return eval(self._expr) # pylint:disable=eval-used
