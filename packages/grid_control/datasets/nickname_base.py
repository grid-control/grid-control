#-#  Copyright 2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from grid_control import LoadableObject, AbstractError, DatasetError

class NickNameProducer(LoadableObject):
	def __init__(self, config):
		self.config = config
		# Ensure the same nickname is used consistently in all blocks of a dataset
		self._checkConsistency = config.getBool('nickname check consistency', True)
		self._checkConsistencyData = {}
		# Check if two different datasets have the same nickname
		self._checkCollision = config.getBool('nickname check collision', True)
		self._checkCollisionData = {}

	# Get nickname and check for collisions
	def process(self, block):
		from grid_control import DataProvider
		blockDS = block[DataProvider.Dataset]
		oldNick = block.get(DataProvider.Nickname, '')
		newNick = self.getName(oldNick, blockDS, block)
		if not (self._checkConsistency or self._checkCollision):
			return newNick # Skip checking for collisions if disabled
		# Check if nickname is used consistenly in all blocks of a datasets
		if self._checkConsistency:
			if self._checkConsistencyData.setdefault(blockDS, newNick) != newNick:
				raise DatasetError('Different blocks of dataset "%s" have different nicknames: "%s" != "%s"' % (
					blockDS, self._checkConsistencyData[blockDS], newNick))
		if self._checkCollision:
			if self._checkCollisionData.setdefault(newNick, blockDS) != blockDS:
				raise DatasetError('Multiple datasets use the same nickname "%s": "%s" != "%s"' % (
					newNick, self._checkCollisionData[newNick], blockDS))
		return newNick

	# Overwritten by users / other implementations
	def getName(self, oldnick, dataset, block):
		raise AbstractError
NickNameProducer.registerObject()


class SimpleNickNameProducer(NickNameProducer):
	def getName(self, oldnick, dataset, block):
		if oldnick == '':
			return dataset.replace('/PRIVATE/', '').lstrip('/').split('/')[0].split('#')[0]
		return oldnick


class InlineNickNameProducer(NickNameProducer):
	def getName(self, oldnick, dataset, block):
		return eval(self.config.get('nickname expr', 'oldnick'))
