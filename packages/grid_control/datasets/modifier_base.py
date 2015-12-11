#-#  Copyright 2015 Karlsruhe Institute of Technology
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

import logging
from grid_control.abstract import NamedObject

class DatasetModifier(NamedObject):
	tagName = 'dmod'

	def __init__(self, config, name):
		self._log = logging.getLogger(self.tagName)

	def processBlock(self, block):
		raise AbstractError


class MultiDataModifier(DatasetModifier):
	def __init__(self, config, name, modifierProxyList):
		DatasetModifier.__init__(self, config, name)
		self._modifierList = map(lambda p: p.getInstance(), modifierProxyList)

	def processBlock(self, block):
		for modifier in self._modifierList:
			block = modifier.processBlock(block)
			if not block:
				break
		return block
