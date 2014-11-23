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

from python_compat import set

class UniqueList(object):
	def __init__(self, values = [], mode = 'first'):
		self._set = set()
		self._list = list()
		self._mode = mode
		for value in values:
			self.append(value)

	def __repr__(self):
		return '{%s}' % repr(self._list).lstrip('[').rstrip(']')

	def __contains__(self, value):
		return value in self._set

	def __iter__(self):
		return self._list.__iter__()

	def append(self, value):
		if value not in self:
			self._set.add(value)
			self._list.append(value)
		elif self._mode == 'last':
			self._list.remove(value)
			self._list.append(value)

	def extend(self, values):
		for value in values:
			self.append(value)

	def remove(self, value):
		self._set.remove(value)
		self._list.remove(value)
