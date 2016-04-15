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

from hpfwk import APIError
from python_compat import imap, izip, lsmap, md5_hex, set

def makeEnum(members = None, cls = None, useHash = False):
	members = members or []
	if cls:
		enumID = md5_hex(str(members) + '!' + cls.__name__)[:4]
	else:
		enumID = md5_hex(str(members))[:4]
		cls = type('Enum_%s_%s' % (enumID, str.join('_', members)), (), {})

	def getValue(idx, name):
		if useHash:
			return idx + int(enumID, 16)
		else:
			return idx
	values = lsmap(getValue, enumerate(members))

	cls.enumNames = members
	cls.enumValues = values
	enumMapNV = dict(izip(imap(str.lower, cls.enumNames), cls.enumValues))
	enumMapVN = dict(izip(cls.enumValues, cls.enumNames))
	if len(enumMapNV) != len(enumMapVN):
		raise APIError('Invalid enum definition!')
	def str2enum(cls, value, *args, **kwargs):
		return enumMapNV.get(value.lower(), *args, **kwargs)
	cls.enum2str = enumMapVN.get
	cls.str2enum = classmethod(str2enum)
	for name, value in izip(cls.enumNames, cls.enumValues):
		setattr(cls, name, value)
	return cls


class UniqueList(object):
	def __init__(self, values = None, mode = 'first'):
		self._set = set()
		self._list = list()
		self._mode = mode
		self.extend(values or [])

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
