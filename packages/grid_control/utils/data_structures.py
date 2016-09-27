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


def make_enum(members=None, cls=None, use_hash=True):
	members = members or []
	if cls:
		enum_id = md5_hex(str(members) + '!' + cls.__name__)[:4]
	else:
		enum_id = md5_hex(str(members))[:4]
		cls = type('Enum_%s_%s' % (enum_id, str.join('_', members)), (), {})

	def get_value(idx, name):
		if use_hash:
			return idx + int(enum_id, 16)
		else:
			return idx
	values = lsmap(get_value, enumerate(members))

	cls.enum_name_list = members
	cls.enum_value_list = values
	_map_name2value = dict(izip(imap(str.lower, cls.enum_name_list), cls.enum_value_list))
	_map_value2name = dict(izip(cls.enum_value_list, cls.enum_name_list))
	if len(_map_name2value) != len(_map_value2name):
		raise APIError('Invalid enum definition!')

	def str2enum(cls, value, *args):
		return _map_name2value.get(value.lower(), *args)
	cls.enum2str = _map_value2name.get
	cls.str2enum = classmethod(str2enum)
	for name, value in izip(cls.enum_name_list, cls.enum_value_list):
		setattr(cls, name, value)
	return cls


class UniqueList(object):
	def __init__(self, values=None):
		self._set = set()
		self._list = list()
		self.extend(values or [])

	def __contains__(self, value):
		return value in self._set

	def __iter__(self):
		return self._list.__iter__()

	def __repr__(self):
		return '<%s>' % repr(self._list)[1:-1]

	def append(self, value):
		if value not in self:
			self._set.add(value)
			self._list.append(value)

	def extend(self, values):
		for value in values:
			self.append(value)
