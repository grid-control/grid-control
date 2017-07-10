# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

from hpfwk import APIError, ignore_exception
from python_compat import imap, md5_hex, set, unspecified


def make_enum(enum_name_list=None, cls=None, use_hash=True, register=True):
	enum_name_list = enum_name_list or []
	if cls:
		enum_id = md5_hex(str(enum_name_list) + '!' + cls.__name__)[:4]
	else:
		enum_id = md5_hex(str(enum_name_list))[:4]
		cls = type('Enum_%s_%s' % (enum_id, str.join('_', enum_name_list)), (), {})

	def _intstr2enum(cls, value, default=unspecified):
		enum = ignore_exception(Exception, default, int, value)
		if enum not in cls.enum_value_list:
			allowed_str = str.join(', ', imap(lambda nv: '%s=%s', _map_name2value.items()))
			raise Exception('Invalid enum value %s (allowed are %r)' % (repr(value), allowed_str))
		return enum

	def _register_enum(cls, name):
		value = len(cls.enum_name_list)
		if use_hash:
			value += int(enum_id, 16)
		for enum_cls in make_enum.enum_list:
			if use_hash and (value in enum_cls.enum_value_list) and (enum_cls.enum_id != enum_id):
				raise APIError('enum value collision detected!')
		cls.enum_name_list.append(name)
		cls.enum_value_list.append(value)
		setattr(cls, name, value)
		_map_name2value[name.lower()] = value
		_map_value2name[value] = name
		if len(_map_name2value) != len(_map_value2name):
			raise APIError('Invalid enum definition! (%s:%s)' % (_map_name2value, _map_value2name))

	def _str2enum(cls, value, *args):
		lookup_fun = _map_name2value.__getitem__
		if args:
			lookup_fun = _map_name2value.get
		try:
			return lookup_fun(value.lower(), *args)
		except Exception:
			allowed_str = str.join(', ', cls.enum_name_list)
			raise Exception('Invalid enum string %s (allowed are %r)' % (repr(value), allowed_str))

	_map_value2name = {}
	_map_name2value = {}
	cls.enum_id = enum_id
	cls.enum_name_list = []
	cls.enum_value_list = []
	cls.enum2str = _map_value2name.get
	cls.str2enum = classmethod(_str2enum)
	cls.intstr2enum = classmethod(_intstr2enum)
	cls.register_enum = classmethod(_register_enum)

	for enum_name in enum_name_list:
		cls.register_enum(enum_name)
	if register:
		make_enum.enum_list.append(cls)
	return cls
make_enum.enum_list = []  # <global-state>


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
