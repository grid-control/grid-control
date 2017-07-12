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

# This class is used by the grid-control python config file parser


class Settings(object):
	_config = {}

	def __init__(self, section=None):
		self._section = section

	def __getattribute__(self, name):
		if name in ['_section', 'section', 'set', 'get_config_dict', 'getConfigDict']:
			return object.__getattribute__(self, name)
		return self.section(name)

	def __setattr__(self, name, value):
		if name == '_section':
			return object.__setattr__(self, name, value)
		return self.set(name, value)

	def __str__(self):
		result = []
		sections = list(Settings._config.keys())  # manual sort for older python versions
		sections.sort()
		for section in sections:
			result.append('[%s]' % section)
			for entry in Settings._config[section]:
				result.append('%s %s= %s' % entry)
			result.append('')
		return str.join('\n', result)

	def get_config_dict(cls):
		result = {}
		for section in Settings._config:
			result[section] = {}
			for (opt, mod, val) in Settings._config[section]:
				result[section][opt + mod] = str(val)
		return result
	get_config_dict = classmethod(get_config_dict)
	getConfigDict = get_config_dict  # <alias>

	def section(self, section, name='', **tags):
		section_parts = [section]
		if name:
			section_parts.append(name)
		for key in tags:
			section_parts.append('%s:%s' % (key, tags[key]))
		return Settings(str.join(' ', section_parts))

	def set(self, name, value, override=False, append=False, force=False):
		if isinstance(value, list):
			def _iter_values():
				for item in value:
					yield str(item)
			value = str.join('\n\t', _iter_values())
		mod = dict([(override, '?'), (append, '+'), (force, '*')]).get(True, '')
		Settings._config.setdefault(self._section, []).append((name.replace('_', ' '), mod, value))
