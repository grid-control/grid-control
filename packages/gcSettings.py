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

class Settings(object):
	def __init__(self, section = None):
		self._s = section

	def section(self, section, name = '', **tags):
		tags_string = str.join(' ', map(lambda k: '%s:%s' % (k, tags[k]), tags))
		return Settings(('%s %s %s' % (section, name, tags_string)).strip())

	def set(self, name, value, override = False, append = False, force = False):
		if isinstance(value, list):
			value = str.join('\n', value)
		mod = dict([(override, '?'), (append, '+'), (force, '*')]).get(True, '')
		Settings._config.setdefault(self._s, []).append((name.replace('_', ' '), mod, value))

	def __getattribute__(self, name):
		if name in ['_s', 'section', 'set']:
			return object.__getattribute__(self, name)
		return self.section(name)

	def __setattr__(self, name, value):
		if name == '_s':
			return object.__setattr__(self, name, value)
		return self.set(name, value)

	def __str__(self):
		result = []
		for section in Settings._config:
			result.append('[%s]' % section)
			result.extend(map(lambda entry: '%s %s= %s' % entry, Settings._config[section]))
			result.append('')
		return str.join('\n', result)

	def getConfigDict(cls):
		result = {}
		for section in Settings._config:
			result[section] = dict(map(lambda (opt, mod, val): (opt + mod, str(val)), Settings._config[section]))
		return result
	getConfigDict = classmethod(getConfigDict)

Settings._config = {}
