#-#  Copyright 2014-2015 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.exceptions import APIError, NestedException
from python_compat import set, sorted

class ConfigError(NestedException):
	pass

# Placeholder to specify a non-existing default
noDefault = utils.makeEnum(['noDefault'])

# return canonized section or option string
def standardConfigForm(value):
	if value != None:
		if not isinstance(value, list):
			value = [value]
		return map(lambda x: str(x).strip().lower(), value)

def multi_line_format(value):
	value_list = filter(lambda x: x != '', map(str.strip, value.strip().splitlines()))
	if len(value_list) > 1:
		return '\n\t%s' % str.join('\n\t', value_list)
	return str.join('\n\t', value_list)


# Holder of config information
class ConfigEntry(object):
	def __init__(self, section, option, value, opttype, source, order = None, accessed = False):
		(self.section, self.option, self.source, self.order) = (section.lower(), option.lower(), source.lower(), order)
		(self.value, self.opttype, self.accessed) = (value, opttype, accessed)

	def __repr__(self):
		varList = str.join(', ', map(lambda (k, v): '%s = %s' % (k, repr(v)), sorted(self.__dict__.items())))
		return '%s(%s)' % (self.__class__.__name__, varList)

	def format_opt(self):
		if '!' in self.section:
			return '<%s> %s' % (self.section.replace('!', ''), self.option)
		return '[%s] %s' % (self.section, self.option)

	def format(self, printSection = False, printDefault = False, default = noDefault):
		if (self.value == noDefault) or (not printDefault and (self.value == default)):
			return ''
		result = '%s %s' % (self.opttype, multi_line_format(self.value))
		if printSection:
			return '%s %s' % (self.format_opt(), result)
		return '%s %s' % (self.option, result)

	def processEntries(cls, entryList):
		result = None
		used = []
		for idx, entry in enumerate(entryList):
			def mkNew(value):
				return ConfigEntry(entry.section, entry.option, value, '=', '<processed>')
			if entry.opttype == '=': # Normal, overwriting setting
				used = [entry]
				result = entry
			elif entry.opttype == '?=': # Conditional setting
				if not result:
					used = [entry]
					result = entry
			elif entry.opttype == '*=': # this option can not be changed by other config entries
				# TODO: notify that subsequent config options will be ignored
				entry.accessed = True
				return (entry, [entry])
			elif entry.opttype == '+=':
				used.append(entry)
				if not result: # Use value if currently nothing is set
					result = entry
				else: # Appending value to current value
					result = mkNew(result.value + '\n' + entry.value)
			elif entry.opttype == '^=':
				used.append(entry)
				if not result: # Use value if currently nothing is set
					result = entry
				else: # Prepending value to current value
					result = mkNew(entry.value + '\n' + result.value)
			elif entry.opttype == '-=':
				if entry.value.strip() == '': # without arguments: replace by default
					used = [entry]
					result = None
				elif result: # with arguments: remove string from current value
					used.append(entry)
					result = mkNew(result.replace(entry.value.strip(), ''))
				else:
					raise ConfigError('Unable to substract "%s" from non-existing value!' % entry.format_opt())
		return (result, used)
	processEntries = classmethod(processEntries)

	def simplifyEntries(cls, entryList):
		(result, used) = cls.processEntries(entryList)
		if used[0].opttype == '=':
			return [cls.combineEntries(used)]
		# Merge subsequent += and ^= entries
		def mergeSubsequent(entries):
			previousEntry = None
			for entry in entries:
				if previousEntry and (entry.opttype == previousEntry.opttype):
					if entry.opttype == '+=':
						entry.value = previousEntry.value + '\n' + entry.value
						entry.source = '<processed>'
					elif entry.opttype == '^=':
						entry.value = entry.value + '\n' + previousEntry.value
						entry.source = '<processed>'
					else:
						yield previousEntry
				previousEntry = entry
			if previousEntry:
				yield previousEntry
		return list(mergeSubsequent(used))
	simplifyEntries = classmethod(simplifyEntries)

	def combineEntries(cls, entryList):
		(result, used) = cls.processEntries(entryList)
		for entry in used:
			entry.accessed = True
		return result
	combineEntries = classmethod(combineEntries)


class ConfigContainer(object):
	def __init__(self, name):
		self.enabled = True
		self._read_only = False
		self._counter = 0
		self._content = {}
		self._content_default = {}

	def setReadOnly(self):
		self._read_only = True

	def getDefault(self, entry):
		return self._content_default.get(entry.option, {}).get(entry.section)

	def setDefault(self, entry):
		curEntry = self.getDefault(entry)
		if self._read_only and not curEntry:
			raise APIError('Config container is read-only!')
		elif curEntry and (curEntry.value != entry.value):
			raise APIError('Inconsistent default values! (%r != %r)' % (curEntry.value, entry.value))
		entry.order = 0
		self._content_default.setdefault(entry.option, {}).setdefault(entry.section, entry)

	def append(self, entry):
		if self._counter == None:
			raise APIError('Config container is read-only!')
		self._counter += 1
		entry.order = self._counter
		self._content.setdefault(entry.option, []).append(entry)

	def getEntries(self, option):
		return self._content.get(option, []) + self._content_default.get(option, {}).values()

	def getKeys(self):
		return set(self._content.keys() + self._content_default.keys())
