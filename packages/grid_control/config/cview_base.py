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

import logging
from grid_control.config.config_entry import ConfigEntry, ConfigError, noDefault, standardConfigForm
from grid_control.utils.data_structures import makeEnum
from grid_control.utils.gc_itertools import ichain
from hpfwk import AbstractError, Plugin
from python_compat import imap, lfilter, sorted

selectorUnchanged = makeEnum(['selector_unchanged'])

class ConfigView(Plugin):
	def __init__(self, name, parent = None):
		if not parent:
			parent = self
		self._parent = parent
		self.pathDict = {}
		self.pathDict.update(parent.pathDict) # inherit path dict from parent
		self.setConfigName(name)

	def setConfigName(self, name):
		self.configName = name
		self._log = logging.getLogger('config.%s' % name.lower())

	def getView(self, setSections = selectorUnchanged, **kwargs):
		raise AbstractError

	def iterContent(self):
		raise AbstractError

	# Return old and current merged config entries
	def get(self, option_list, default_str, persistent):
		raise AbstractError

	def set(self, option, value, opttype, source):
		raise AbstractError

	def _get_write_entries(self):
		return self.iterContent()

	def _prepare_write(self, entries = None, printState = False, printUnused = True, printDefault = True):
		entries = entries or self._get_write_entries()
		result = {}
		for entry in entries:
			if printUnused or entry.used:
				if printDefault or not entry.source.startswith('<default'):
					if printState or not entry.option.startswith('#'):
						result.setdefault(entry.section, {}).setdefault(entry.option, []).append(entry)
		return result

	def write(self, stream, printMinimal = False, printSource = False, printTight = False, **kwarg):
		config = self._prepare_write(**kwarg)
		for section in sorted(config):
			if not printTight:
				stream.write('[%s]\n' % section)
			for option in sorted(config[section]):
				entryList = sorted(config[section][option], key = lambda e: e.order)
				if printMinimal:
					entryList = ConfigEntry.simplifyEntries(entryList)
				for entry in entryList:
					source = ''
					if printSource:
						source = entry.source
					stream.write(entry.format(printSection = printTight, source = source) + '\n')
			if not printTight:
				stream.write('\n')


# Historical config view
class HistoricalConfigView(ConfigView):
	def __init__(self, name, oldContainer, curContainer, parent = None):
		ConfigView.__init__(self, name, parent)
		self._oldContainer = oldContainer
		self._curContainer = curContainer

	def getView(self, viewClass = None, setSections = selectorUnchanged, **kwargs):
		if not viewClass:
			viewClass = self.__class__
		elif isinstance(viewClass, str):
			viewClass = ConfigView.getClass(viewClass)
		return viewClass(self.configName, self._oldContainer, self._curContainer, self,
			setSections = setSections, **kwargs)

	def _getSection(self, specific):
		raise AbstractError

	def _getSectionKey(self, section):
		raise AbstractError

	def _createEntry(self, option_list, value, opttype, source, specific, reverse):
		section = self._getSection(specific)
		if reverse:
			section += '!'
		return ConfigEntry(section, option_list[0], value, opttype, source)

	def _matchEntries(self, container, option_list = None):
		key_list = container.getKeys()
		if option_list is not None:
			key_list = lfilter(lambda key: key in key_list, option_list)

		result = []
		getFilteredSectionKey = lambda entry: self._getSectionKey(entry.section.replace('!', '').strip())
		def removeNone(key):
			if key is None:
				return -1
			return key
		getOrderedEntryKey = lambda entry: (tuple(imap(removeNone, getFilteredSectionKey(entry))), entry.order)
		for key in key_list:
			(entries, entries_reverse) = ([], [])
			for entry in container.getEntries(key, lambda x: getFilteredSectionKey(x) is not None):
				if entry.section.endswith('!'):
					entries_reverse.append(entry)
				else:
					entries.append(entry)
			result.extend(sorted(entries_reverse, key = getOrderedEntryKey, reverse = True))
			result.extend(sorted(entries, key = getOrderedEntryKey))
		return result

	def iterContent(self):
		return self._matchEntries(self._curContainer)

	def _getEntry(self, option_list, defaultEntry, defaultEntry_fallback):
		if defaultEntry.value != noDefault:
			self._curContainer.setDefault(defaultEntry)
		# Assemble matching config entries and combine them
		entries = self._matchEntries(self._curContainer, option_list)
		if defaultEntry.value != noDefault:
			entries.append(defaultEntry_fallback)
		self._log.log(logging.DEBUG1, 'Used config entries:')
		for entry in entries:
			self._log.log(logging.DEBUG1, '  %s (%s | %s)', entry.format(printSection = True), entry.source, entry.order)
		curEntry = ConfigEntry.combineEntries(entries)
		# Ensure that fallback default value is stored in persistent storage
		if (defaultEntry.value != noDefault) and defaultEntry_fallback.used:
			self._curContainer.setDefault(defaultEntry_fallback)
		return curEntry

	def _getDefaultEntries(self, option_list, default_str, persistent, oldEntry):
		if persistent and oldEntry:
			default_str = oldEntry.value
		defaultEntry = self._createEntry(option_list, default_str, '?=', '<default>', specific = False, reverse = True)
		if persistent and not oldEntry:
			if self._curContainer.getDefault(defaultEntry):
				defaultEntry = self._curContainer.getDefault(defaultEntry)
		defaultEntry_fallback = self._createEntry(option_list, defaultEntry.value, '?=', '<default fallback>', specific = True, reverse = False)
		return (defaultEntry, defaultEntry_fallback)

	# Return old and current merged config entries
	def get(self, option_list, default_str, persistent):
		oldEntry = None
		if self._oldContainer.enabled: # If old container is enabled => return stored entry
			oldEntry = ConfigEntry.combineEntries(self._matchEntries(self._oldContainer, option_list))
		# Process current entry
		(defaultEntry, defaultEntry_fallback) = self._getDefaultEntries(option_list, default_str, persistent, oldEntry)
		curEntry = self._getEntry(option_list, defaultEntry, defaultEntry_fallback)
		if curEntry is None:
			raise ConfigError('"[%s] %s" does not exist!' % (self._getSection(specific = False), option_list[0]))
		description = 'Using user supplied %s'
		if persistent and defaultEntry.used:
			description = 'Using persistent    %s'
		elif defaultEntry.used:
			description = 'Using default value %s'
		elif '!' in curEntry.section:
			description = 'Using dynamic value %s'
		self._log.log(logging.INFO2, description, curEntry.format(printSection = True))
		return (oldEntry, curEntry)

	def set(self, option_list, value, opttype, source):
		entry = self._createEntry(option_list, value, opttype, source, specific = True, reverse = True)
		self._curContainer.append(entry)
		self._log.log(logging.INFO3, 'Setting option %s', entry.format(printSection = True))
		return entry


# Simple ConfigView implementation
class SimpleConfigView(HistoricalConfigView):
	def __init__(self, name, oldContainer, curContainer, parent = None,
			setSections = selectorUnchanged, addSections = None):
		HistoricalConfigView.__init__(self, name, oldContainer, curContainer, parent)
		self._initVariable('_cfgSections', None, setSections, addSections, standardConfigForm)

	def _initVariable(self, memberName, default, setValue, addValue, normValues, parseValue = lambda x: [x]):
		def collect(value):
			return list(ichain(imap(parseValue, value)))
		# Setting initial value of variable
		result = default
		if hasattr(self._parent, memberName): # get from parent if available
			result = getattr(self._parent, memberName)
		if setValue is None:
			result = setValue
		elif setValue != selectorUnchanged:
			result = normValues(list(collect(setValue)))
		# Add to settings
		if addValue and (result is not None):
			result = result + normValues(list(collect(addValue)))
		elif addValue:
			result = normValues(list(collect(addValue)))
		setattr(self, memberName, result)
		return result

	def __repr__(self):
		return '<%s(sections = %r)>' % (self.__class__.__name__, self._cfgSections)

	def _getSectionKey(self, section):
		if self._cfgSections is None:
			return (section,)
		if section in self._cfgSections:
			return (self._cfgSections.index(section),)

	def _getSection(self, specific):
		if self._cfgSections and specific:
			return self._cfgSections[-1]
		elif self._cfgSections:
			return self._cfgSections[0]
		return 'global'
