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

import logging
from grid_control import utils
from grid_control.config.config_entry import ConfigEntry, noDefault, standardConfigForm
from grid_control.exceptions import ConfigError
from python_compat import sorted

selectorUnchanged = utils.makeEnum(['selector_unchanged'])

class ConfigView(object):
	def __init__(self, name, parent = None):
		if not parent:
			parent = self
		self._parent = parent
		self.pathDict = {}
		self.pathDict.update(parent.pathDict) # inherit path dict from parent
		self.setConfigName(name)

	def setConfigName(self, name):
		self.configName = name
		self._log = logging.getLogger('config.%s' % name)

	def getView(self, setSections = selectorUnchanged, **kwargs):
		raise AbstractError

	def iterContent(self):
		raise AbstractError

	# Return old and current merged config entries
	def get(self, option_list, default_str, persistent):
		raise AbstractError

	def set(self, option, value, opttype, source, markAccessed = True):
		raise AbstractError

	def write(self, stream, entries = None, printMinimal = False, printUnused = True, printSource = False, printDefault = True):
		if not entries:
			entries = self.iterContent()
		config_sections = {}
		for entry in entries:
			if printUnused or entry.accessed:
				if printDefault or not entry.source.startswith('<default'):
					config_sections.setdefault(entry.section, {}).setdefault(entry.option, []).append(entry)
		for section in sorted(config_sections):
			stream.write('[%s]\n' % section)
			for option in sorted(config_sections[section]):
				entryList = sorted(config_sections[section][option], key = lambda e: e.order)
				if printMinimal:
					entryList = ConfigEntry.simplifyEntries(entryList)
				for entry in entryList:
					for idx, line in enumerate(entry.format().splitlines()):
						if printSource and (idx == 0) and entry.source:
							stream.write('%-30s; %s\n' % (line, entry.source))
						else:
							stream.write(line + '\n')
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
		if option_list != None:
			key_list = filter(lambda key: key in key_list, option_list)

		result = []
		getFilteredSectionKey = lambda entry: self._getSectionKey(entry.section.replace('!', '').strip())
		getOrderedEntryKey = lambda entry: (getFilteredSectionKey(entry), entry.order)
		for key in key_list:
			(entries, entries_reverse) = ([], [])
			for entry in filter(lambda x: getFilteredSectionKey(x) != None, container.getEntries(key)):
				if entry.section.endswith('!'):
					entries_reverse.append(entry)
				else:
					entries.append(entry)
			result.extend(sorted(entries_reverse, key = getOrderedEntryKey, reverse = True))
			result.extend(sorted(entries, key = getOrderedEntryKey))
		return result

	def iterContent(self):
		return self._matchEntries(self._curContainer)

	# Return old and current merged config entries
	def get(self, option_list, default_str, persistent):
		def getEntriesWithDefault(container, option_list, default_str, desc):
			if default_str != noDefault:
				defaultEntry_fallback = self._createEntry(option_list, default_str, '?=', '<default fallback>', specific = True, reverse = False)
				defaultEntry = self._createEntry(option_list, default_str, '?=', '<default>', specific = False, reverse = True)
				container.setDefault(defaultEntry)

			entries = self._matchEntries(container, option_list)
			if default_str != noDefault:
				entries.append(defaultEntry_fallback)
			self._log.log(logging.DEBUG1, '%s config entries:' % desc)
			for entry in entries:
				self._log.log(logging.DEBUG1, '  %s (%s | %s)' % (entry.format(printSection = True), entry.source, entry.order))
			result = ConfigEntry.combineEntries(entries)
			if (default_str != noDefault) and defaultEntry_fallback.accessed:
				container.setDefault(defaultEntry_fallback)
			return (result, (default_str != noDefault) and (defaultEntry.accessed or defaultEntry_fallback.accessed))

		oldEntry = None
		if self._oldContainer.enabled:
			oldEntry = ConfigEntry.combineEntries(self._matchEntries(self._oldContainer, option_list))
			if oldEntry and persistent: # Override current default value with stored value
				default_str = oldEntry.value
		(curEntry, curUsedDefault) = getEntriesWithDefault(self._curContainer, option_list, default_str, 'Used')
		if curEntry == None:
			raise ConfigError('"[%s] %s" does not exist!' % (self._getSection(specific = False), option_list[0]))
		elif curUsedDefault and oldEntry and persistent:
			self._log.log(logging.INFO2, 'Using persistent    %s' % curEntry.format(printSection = True))
		elif curUsedDefault:
			self._log.log(logging.INFO2, 'Using default value %s' % curEntry.format(printSection = True))
		elif '!' in curEntry.section:
			self._log.log(logging.INFO2, 'Using dynamic value %s' % curEntry.format(printSection = True))
		else:
			self._log.log(logging.INFO2, 'Using user supplied %s' % curEntry.format(printSection = True))

		return (oldEntry, curEntry)

	def set(self, option_list, value, opttype, source, markAccessed = True):
		entry = self._createEntry(option_list, value, opttype, source, specific = True, reverse = True)
		self._curContainer.append(entry)
		self._log.log(logging.INFO3, 'Setting option %s' % entry.format(printSection = True))
		return entry


# Simple ConfigView implementation
class SimpleConfigView(HistoricalConfigView):
	def __init__(self, name, oldContainer, curContainer, parent = None,
			setSections = selectorUnchanged, addSections = []):
		HistoricalConfigView.__init__(self, name, oldContainer, curContainer, parent)
		self._initVariable('_cfgSections', None, setSections, addSections, standardConfigForm)

	def _initVariable(self, memberName, default, setValue, addValue, normValues, parseValue = lambda x: [x]):
		def collect(value):
			for entries in map(parseValue, value):
				for entry in entries:
					yield entry
		# Setting initial value of variable
		result = default
		if hasattr(self._parent, memberName): # get from parent if available
			result = getattr(self._parent, memberName)
		if setValue == None:
			result = setValue
		elif setValue != selectorUnchanged:
			result = normValues(list(collect(setValue)))
		# Add to settings
		if addValue and (result != None):
			result = result + normValues(list(collect(addValue)))
		elif addValue:
			result = normValues(list(collect(addValue)))
		setattr(self, memberName, result)
		return result

	def __str__(self):
		return '<%s(sections = %r)>' % (self.__class__.__name__, self._cfgSections)

	def _getSectionKey(self, section):
		if self._cfgSections == None:
			return section
		if section in self._cfgSections:
			return self._cfgSections.index(section)

	def _getSection(self, specific):
		if self._cfgSections and specific:
			return self._cfgSections[-1]
		elif self._cfgSections:
			return self._cfgSections[0]
		return 'global'
