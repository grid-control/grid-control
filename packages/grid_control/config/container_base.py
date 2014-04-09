from python_compat import sorted, set
from grid_control import APIError, AbstractError, ConfigError, RethrowError, utils, QM
import logging

# Placeholder to specify a non-existing default or not-set value
noDefault = utils.makeEnum(['noDefault'])
notSet = utils.makeEnum(['notSet'])

# return canonized section or option string
standardConfigForm = lambda x: str(x).strip().lower()

def multi_line_format(value):
	value_list = map(str.strip, filter(lambda x: x.strip() != '', value.strip().splitlines()))
	if len(value_list) > 1:
		return '\n\t%s' % str.join('\n\t', value_list)
	return ' %s' % str.join('\n\t', value_list)

# Holder of config information
class ConfigEntry(object):
	def __init__(self, value, source, section = None, option = None, default = notSet, accessed = False):
		(self.section, self.option) = (section, option)
		(self.value, self.source) = (value, source)
		(self.default, self.accessed) = (default, accessed)

	def __repr__(self):
		def fmtValue(value):
			if value == noDefault:
				return '<no default>'
			elif value == notSet:
				return '<not set>'
			return repr(value)
		return '%s(%s)' % (self.__class__.__name__, str.join(', ', map(lambda (k, v): '%s = %s' % (k, fmtValue(v)), sorted(self.__dict__.items()))))

	def format_opt(self):
		return '[%s] %s' % (self.section, self.option)

	def format(self, printSection = False, printDefaultValue = False):
		if self.option.startswith('#'):
			return ''
		entries = self._format(self.value)
		if printSection: # Print prefix with section information
			entries = map(lambda entry: '[%s] %s' % (self.section, entry), entries)
		if printDefaultValue and (self.default not in [notSet, noDefault]): # Add default information
			defentries = self._format([self.default], printOption = False)
			assert(len(defentries) == 1) # Defaults can't be queues!
			lines_def = map(str.lstrip, defentries[0].splitlines())
			lines_def[0] = 'Default: %s' % lines_def[0]
			lines_val = entries[0].splitlines()
			tmp = [] # Combine values and defaults line-wise.
			for line in range(max(len(lines_def), len(lines_val))):
				if (line < len(lines_val)) and (line < len(lines_def)):
					tmp.append('%s ; %s' % (lines_val[line], lines_def[line]))
				elif line < len(lines_val):
					tmp.append(lines_val[line])
				elif line < len(lines_def):
					tmp.append('\t ; %s' % lines_def[line])
			entries = [str.join('\n', tmp)] + entries[1:]
		return str.join('\n', entries)

	# Returns list of option settings corresponding to the current set
	def _format(self, value, printOption = True):
		# Package resolved values into list container
		value = QM(isinstance(value, list), value, [value])
		# Handle value queues
		if notSet in value:
			if value[0] == notSet:
				# Return current append queue
				result = '%s +=' % self.option
				return [result + multi_line_format(str.join('\n', value[1:]).strip())]
			elif value[-1] == notSet:
				# Return current prepend queue
				result = '%s ^=' % self.option
				return [result + multi_line_format(str.join('\n', value[:-1]).strip())]
			else:
				# In case of append / prepend queues, multiple settings are necessary to reflect the setup
				idx = value.index(notSet)
				return [self._format(value[:idx + 1]), self._format(value[idx:])]
		else:
			# Simple value handling
			result = multi_line_format(str.join('\n', value).strip())
			if printOption:
				return [('%s =' % self.option) + result]
			return [result]


# General container for config data
class ConfigContainer(object):
	def __init__(self, name):
		self._logger = logging.getLogger('config.%s' % name)
		self._readOnly = False

	def iterContent(self):
		raise AbstractError

	def getOptions(self, section, getDefault = False):
		raise AbstractError

	def setReadOnly(self):
		self._readOnly = True

	def setEntry(self, section, option, value, source, markAccessed = False):
		raise AbstractError

	def getEntry(self, section, option, default, markDefault = True, raiseMissing = True):
		raise AbstractError

	def write(self, stream, printDefault = True, printUnused = True):
		stream.write('\n; %s\n; This is the %s set of %sconfig options:\n; %s\n\n' % \
			('='*60, QM(printDefault, 'complete', 'minimal'), QM(printUnused, '', 'used '), '='*60))
		output = {} # {'section1': [output1, output2, ...], 'section2': [...output...], ...}
		def addToOutput(section, value, prefix = '\t'):
			if value:
				output.setdefault(section.lower(), ['[%s]' % section]).append(value)
		for entry in filter(lambda e: e.accessed == True, self.iterContent()):
			# Don't print default values unless specified - dynamic settings always derive from non-default settings
			if not printDefault and (entry.source in ['<default>', '<dynamic>']):
				continue
			# value-default comparison, since for persistent entries: value == default, source != '<default>'
			addToOutput(entry.section, entry.format(printDefaultValue = (entry.value != entry.default)))
		if printUnused: # Unused entries have no stored default value => printDefault is not utilized
			for entry in filter(lambda e: e.accessed == False, self.iterContent()):
				addToOutput(entry.section, entry.format(printSection = False, printDefaultValue = False))
		stream.write('%s\n' % str.join('\n\n', map(lambda s: str.join('\n', output[s]), sorted(output))))


# Container for config data using dictionaries
# TODO: further move functionality into ConfigContainer - in case a dynamic container is implemented
class BasicConfigContainer(ConfigContainer):
	def __init__(self, name):
		(self._content, self._fixed) = ({}, [])
		ConfigContainer.__init__(self, name)


	def iterContent(self):
		for section in sorted(self._content):
			for option in sorted(self._content[section]):
				yield self._content[section][option]


	def getOptions(self, section, getDefault = False):
		result = []
		content_section = self._content.get(standardConfigForm(section), {})
		for option in content_section:
			if getDefault or (content_section.get(option).source not in ['<default>', '<default-unmarked>']):
				result.append(option)
		return result


	def setEntry(self, section, option, value, source, markAccessed = False):
		if self._readOnly:
			raise APIError('Config container is read-only!')
		(section, option) = (standardConfigForm(section), standardConfigForm(option))
		option_type = None # Get config option modifier
		if option[-1] in ['+', '-', '*', '?', '^']:
			option_type = option[-1]
			option = option[:-1].strip() # option without modifier
		self._logger.log(logging.INFO2, 'Setting config opt. [%s] %s %s=%s' % \
			(section, option, QM(option_type, option_type, ''), multi_line_format(value)))
		if (section, option) in self._fixed:
			self._logger.log(logging.INFO2, 'Skipped setting fixed option [%s] %s!' % (section, option))
			return
		tmp = self._content.setdefault(section, {})
		if option_type == '+': # append to existing option / default value
			if option not in self._content[section]: # queue value in append queue
				tmp[option] = ConfigEntry([notSet, value], source)
			else: # append to existing value
				if not isinstance(tmp[option].value, list):
					tmp[option].value = [tmp[option].value]
				tmp[option] = ConfigEntry(tmp[option].value + [value], source)
		elif option_type == '^': # prepend to existing option / default value
			if option not in self._content[section]: # queue value in append queue
				tmp[option] = ConfigEntry([value, notSet], source)
			else: # append to existing value
				if not isinstance(tmp[option].value, list):
					tmp[option].value = [tmp[option].value]
				tmp[option] = ConfigEntry([value] + tmp[option].value, source)
		elif option_type == '?': # not sure how to handle "?=" after "+=" - replace or append?
			if option not in self._content[section]: # set only unset options
				tmp[option] = ConfigEntry([value], source)
			elif self._content[section][option].source in ['<default>', '<dynamic>']:
				tmp[option] = ConfigEntry([value], source) # dynamic and default values are also overriden by ?=
		elif option_type == '*': # this option can not be changed by other config files
			tmp[option] = ConfigEntry([value], source)
			self._fixed.append((section, option))
		elif option_type == '-': # remove any existing entries
			if option in self._content[section]:
				if value.strip() == '':
					self._content[section].pop(option)
				else:
					raise ConfigError('Unable to apply -= operator to option [%s] %s!' % (section, option))
		else:
			tmp[option] = ConfigEntry([value], source)
		if markAccessed:
			tmp[option].accessed = True
		(tmp[option].section, tmp[option].option) = (section, option)
		return tmp[option]


	def getEntry(self, section, option, default, markDefault = True, raiseMissing = True):
		(section, option) = (standardConfigForm(section), standardConfigForm(option))
		entry = self._content.setdefault(section, {}).get(option)
		if not entry: # option was not set
			if default == noDefault:
				if raiseMissing: # Fix for new config options, without default value
					raise ConfigError('[%s] "%s" does not exist!' % (section, option))
				return None
			if markDefault: # Mark as default value
				entry = ConfigEntry([default], '<default>') # store default value
			else: # poorly hide the fact that its a default value (ie. to ensure persistency)
				entry = ConfigEntry([default], '<default-unmarked>') # store default value
			(entry.section, entry.option) = (section, option)
			self._content[section][option] = entry
			self._logger.log(logging.INFO3, 'Using default value %s' % entry.format(printSection = True))
		else:
			self._logger.log(logging.INFO3, 'Using user supplied %s' % entry.format(printSection = True))
		if (entry.default != notSet) and (entry.default != default):
			raise APIError('Inconsistent default values: [%s] "%s"' % (section, option))
		(entry.default, entry.accessed) = (default, True) # store default value and access
		if isinstance(entry.value, list): # resolve append/prefix queue
			if default != noDefault:
				entry.value = map(lambda x: QM(x == notSet, default, x), entry.value)
			entry.value = str.join('\n', filter(lambda x: x != notSet, entry.value))
		return entry


# Container allowing access via selectors
class ResolvingConfigContainer(BasicConfigContainer):
	def iterContent(self, selOptions = [], selSections = [], selNames = [], selTags = []):
		self._logger.log(logging.DEBUG1, 'Matching section: %r names: %r tags: %r options: %r' %
			(selSections, selNames, selTags, selOptions))
		# Function to parse section into section name, section titles and section tags
		def parseSection(section):
			tmp = section.split()
			assert(len(tmp) > 0)
			nameList = []
			tagDict = {}
			for entry in tmp[1:]:
				if ':' in entry:
					tagentry = entry.split(':')
					assert(len(tagentry) == 2)
					tagDict[tagentry[0]] = tagentry[1]
				else:
					nameList.append(entry)
			return (section, (tmp[0], nameList, tagDict)) # section, (main, nameList, tagDict)
		sectionList = map(parseSection, self._content)

		# Function to impose weak ordering on sections
		def cmpSection(a, b):
			(a_sMain, a_sNames, a_sTags) = a[1] # a = (section, sectionInfo)
			(b_sMain, b_sNames, b_sTags) = b[1]
			def findIndex(search, value, default):
				try:
					return -search.index(value)
				except:
					return default
			cmpSection = cmp(findIndex(selSections, a_sMain, a_sMain), findIndex(selSections, b_sMain, b_sMain))
			if cmpSection != 0:
				return -cmpSection # return same order as used in selSelections
			cmpNames = cmp(a_sNames, b_sNames)
			if cmpNames != 0:
				return -cmpNames # entries without names come *after* entries with names
			selTagsOrder = map(lambda (tk, tv): tk, selTags)
			score = lambda tags: sum(map(lambda (i, t): 1 << (len(selTagsOrder) - findIndex(selTagsOrder, t, 0)), enumerate(tags)))
			return -cmp(score(a_sTags), score(b_sTags))

		def matchSection(sectionEntry):
			(section, sectionInfos) = sectionEntry
			(sMain, sTitles, sTags) = sectionInfos
			if selSections and (sMain not in map(str.lower, selSections)):
				return False
			if sTitles:
				for selName in map(str.lower, selNames):
					if selName not in sTitles:
						return False
			for (selTagKey, selTagValue) in selTags:
				if selTagKey not in sTags:
					continue
				if sTags[selTagKey] != selTagValue:
					return False
			return True

		sectionList = sorted(filter(matchSection, sectionList), cmp = cmpSection)
		def iterContentImpl():
			if selOptions: # option list specified - return matching sections in same order
				for option in map(str.lower, selOptions):
					for (section, sectionInfo) in sectionList:
						if option in self._content[section]:
							yield self._content[section][option]
			else: # no option specified
				for (section, sectionInfo) in sectionList:
					for option in sorted(self._content[section]):
						yield self._content[section][option]
		result = list(iterContentImpl())
		for entry in result:
			self._logger.log(logging.DEBUG1, '\t%s matches' % entry.format_opt())
		return result


	def getOptions(self, selector):
		result = []
		(selSections, selOptions, selNames, selTags) = selector
		for entry in self.iterContent(selOptions, selSections, selNames, selTags):
			if entry.option not in result:
				result.append(entry.option)
		return result


	def get(self, selector, default, markDefault = True, raiseMissing = True):
		(selSections, selOptions, selNames, selTags) = selector
		# TODO: Option inheritance: dont just stop at best matching entry
		#       collect / apply settings from all matching entries
		for entry in self.iterContent(selOptions, selSections, selNames, selTags):
			return self.getEntry(entry.section, entry.option,
				default, markDefault, raiseMissing)
		return self.getEntry(selSections[0], selOptions[0],
			default, markDefault, raiseMissing)


	def set(self, selector, value, source, markAccessed = False):
		(selSections, selOptions, selNames, selTags) = selector
		# TODO: Dont just write to most specific entry - go up in priority
		#       till existing section without specific option is found
		section = selSections[0]
		strName = str.join(' ', selNames)
		if selSections[0].lower() != strName.lower():
			section += (' ' + strName)
		selTags = filter(lambda (tk, tv): tk != tv, selTags)
		section += (' ' + str.join(' ', map(lambda ti: '%s:%s' % ti, selTags)))
		return self.setEntry(section, selOptions[0], value, source, markAccessed)
