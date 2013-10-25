from python_compat import *
from grid_control import APIError, ConfigError, RethrowError, utils, QM
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
	def __init__(self, value, source):
		(self.section, self.option) = (None, None)
		(self.value, self.source) = (value, source)
		(self.default, self.accessed) = (notSet, False)

	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self.__dict__)

	def format(self, section, value = None):
		result = ''
		if section:
			result += '[%s] ' % self.section
		value = QM(value == None, self.value, value)
		value = QM(isinstance(value, list), value, [value])
		if isinstance(self.value, list) and (notSet in self.value):
			if self.value[0] == notSet:
				result += '%s +=' % self.option
				return result + multi_line_format(str.join('\n', value[1:]).strip())
			elif self.value[-1] == notSet:
				result += '%s ^=' % self.option
				return result + multi_line_format(str.join('\n', value[:-1]).strip())
			else:
				idx = value.index(notSet)
				return self.format(section, value[:idx + 1]) + '\n' + self.format(section, value[idx:])
		else:
			result += '%s =' % self.option
			return result + multi_line_format(str.join('\n', value).strip())


# Container for config data - initialized from config file / dict, providing accessors
# Future: Further split into static and fully dynamic container - (how to bootstrap from static?)
class ConfigContainer(object):
	def __init__(self, name):
		(self._content, self._fixed) = ({}, [])
		self._logger = logging.getLogger('config.%s' % name)


	def iterContent(self, accessed):
		for section in sorted(self._content):
			for option in filter(lambda o: accessed == self._content[section][o].accessed, sorted(self._content[section])):
				yield self._content[section][option]


	def getOptions(self, section):
		return sorted(self._content.get(standardConfigForm(section), {}).keys())


	def setEntry(self, section, option, value, source, markAccessed = False):
		(section, option) = (standardConfigForm(section), standardConfigForm(option))
		option_type = None # Get config option modifier
		if option[-1] in ['+', '-', '*', '?', '^']:
			option_type = option[-1]
			option = option[:-1].strip() # option without modifier
		self._logger.log(logging.INFO2, 'Setting option [%s] %s %s=%s' % \
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
			elif self._content[section][option].source == '<default>':
				tmp[option] = ConfigEntry([value], source) # default values are also overriden by ?=
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


	def getEntry(self, section, option, default, markDefault = True):
		(section, option) = (standardConfigForm(section), standardConfigForm(option))
		entry = self._content.setdefault(section, {}).get(option)
		if not entry: # option was not set
			if default == noDefault:
				raise ConfigError('[%s] "%s" does not exist!' % (section, option))
			if markDefault: # Mark as default value
				entry = ConfigEntry([default], '<default>') # store default value
			else: # poorly hide the fact that its a default value (ie. to ensure persistency)
				entry = ConfigEntry([default], '<default-unmarked>') # store default value
			(entry.section, entry.option) = (section, option)
			self._content[section][option] = entry
			self._logger.log(logging.INFO3, 'Using default value %s' % entry.format(section = True))
		else:
			self._logger.log(logging.INFO3, 'Using user supplied %s' % entry.format(section = True))
		if (entry.default != notSet) and (entry.default != default):
			raise APIError('Inconsistent default values: [%s] "%s"' % (section, option))
		(entry.default, entry.accessed) = (default, True) # store default value and access
		if isinstance(entry.value, list): # resolve append/prefix queue
			if default != noDefault:
				entry.value = map(lambda x: QM(x == notSet, default, x), entry.value)
			entry.value = str.join('\n', filter(lambda x: x != notSet, entry.value))
		return entry


	def write(self, stream, printDefault = True, printUnused = True, printHeader = True):
		if printHeader:
			stream.write('\n; %s\n; This is the %s set of %sconfig options:\n; %s\n\n' % \
				('='*60, utils.QM(printDefault, 'complete', 'minimal'), utils.QM(printUnused, '', 'used '), '='*60))
		output = {} # {'section1': [output1, output2, ...], 'section2': [...output...], ...}
		def addToOutput(section, value, prefix = '\t'):
			output.setdefault(section.lower(), ['[%s]' % section]).append(value)
		for entry in self.iterContent(accessed = True): 
			# Don't print dynamically set config options unless requested
			isDynamic = entry.source in ['<default>', '<dict>', '<cmdline>', '<cmdline override>', '<dynamic>']
			if not printDefault and isDynamic:
				continue
			value_str = entry.format(section = False)
			default_str = ''
			if (entry.default not in [notSet, noDefault]) and not isDynamic:
				if '\n' not in entry.default:
					default_str = ' ; Default:'
				else:
					default_str = '\n; Default setting: %s =' % entry.option
				default_str += multi_line_format(entry.default).replace('\n', '\n;')
			if '\n' not in default_str and '\n' in value_str:
				value_str = value_str.replace('\n', default_str + '\n', 1)
			else:
				value_str += default_str
			addToOutput(entry.section, value_str)
		if printUnused:
			for entry in self.iterContent(accessed = False):
				addToOutput(entry.section, entry.format(section = False))
		stream.write('%s\n' % str.join('\n\n', map(lambda s: str.join('\n', output[s]), sorted(output))))
