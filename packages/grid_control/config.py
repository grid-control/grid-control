import os, inspect, socket, logging, ConfigParser as cp
from grid_control import *
from python_compat import *

noDefault = cp.NoOptionError
def fmtDef(value, default, defFmt = lambda x: x): 
	if value == noDefault:
		return noDefault
	return defFmt(default)

def cleanSO(section, option): # return canonized section/option tuple
	strStrip = lambda x: str(x).strip().lower()
	if isinstance(section, list):
		return (utils.uniqueListRL(map(strStrip, section)), strStrip(option))
	return (strStrip(section), strStrip(option))

def fmtStack(stack):
	for frame in stack:
		caller = frame[0].f_locals.get('self', None)
		if caller and caller.__class__.__name__ != 'Config':
			return frame[0].f_locals.get('self', None).__class__.__name__
	return 'main'

class Config:
	def __init__(self, configFile = None, configDict = {}):
		# Configure logging for this config instance
		self.confName = str.join('', os.path.basename(str(configFile)).split('.')[:-1])
		self.logger = logging.getLogger(('config.%s' % self.confName).rstrip('.'))

		self.hidden = [('global', 'workdir'), ('global', 'workdir base'), ('global', 'include')]
		(self.allowSet, self.workDir) = (True, None)
		(self.content, self.apicheck, self.append, self.accessed, self.dynamic) = ({}, {}, {}, {}, {})
		(self.configLog, self.configLogName) = ({}, None)
		self.setConfigLog(None)

		# Collect host / user / installation specific config files
		host = socket.gethostbyaddr(socket.gethostname())[0]
		hostCfg = map(lambda c: utils.pathGC('config/%s.conf' % host.split('.', c)[-1]), range(host.count('.') + 1, 0, -1))
		defaultCfg = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathGC('config/default.conf')]
		# Read default config files
		for cfgFile in filter(os.path.exists, map(lambda p: utils.resolvePath(p, check = False), hostCfg + defaultCfg)):
			self.parseFile(cfgFile)

		if configFile:
			# use the directory of the config file as base directory
			self.baseDir = utils.cleanPath(os.path.dirname(configFile))
			self.configFile = os.path.join(self.baseDir, os.path.basename(configFile))
			self.parseFile(configFile)
		else:
			(self.baseDir, self.configFile, self.confName) = ('.', 'gc.conf', 'gc')
		wdBase = self.getPath('global', 'workdir base', self.baseDir, check = False)
		self.workDir = self.getPath('global', 'workdir', os.path.join(wdBase, 'work.' + self.confName), check = False)

		# Override config settings via dictionary
		for section in configDict:
			for item in configDict[section]:
				self.setInternal(section, item, str(configDict[section][item]), False)
		self.set('global', 'include', '', default = '')


	def setConfigLog(self, name):
		self.configLog[name] = {}
		self.configLogName = name
		return self.configLog[name]


	def getConfigLog(self, name):
		return self.configLog[name]


	def getTaskDict(self):
		return utils.PersistentDict(os.path.join(self.workDir, 'task.dat'), ' = ')


	def setInternal(self, section, item, value, append):
		section_item = cleanSO(section, item)
		# Split into lines, remove comments following the last ";" and return merged multi-line result
		value = map(lambda x: utils.rsplit(x, ';', 1)[0].strip(), value.splitlines())
		value = str.join('\n', filter(lambda x: x != '', value))
		if append and (section_item in self.content):
			# Append new value to existing value
			self.content[section_item] += '\n%s' % value
			self.append[section_item] = False
		else:
			# If there is no existing value, mark it so the user value is appended to the default value
			self.content[section_item] = value
			self.append[section_item] = append


	def parseFile(self, configFile, defaults = None):
		self.logger.log(logging.INFO1, 'Reading config file: %s' % configFile)
		try:
			configFile = utils.resolvePath(configFile)
			for line in map(lambda x: x.rstrip() + '=:', open(configFile, 'r').readlines()):
				# Abort if non-indented, non-commented line with ":" preceeding "=" was found
				if (line.find(":") < line.find("=")) and (line.lstrip() == line) and not line.lstrip().startswith(';'):
					raise ConfigError('Invalid config line:\n\t%s\nPlease use "key = value" syntax or indent values!' % line)
			parser = cp.ConfigParser(defaults)
			parser.readfp(open(configFile, 'r'))
			if parser.has_section('global') and parser.has_option('global', 'include'):
				self.setInternal('global', 'include', parser.get('global', 'include'), False)
				for includeFile in self.getPaths('global', 'include', [], mutable = True):
					self.parseFile(includeFile, parser.defaults())
			for section in parser.sections():
				for option in parser.options(section):
					value = parser.get(section, option)
					try:
						self.setInternal(section, option.rstrip('+'), value, option.endswith('+'))
					except:
						raise ConfigError('[%s] "%s" could not be parsed!' % (section, option))
			self.setInternal('global', 'include', '', False)
		except:
			raise RethrowError("Error while reading configuration file '%s'!" % configFile, ConfigError)


	def set(self, section, item, value = None, override = True, append = False, default = noDefault):
		self.logger.log(logging.DEBUG2, 'Config set request from: %s' % fmtStack(inspect.stack()))
		(section, item) = cleanSO(section, item)
		if isinstance(section, list):
			section = section[0] # set most specific setting
		if not self.allowSet:
			raise APIError('Invalid runtime config override: [%s] %s = %s' % (section, item, value))
		self.logger.log(logging.INFO3, 'Overwrite of option [%s] %s = %s' % (section, item, value))
		if ((section, item) not in self.content) or ((section, item) in self.dynamic) or override:
			self.setInternal(section, item, value, append)
			self.accessed[(section, item)] = (value, default)
			self.dynamic[(section, item)] = True
			self.configLog[self.configLogName][(section, item)] = value


	def getInternal(self, section, item, default, mutable, noVar):
		(section, item) = cleanSO(section, item)
		# Handle multi section get calls, use least specific setting for error message
		if isinstance(section, list):
			self.logger.log(logging.DEBUG1, 'Searching option "%s" in:\n\t%s' % (item, str.join('\n\t', section)))
			for specific in filter(lambda s: item in self.getOptions(s), section):
				return self.getInternal(specific, item, default, mutable, noVar)
			return self.getInternal(section[-1], item, default, mutable, noVar) # will trigger error message
		self.logger.log(logging.DEBUG2, 'Config get request from: %s' % fmtStack(inspect.stack()))
		# API check: Keep track of used default values
		if self.apicheck.setdefault((section, item), (default, mutable))[0] != default:
			raise APIError('Inconsistent default values: [%s] "%s"' % (section, item))
		self.apicheck[(section, item)] = (default, mutable and self.apicheck[(section, item)][1])
		# Get config option value and protocol access
		isDefault = False
		if (section, item) in self.content:
			value = self.content[(section, item)]
			if self.append.get((section, item), False) and default != noDefault:
				value = '%s\n%s' % (default, value)
			self.logger.log(logging.INFO3, 'Using user supplied [%s] %s = %s' % (section, item, value))
		else:
			if default == noDefault:
				raise ConfigError('[%s] "%s" does not exist!' % (section, item))
			self.logger.log(logging.INFO3, 'Using default value [%s] %s = %s' % (section, item, default))
			value = default
			isDefault = True
		self.accessed[(section, item)] = (value, default)
		self.configLog[self.configLogName][(section, item)] = value
		return (utils.checkVar(value, '[%s] "%s" may not contain variables.' % (section, item), noVar), isDefault)


	def getTyped(self, desc, value2str, str2value, section, item, default, mutable, noVar, default2value = None):
		# First transform default into string if applicable
		default_str = noDefault
		if default != noDefault:
			default_str = value2str(default)
		# Get string from config file
		if isinstance(section, list):
			section = utils.uniqueListRL(map(lambda x: str(x).lower(), section))
		(resultraw, isDefault) = self.getInternal(section, item, default_str, mutable, noVar)
		self.logger.log(logging.DEBUG1, 'Raw result for %s from [%s] %s: %s' % (desc, section, item, resultraw))
		# Convert string back to type - or directly use default
		if isDefault:
			if default2value == None:
				result = str2value(value2str(default))
			else:
				result = default2value(default)
		else:
			result = str2value(resultraw)
		if isinstance(section, list):
			section = str.join(', ', section)
		self.logger.log(logging.INFO2, 'Result for %s from [%s] %s: %s' % (desc, section, item, result))
		return result


	def get(self, section, item, default = noDefault, mutable = False, noVar = True):
		return self.getTyped('string', str, str, section, item, default, mutable, noVar)


	def getInt(self, section, item, default = noDefault, mutable = False, noVar = True):
		return self.getTyped('int', str, int, section, item, default, mutable, noVar)


	def getBool(self, section, item, default = noDefault, mutable = False, noVar = True):
		value2str = lambda value: QM(value, 'true', 'false')
		result = self.getTyped('bool', value2str, utils.parseBool, section, item, default, mutable, noVar)
		if result == None:
			raise ConfigError('Unable to parse bool from [%s] %s' % (section, item))
		return result


	def getTime(self, section, item, default = noDefault, mutable = False, noVar = True):
		return self.getTyped('time', utils.strTimeShort, utils.parseTime, section, item, default, mutable, noVar)


	def getList(self, section, item, default = noDefault, mutable = False, noVar = True):
		# getList('') == []; getList(None) == None;
		def value2str(value):
			if value:
				return '\n' + str.join('\n', map(str, value))
			return ''
		str2value = lambda value: utils.parseList(value, None)
		result = self.getTyped('list', value2str, str2value, section, item, default, mutable, noVar,
			default2value = lambda x: x)
		if result != None:
			return map(str, result)


	def getDict(self, section, item, default = noDefault, mutable = False, noVar = True, parser = lambda x: x):
		value2str = lambda value: str.join('\n\t', map(lambda kv: '%s => %s' % kv, value.items()))
		str2value = lambda value: utils.parseDict(value, parser)
		result = self.getTyped('dictionary', value2str, str2value, section, item, default, mutable, noVar)
		if result == default: # default is given by dict, but this function returns ({dict}, [key order])
			return (default, default.keys())
		return result


	def parsePath(self, value, check):
		if value == '':
			return ''
		try:
			return utils.resolvePath(value, [self.baseDir], check, ConfigError)
		except:
			raise RethrowError('Error resolving path %s' % value, ConfigError)


	def getPath(self, section, item, default = noDefault, mutable = False, noVar = True, check = True):
		return self.getTyped('path', str, lambda p: self.parsePath(p, check), section, item, default, mutable, noVar)


	def parsePaths(self, value, check):
		result = []
		for path in utils.parseList(value, None, onEmpty = []):
			result.append(self.parsePath(path, check))
		return result


	def getPaths(self, section, item, default = noDefault, mutable = False, noVar = True, check = True):
		value2str = lambda value: '\n' + str.join('\n', value)
		return self.getTyped('paths', value2str, lambda pl: self.parsePaths(pl, check), section, item, default, mutable, noVar)


	def getOptions(self, section):
		return map(lambda (s, i): i, filter(lambda (s, i): s == section.lower(), self.content.keys()))


	# Compare this config object to another config file
	# Return true in case non-mutable parameters are changed
	def needInit(self, saveConfigPath):
		if not os.path.exists(saveConfigPath):
			return False
		savedConfig = Config(saveConfigPath)
		flag = False
		for (section, option) in filter(lambda so: so not in self.hidden, self.accessed):
			default, mutable = self.apicheck.get((section, option), (None, False))
			value, default = self.accessed[(section, option)]
			try:
				oldValue = savedConfig.get(section, option, default, noVar = False)
			except:
				oldValue = '<not specified>'
			if (str(value).strip() != str(oldValue).strip()) and not mutable:
				if not flag:
					self.logger.warn('Found some changes in the config file, which will only ' +
						'apply to the current task after a reinitialization:')
				outputLine = '[%s] %s = %s' % (section, option, value.replace('\n', '\n\t'))
				outputLine += QM(len(outputLine) > 60, '\n', '') + '  (old value: %s)' % oldValue.replace('\n', '\n\t')
				self.logger.warn(outputLine)
				flag = True
		unused = sorted(filter(lambda x: x not in self.accessed, self.content))
		self.logger.log(logging.INFO1, 'There are %s unused config options!' % len(unused))
		for (section, option) in unused:
			self.logger.log(logging.INFO1, '\t[%s] %s = %s' % (section, option, self.content[(section, option)]))
		return flag


	def prettyPrint(self, stream, printDefault = True, printUnused = True, printHeader = True):
		if printHeader:
			stream.write('\n; %s\n; This is the %s set of %sconfig options:\n; %s\n\n' % \
				('='*60, utils.QM(printDefault, 'complete', 'minimal'), utils.QM(printUnused, '', 'used '), '='*60))
		output = {} # {'section1': [output1, output2, ...], 'section2': [...output...], ...}
		def addToOutput(section, value, prefix = '\t'):
			value = str(value).replace('\n', '\n' + prefix) # format multi-line options
			output.setdefault(section.lower(), ['[%s]' % section]).append(value)
		for (section, option) in sorted(self.accessed):
			value, default = self.accessed[(section, option)]
			dummy, mutable = self.apicheck.get((section, option), (None, False))
			if (value != default) or printDefault:
				tmp = '%s = %s' % (option, value)
				if mutable:
					tmp += ' ; Mutable'
				if (value != default) and (default != noDefault) and ('\n' not in str(default)):
					tmp += QM(mutable, ', Default: %s' % default, ' ; Default: %s' % default)
				addToOutput(section, tmp)
				if (value != default) and (default != noDefault) and ('\n' in str(default)):
					addToOutput(section, '; Default setting: %s = %s' % (option, default), ';\t')
		if printUnused:
			for (section, option) in sorted(filter(lambda x: x not in self.accessed, self.content)):
				addToOutput(section, '%s = %s' % (option, self.content[(section, option)]))
		stream.write('%s\n' % str.join('\n\n', map(lambda s: str.join('\n', output[s]), sorted(output))))
