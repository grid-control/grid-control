import os, inspect, ConfigParser as cp
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
		(self.allowSet, self.workDir) = (True, None)
		self.hidden = [('global', 'workdir'), ('global', 'workdir base'), ('global', 'include')]
		(self.content, self.apicheck, self.append, self.accessed, self.dynamic) = ({}, {}, {}, {}, {})
		(self.configLog, self.configLogName) = ({}, None)
		self.setConfigLog(None)

		defaultCfg = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathGC('default.conf')]
		for cfgFile in filter(os.path.exists, map(lambda p: utils.resolvePath(p, check = False), defaultCfg)):
			self.parseFile(cfgFile)
		if configFile:
			# use the directory of the config file as base directory
			self.baseDir = utils.cleanPath(os.path.dirname(configFile))
			self.configFile = os.path.join(self.baseDir, os.path.basename(configFile))
			self.confName = str.join('', os.path.basename(configFile).split('.')[:-1])
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


	def setInternal(self, section, option, value, append):
		section_option = cleanSO(section, option)
		# Split into lines, remove comments and return merged result
		value = map(lambda x: utils.rsplit(x, ';', 1)[0].strip(), value.splitlines())
		value = str.join('\n', filter(lambda x: x != '', value))
		if append and (section_option in self.content):
			self.content[section_option] += '\n%s' % value
			self.append[section_option] = False
		else:
			self.content[section_option] = value
			self.append[section_option] = append


	def parseFile(self, configFile, defaults = None):
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
		utils.vprint('{%s}: ' % fmtStack(inspect.stack()), 4, newline=False)
		(section, item) = cleanSO(section, item)
		if isinstance(section, list):
			section = section[0] # set most specific setting
		if not self.allowSet:
			raise APIError('Invalid runtime config override: [%s] %s = %s' % (section, item, value))
		utils.vprint('Overwrite of option [%s] %s = %s' % (section, item, value), 2)
		if ((section, item) not in self.content) or ((section, item) in self.dynamic) or override:
			self.setInternal(section, item, value, append)
			self.accessed[(section, item)] = (value, default)
			self.dynamic[(section, item)] = True
			self.configLog[self.configLogName][(section, item)] = value


	def get(self, section, item, default = noDefault, mutable = False, noVar = True):
		(section, item) = cleanSO(section, item)
		# Handle multi section get calls, use least specific setting for error message
		if isinstance(section, list):
			utils.vprint('Searching option "%s" in:\n\t%s' % (item, str.join('\n\t', section)), 4)
			for specific in filter(lambda s: item in self.getOptions(s), section):
				return self.get(specific, item, default, mutable, noVar)
			return self.get(section[-1], item, default, mutable, noVar) # will trigger error message
		utils.vprint('{%s}: ' % fmtStack(inspect.stack()), 5, newline=False)
		# API check: Keep track of used default values
		if self.apicheck.setdefault((section, item), (default, mutable))[0] != default:
			raise APIError('Inconsistent default values: [%s] "%s"' % (section, item))
		self.apicheck[(section, item)] = (default, mutable and self.apicheck[(section, item)][1])
		# Get config option value and protocol access
		if (section, item) in self.content:
			value = self.content[(section, item)]
			if self.append.get((section, item), False) and default != noDefault:
				value = '%s\n%s' % (default, value)
			utils.vprint('Using user supplied [%s] %s = %s' % (section, item, value), 3)
		else:
			if default == noDefault:
				raise ConfigError('[%s] "%s" does not exist!' % (section, item))
			utils.vprint('Using default value [%s] %s = %s' % (section, item, default), 3)
			value = default
		self.accessed[(section, item)] = (value, default)
		self.configLog[self.configLogName][(section, item)] = value
		return utils.checkVar(value, '[%s] "%s" may not contain variables.' % (section, item), noVar)


	def getPaths(self, section, item, default = noDefault, mutable = False, noVar = True, check = True):
		def getPathsInt():
			try:
				for value in self.getList(section, item, default, mutable, noVar):
					yield utils.resolvePath(value, [self.baseDir], check, ConfigError)
			except:
				raise RethrowError('Error resolving path in [%s] %s' % (section, item), ConfigError)
		return list(getPathsInt())


	def getPath(self, section, item, default = noDefault, mutable = False, noVar = True, check = True):
		return (self.getPaths(section, item, fmtDef(default, [default]), mutable, noVar, check) + [''])[0]


	def getInt(self, section, item, default = noDefault, mutable = False, noVar = True):
		return int(self.get(section, item, default, mutable, noVar))


	def getBool(self, section, item, default = noDefault, mutable = False, noVar = True):
		return utils.parseBool(self.get(section, item, fmtDef(default, QM(default, 'true', 'false')), mutable, noVar))


	def getTime(self, section, item, default = noDefault, mutable = False, noVar = True):
		return utils.parseTime(self.get(section, item, fmtDef(default, default, utils.strTimeShort), mutable, noVar))


	def getList(self, section, item, default = noDefault, mutable = False, noVar = True, delim = None, empty = []):
		if default == None:
			(default, empty) = ('', None)
		elif default != noDefault:
			default = str.join(QM(delim, delim, ' '), map(str, default))
		if empty != None:
			empty = list(empty)
		return utils.parseList(self.get(section, item, default, mutable, noVar), delim, onEmpty = empty)


	def getDict(self, section, item, default = noDefault, mutable = False, noVar = True, parser = lambda x: x):
		if default != noDefault:
			default = str.join('\n\t', map(lambda kv: '%s => %s' % kv, default.items()))
		return utils.parseDict(self.get(section, item, default, mutable, noVar), parser)


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
					utils.eprint('\nFound some changes in the config file, which will only apply')
					utils.eprint('to the current task after a reinitialization:\n')
				outputLine = '[%s] %s = %s' % (section, option, value.replace('\n', '\n\t'))
				outputLine += QM(len(outputLine) > 60, '\n', '') + '  (old value: %s)' % oldValue.replace('\n', '\n\t')
				utils.eprint(outputLine)
				flag = True
		unused = sorted(filter(lambda x: x not in self.accessed, self.content))
		utils.vprint('There are %s unused config options!' % len(unused))
		for (section, option) in unused:
			utils.vprint('\t[%s] %s = %s' % (section, option, self.content[(section, option)]), 1)
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
