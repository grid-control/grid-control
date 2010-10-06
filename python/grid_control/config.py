import os, ConfigParser as cp
from grid_control import *

class Config:
	def __init__(self, configFile = None, configDict = {}):
		(self.allowSet, self.protocol, self.parser) = (True, {}, cp.ConfigParser())
		if configFile:
			# use the directory of the config file as base directory
			self.baseDir = os.path.abspath(os.path.normpath(os.path.dirname(configFile)))
			self.configFile = os.path.join(self.baseDir, os.path.basename(configFile))
			self.confName = str.join("", os.path.basename(configFile).split(".")[:-1])
			self.parseFile(self.parser, configFile)
		else:
			(self.baseDir, self.configFile, self.confName) = ('.', 'gc.conf', 'gc')
		self.workDirDefault = os.path.join(self.baseDir, 'work.%s' % self.confName)

		# Override config settings via dictionary
		for section in configDict:
			for item in configDict[section]:
				self.set(section, item, configDict[section][item])


	def parseFile(self, parser, configFile):
		def parseFileInt(fn):
			try:
				parser.readfp(open(fn, 'r'))
			except:
				raise RethrowError("Error while reading configuration file '%s'!" % fn)
		userDefaultsFile = utils.resolvePath("~/.grid-control.conf", check = False)
		if os.path.exists(userDefaultsFile):
			parseFileInt(userDefaultsFile)
		# Read default values and reread main config file
		for includeFile in self.getPaths("global", "include", ''):
			parseFileInt(includeFile)
		parseFileInt(configFile)


	def parseLine(self, parser, section, item):
		# Split into lines, remove comments and return merged result
		lines = parser.get(section, item).splitlines()
		lines = map(lambda x: x.split(';')[0].strip(), lines)
		return str.join("\n", filter(lambda x: x != '', lines))


	def set(self, section, item, value = None, override = True):
		if not self.allowSet:
			raise APIError("Invalid runtime config override: [%s] %s = %s" % (str(section), str(item), str(value)))
		utils.vprint("Config option was overridden: [%s] %s = %s" % (str(section), str(item), str(value)), 2)
		if not self.parser.has_section(str(section)):
			self.parser.add_section(str(section))
		if (not self.parser.has_option(str(section), str(item))) or override:
			self.parser.set(str(section), str(item), str(value))


	def get(self, section, item, default = None, volatile = False):
		# Make protocol of config queries - flag inconsistencies
		if item in self.protocol.setdefault(section, {}):
			if self.protocol[section][item][1] != default:
				raise ConfigError("Inconsistent default values: [%s] %s" % (section, item))
		# Default value helper function
		def tryDefault(errorMessage):
			if default != None:
				utils.vprint("Using default value [%s] %s = %s" % (section, item, str(default)), 3)
				self.protocol[section][item] = (default, default, volatile)
				return default
			raise ConfigError(errorMessage)
		# Read from config file or return default if possible
		try:
			value = self.parseLine(self.parser, str(section), item)
			self.protocol[section][item] = (value, default, volatile)
			return value
		except cp.NoSectionError:
			return tryDefault("No section %s in config file." % section)
		except cp.NoOptionError:
			return tryDefault("No option %s in section %s of config file." % (item, section))
		except:
			raise ConfigError("Parse error in option %s of config file section %s." % (item, section))


	def getPaths(self, section, item, default = None, volatile = False, check = True):
		value = self.get(section, item, default, volatile)
		return map(lambda x: utils.resolvePath(x, [self.baseDir], check), value.splitlines())


	def getPath(self, section, item, default = None, volatile = False, check = True):
		return (self.getPaths(section, item, default, volatile, check) + [''])[0]


	def getInt(self, section, item, default = None, volatile = False):
		return int(self.get(section, item, default, volatile))


	def getBool(self, section, item, default = None, volatile = False):
		value = self.get(section, item, default, volatile)
		return str(value).lower() in ('yes', 'y', 'true', 't', 'ok', '1', 'on')


	# Compare this config object to another config file
	# Return true in case non-volatile parameters are changed
	def needInit(self, saveConfigPath):
		if not os.path.exists(saveConfigPath):
			return False
		saveConfig = cp.ConfigParser()
		self.parseFile(saveConfig, saveConfigPath)
		flag = False
		for section in self.protocol:
			for (key, (value, default, volatile)) in self.protocol[section].iteritems():
				try:
					oldValue = self.parseLine(saveConfig, section, key)
				except:
					oldValue = default
				if (str(value).strip() != str(oldValue).strip()) and not volatile:
					if not flag:
						utils.eprint("\nFound some changes in the config file, which will only apply")
						utils.eprint("to the current task after a reinitialization:\n")
					utils.eprint("[%s] %s = %s" % (section, key, value), newline = False)
					if len(str(oldValue)) + len(str(value)) > 60:
						utils.eprint()
					utils.eprint("  (old value: %s)" % oldValue)
					flag = True
		if flag:
			utils.eprint()
		return flag
