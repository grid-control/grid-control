import os, ConfigParser
from grid_control import *

class Config:
	def __init__(self, configFile):
		self.protocol = {}
		try:
			# try to parse config file
			self.parser = ConfigParser.ConfigParser()
			self.parser.read(configFile)
		except ConfigParser.Error, e:
			raise ConfigError("Configuration file `%s' contains an error: %s" % (configFile, e.message))

		# use the directory of the config file as base directory
		self.baseDir = os.path.abspath(os.path.normpath(os.path.dirname(configFile)))
		self.confName = str.join("", os.path.basename(configFile).split(".")[:-1])
		self.workDirDefault = os.path.join(self.baseDir, 'work.%s' % self.confName)

		# Read default values and reread main config file
		includeFile = self.getPath("global", "include", '')
		if includeFile != '':
			self.parser.read(includeFile)
			self.parser.read(configFile)


	def parseLine(self, parser, section, item):
		# Split into lines, remove comments and return merged result
		lines = parser.get(section, item).splitlines()
		lines = map(lambda x: x.split(';')[0].strip(), lines)
		return str.join("\n", filter(lambda x: x != '', lines))


	def get(self, section, item, default = None, volatile = False):
		# Make protocol of config queries - flag inconsistencies
		if section not in self.protocol:
			self.protocol[section] = {}
		if item in self.protocol[section]:
			if self.protocol[section][item][1] != default:
				raise ConfigError("Inconsistent default values: [%s] %s" % (section, item))
		# Default value helper function
		def tryDefault(errorMessage):
			if default != None:
				utils.vprint("Using default value [%s] %s = %s" % (section, item, str(default)), 1)
				self.protocol[section][item] = (default, default, volatile)
				return default
			raise ConfigError(errorMessage)
		# Read from config file or return default if possible
		try:
			value = self.parseLine(self.parser, section, item)
			self.protocol[section][item] = (value, default, volatile)
			return value
		except ConfigParser.NoSectionError:
			return tryDefault("No section %s in config file." % section)
		except ConfigParser.NoOptionError:
			return tryDefault("No option %s in section %s of config file." % (item, section))
		except:
			raise ConfigError("Parse error in option %s of config file section %s." % (item, section))


	def getPaths(self, section, item, default = None, volatile = False):
		pathRaw = self.get(section, item, default, volatile)
		if pathRaw == '':
			return ''
		def formatPath(path):
			path = os.path.expanduser(path.strip())	# ~/bla -> /home/user/bla
			path = os.path.normpath(path)   # xx/../yy -> yy
			if not os.path.isabs(path):	# ./lala -> /foo/bar/lala
				basePath = os.path.join(self.baseDir, path)
				if not os.path.exists(basePath) and os.path.exists(utils.pathGC(path)):
					path = utils.pathGC(path)
				else:
					path = basePath
			return path
		return map(formatPath, pathRaw.splitlines())


	def getPath(self, section, item, default = None, volatile = False):
		tmp = self.getPaths(section, item, default, volatile)
		if len(tmp) == 0:
			return ''
		return tmp[0]


	def getInt(self, section, item, default = None, volatile = False):
		return int(self.get(section, item, default, volatile))


	def getBool(self, section, item, default = None, volatile = False):
		value = self.get(section, item, default, volatile)
		return str(value).lower() in ('yes', 'y', 'true', 't', 'ok', '1')


	# Compare this config object to another config file
	# Return true in case non-volatile parameters are changed
	def needInit(self, saveConfigPath):
		if not os.path.exists(saveConfigPath):
			return False
		saveConfig = ConfigParser.ConfigParser()
		saveConfig.read(saveConfigPath)
		flag = False
		for section in self.protocol:
			for (key, (value, default, volatile)) in self.protocol[section].iteritems():
				try:
					oldValue = self.parseLine(saveConfig, section, key)
				except:
					oldValue = default
				if (str(value).strip() != str(oldValue).strip()) and not volatile:
					if not flag:
						print "\nFound some changes in the config file, which will only apply"
						print "to the current task after a reinitialization:\n"
					print "[%s] %s = %s" % (section, key, value),
					if len(str(oldValue)) + len(str(value)) > 60:
						print
					print "  (old value: %s)" % oldValue
					flag = True
		if flag:
			print
		return flag
