import os, ConfigParser, utils
from grid_control import ConfigError

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

		# Read default values and reread main config file
		includeFile = self.getPath("global", "include", '')
		if includeFile != '':
			self.parser.read(includeFile)
			self.parser.read(configFile)


	def get(self, section, item, default = None):
		if not self.protocol.has_key(section):
			self.protocol[section] = {}
		if self.protocol[section].has_key(item):
			if self.protocol[section][item][1] != default:
				raise ConfigError("Inconsistent default values: [%s] %s" % (section, item))
		try:
			value = self.parser.get(section, item).split(';')[0]
			self.protocol[section][item] = (value, default)
			return value
		except ConfigParser.NoSectionError:
			if default != None:
				utils.vprint("Using default value [%s] %s = %s" % (section, item, str(default)), 1)
				self.protocol[section][item] = (default, default)
				return default
			raise ConfigError("No section %s in config file." % section)
		except ConfigParser.NoOptionError:
			if default != None:
				utils.vprint("Using default value [%s] %s = %s" % (section, item, str(default)), 1)
				self.protocol[section][item] = (default, default)
				return default
			raise ConfigError("No option %s in section %s of config file." % (item, section))
		except:
			raise ConfigError("Parse error in option %s of config file section %s." % (item, section))


	def getPath(self, section, item, default = None):
		path = self.get(section, item, default)
		if path == '':
			return ''
		path = os.path.expanduser(path)	# ~/bla -> /home/user/bla
		path = os.path.normpath(path)   # xx/../yy -> yy
		if not os.path.isabs(path):	# ./lala -> /foo/bar/lala
			path = os.path.join(self.baseDir, path)
		return path


	def getInt(self, section, item, default = None):
		return int(self.get(section, item, default))


	def getBool(self, section, item, default = None):
		value = self.get(section, item, default)
		try:
			return bool(int(value))
		except:
			return value.lower() in ('yes', 'y', 'true', 't', 'ok')
