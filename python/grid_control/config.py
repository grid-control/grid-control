import os, ConfigParser, utils

from grid_control import ConfigError

class Config:
	def __init__(self, fp):
		try:
			# try to parse config file
			parser = ConfigParser.ConfigParser()
			parser.readfp(fp)
		except ConfigParser.Error, e:
			raise ConfigError("Configuration file `%s' contains an error: %s" % (fp, e.message))

		self.name = fp.name
		self.parser = parser

		# use the directory of the config file as base directory
		dir = os.path.dirname(fp.name)
		dir = os.path.normpath(dir)
		self.baseDir = os.path.abspath(dir)

		includeFile = self.getPath("global", "include", '')
		if includeFile != '':
			parser.read(includeFile)
			parser.read(fp.name)


	def get(self, section, item, default = None):
		try:
			return self.parser.get(section, item)
		except ConfigParser.NoSectionError:
			if default != None:
				if (utils.verbosity() > 1) and (default != 'FAIL'):
					print "Using default value [%s] %s = %s" % (section, item, str(default))
				return default
			raise ConfigError("No section %s in config file." % section)
		except ConfigParser.NoOptionError:
			if default != None:
				if (utils.verbosity() > 1) and (default != 'FAIL'):
					print "Using default value [%s] %s = %s" % (section, item, str(default))
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
