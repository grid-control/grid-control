from grid_control import ConfigError, RethrowError, utils
import sys, os, logging, ConfigParser, socket

class ConfigFiller(object):
	pass


# Config filler which collects data from config files
class FileConfigFiller(ConfigFiller):
	def __init__(self, container, configFiles):
		for configFile in configFiles:
			self.parseFile(container, configFile)

	def parseFile(self, container, configFile, defaults = None):
		try:
			configFile = utils.resolvePath(configFile, ErrorClass = ConfigError)
			log = logging.getLogger(('config.%s' % utils.getRootName(configFile)).rstrip('.'))
			log.log(logging.INFO1, 'Reading config file %s' % configFile)
			for line in map(lambda x: x.rstrip() + '=:', open(configFile, 'r').readlines()):
				if line.startswith('[') or line.lstrip().startswith(';'):
					continue # skip section and comment lines
				# Abort if non-indented line with ":" preceeding "=" was found
				if (line.lstrip() == line) and (line.find(":") < line.find("=")):
					raise ConfigError('Invalid config line:\n\t%s\nPlease use "key = value" syntax or indent values!' % line)
			# Parse with python config parser
			parser = ConfigParser.ConfigParser(defaults)
			parser.readfp(open(configFile, 'r'))
			# Parse include files
			if parser.has_option('global', 'include'):
				includeFiles = parser.get('global', 'include').split('#')[0].split(';')[0]
				for includeFile in utils.parseList(includeFiles, None):
					self.parseFile(container, includeFile, parser.defaults())
			# Store config settings
			for section in parser.sections():
				for option in parser.options(section):
					if (section, option) != ('global', 'include'):
						value_list = parser.get(section, option).splitlines() # Strip comments
						value_list = map(lambda l: l.rsplit(';', 1)[0].strip(), value_list)
						value_list = filter(lambda l: l != '', value_list)
						container.setEntry(section, option, str.join('\n', value_list), configFile)
		except:
			raise RethrowError('Error while reading configuration file "%s"!' % configFile, ConfigError)


# Config filler which collects data from default config files
class DefaultFilesConfigFiller(FileConfigFiller):
	def __init__(self, container):
		# Collect host / user / installation specific config files
		host = socket.gethostbyaddr(socket.gethostname())[0]
		hostCfg = map(lambda c: utils.pathGC('config/%s.conf' % host.split('.', c)[-1]), range(host.count('.') + 1, 0, -1))
		defaultCfg = ['/etc/grid-control.conf', '~/.grid-control.conf', utils.pathGC('config/default.conf')]
		fqConfigFiles = map(lambda p: utils.resolvePath(p, mustExist = False), hostCfg + defaultCfg)
		FileConfigFiller.__init__(self, container, filter(os.path.exists, fqConfigFiles))


# Config filler which collects data from command line arguments
class OptsConfigFiller(ConfigFiller):
	def __init__(self, container, optParser):
		defaultCmdLine = container.getEntry('global', 'cmdargs', '').value
		(opts, args) = optParser.parse_args(args = defaultCmdLine.split() + sys.argv[1:])
		def setConfigFromOpt(section, option, value):
			if value != None:
				container.setEntry(section, option, str(value), '<cmdline>')
		for (option, value) in {'max retry': opts.maxRetry, 'action': opts.action,
				'continuous': opts.continuous, 'selected': opts.selector}.items():
			setConfigFromOpt('jobs', option, value)
		setConfigFromOpt('global', 'gui', opts.gui)

		# Allow to override config options on the command line:
		for uopt in opts.override:
			try:
				section, tmp = tuple(uopt.lstrip('[').split(']', 1))
				option, value = tuple(map(str.strip, tmp.split('=', 1)))
				container.setEntry(section, option, value, '<cmdline override>')
			except:
				raise RethrowError('Unable to parse option %s' % uopt, ConfigError)


# Config filler which collects data from dictionary
class DictConfigFiller(ConfigFiller):
	def __init__(self, container, configDict):
		for section in configDict:
			for option in configDict[section]:
				container.setEntry(section, option, configDict[section][option], '<dict>')
