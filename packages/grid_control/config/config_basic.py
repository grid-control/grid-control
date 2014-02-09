import os, inspect, logging, ConfigParser
from grid_control import *
from python_compat import set, sorted
from container_base import noDefault, standardConfigForm, ConfigContainer
from container_basic import BasicConfigContainer
from config_base import ConfigBase

# return canonized section/option tuple
def cleanSO(section, option):
	if isinstance(section, list): # clean section lists
		section = utils.uniqueListRL(map(standardConfigForm, section))
	else:
		section = standardConfigForm(section)
	if isinstance(option, list): # clean option lists
		option = utils.uniqueListRL(map(standardConfigForm, option))
	else:
		option = standardConfigForm(option)
	return (section, option)


# Change handler to notify about impossible changes
def changeImpossible(config, old_obj, cur_obj, cur_entry, obj2str):
	raise ConfigError('It is *not* possible to change "%s" from %s to %s!' %
		(cur_entry.format_opt(), obj2str(old_obj), obj2str(cur_obj)))


# Change handler to trigger re-inits
class changeInitNeeded:
	def __init__(self, option):
		self._option = option

	def __call__(self, config, old_obj, cur_obj, cur_entry, obj2str):
		log = logging.getLogger('config.onChange.%s' % self._option)
		raw_config = config.getScoped(None)
		interaction_def = raw_config.getBool('interactive', 'default', True, onChange = None)
		interaction_opt = raw_config.getBool('interactive', self._option, interaction_def, onChange = None)
		if interaction_opt:
			if utils.getUserBool('The option "%s" was changed from the old value:' % cur_entry.format_opt() +
				'\n\t%s\nto the new value:\n\t%s\nDo you want to abort?' % (obj2str(old_obj), obj2str(cur_obj)), False):
				raise ConfigError('Abort due to unintentional config change!')
			if not utils.getUserBool('A partial reinitialization (same as --reinit %s) is needed to apply this change! Do you want to continue?' % self._option, True):
				log.log(logging.INFO1, 'Using stored value %s for option %s' % (obj2str(cur_obj), cur_entry.format_opt()))
				return old_obj
		config.set('init %s' % self._option, 'True')
		config.set('init config', 'True', section = 'global') # This will trigger a write of the new options
		return cur_obj


# Validation handler to check for variables in string
def validNoVar(section, option, obj):
	return utils.checkVar(obj, '[%s] "%s" may not contain variables.' % (section, option))


def fmtStack(stack):
	for frame in stack:
		caller = frame[0].f_locals.get('self', None)
		if caller and caller.__class__.__name__ not in ['Config', 'NewConfig', 'BaseConfigResolver', 'ResolvedConfigBase']:
			return frame[0].f_locals.get('self', None).__class__.__name__
	return 'main'


class BaseConfigResolver:
	def __init__(self, logger):
		self._logger = logger

	# Return options in matching sections
	def getOptions(self, container, section):
		if isinstance(section, list):
			result = []
			for s in section: # Collect options from multiple sections
				result += container.getOptions(s, getDefault = False)
			return utils.uniqueListRL(sorted(result))
		return sorted(container.getOptions(section, getDefault = False))

	# Return target for specified section and option
	def getTarget(self, container, section, option):
		if isinstance(section, list):
			section = section[0] # set in most specific section
		if isinstance(option, list):
			option = option[0]   # set in most recent option
		return (standardConfigForm(section), standardConfigForm(option))

	# Return source from specified section and option
	def getSource(self, container, section, option):
		def flat(value): # Small function to flatten section / option lists
			if isinstance(value, list):
				return str.join('|', value)
			return value
		self._logger.log(logging.DEBUG1, 'Query config section(s) [%s] for option(s) "%s"' % (flat(section), flat(option)))

		# Handle multi section get calls, use least specific setting for error message
		if isinstance(section, list):
			for specific in filter(lambda s: self.hasOption(container, s, option), section):
				# return result of most specific section 
				return self.getSource(container, specific, option)
			# trigger error message with most general section
			return self.getSource(container, section[-1], option)
		# Handle multi option get calls, use first option for error message
		if isinstance(option, list):
			for specific in filter(lambda i: self.hasOption(container, section, i), option):
				# return result of most recent option
				return self.getSource(container, section, specific)
			# trigger error message with most recent option
			return self.getSource(container, section, option[0])
		self._logger.log(logging.DEBUG2, 'Config get request from: %s' % fmtStack(inspect.stack()))
		return (standardConfigForm(section), standardConfigForm(option))

	# Check if section and option is in config
	def hasOption(self, container, section, option):
		if isinstance(option, list):
			return True in map(lambda i: self.hasOption(container, section, i), option)
		return standardConfigForm(option) in self.getOptions(container, section)


# Class returned from getScoped calls - it is only using the new getter API
class ResolvedConfigBase(ConfigBase):
	def __init__(self, config, scope, forward = []):
		(self._config, self._scope, self._forward) = (config, utils.uniqueListRL(map(str.lower, scope)), forward)
		def mySet(option, value, *args, **kwargs):
			return self._config.set(scope, option, value, *args, **kwargs)
		def myGet(desc, obj2str, str2obj, def2obj, option, *args, **kwargs):
			primedResolver = lambda cc: self._config._resolver.getSource(cc, self._scope, option)
			kwargs["caller"] = self
			return self._config.getTyped(desc, obj2str, str2obj, def2obj, primedResolver, *args, **kwargs)
		def myIter():
			return self._config.getOptions(scope)
		ConfigBase.__init__(self, mySet, myGet, myIter, config._baseDir, config._workDir)
		for attr in forward: # Forward specified attributes from main config to this instance
			setattr(self, attr, getattr(config, attr))

	# Setter function with option section override
	def set(self, *args, **kwargs):
		section = kwargs.pop('section', None)
		if section:
			return self.getScoped(None).set(section, *args, **kwargs)
		return ConfigBase.set(self, *args, **kwargs)

	# Factory for more specific instances
	def getScoped(self, scope_left = [], scope_right = []):
		if scope_left == None:
			return self._config # Allow to get unspecific instance
		return ResolvedConfigBase(self._config, scope_left + self._scope + scope_right, self._forward)

	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self._scope)


# Main config interface
class NewConfig(ConfigBase):
	def __init__(self, configFile = None, configDict = {}, optParser = None, configHostSpecific = True):
		(self._allowSet, self._oldCfg) = (True, None)
		# Read in the current configuration from config file, manual dictionary, command line and "config" dir
		self._curCfg = BasicConfigContainer('current', configFile, configDict, optParser, configHostSpecific)
		# Future: allow hooks / other classes derived from BaseConfigContainer to dynamically provide options?

		if configFile:
			# use the directory of the config file as base directory for file searches in getPath
			self._baseDir = os.path.dirname(utils.resolvePath(configFile))
			self.configFile = os.path.join(self._baseDir, os.path.basename(configFile))
		else: # self.configFile is only used to forward the filename to monitoring scripts
			(self._baseDir, self.configFile) = ('.', 'gc.conf')
		confName = utils.getRootName(self.configFile)
		self._logger = logging.getLogger(('config.%s' % confName).rstrip('.'))
		self._resolver = BaseConfigResolver(self._logger)

		# Setup config interface for following get*,... calls
		def mySet(section, option, value, *args, **kwargs):
			self._logger.debug("old style call from %s [%s] %s" % (fmtStack(inspect.stack()), section, option))
			primedResolver = lambda cc: self._resolver.getTarget(cc, section, option)
			return self.setChecked(primedResolver, value, *args, **kwargs)
		def myGet(desc, obj2str, str2obj, def2obj, section, option, *args, **kwargs):
			self._logger.debug("old style call from %s [%s] %s" % (fmtStack(inspect.stack()), section, option))
			primedResolver = lambda cc: self._resolver.getSource(cc, section, option)
			return self.getTyped(desc, obj2str, str2obj, def2obj, primedResolver, *args, **kwargs)
		def myIter(section):
			self._logger.debug("old style call from %s [%s]" % (fmtStack(inspect.stack()), section))
			return self._resolver.getOptions(self._curCfg, section)
		ConfigBase.__init__(self, mySet, myGet, myIter, self._baseDir, None)

		# Determine work directory 
		wdBase = self.getPath('global', 'workdir base', self._baseDir, mustExist = False)
		self._workDir = self.getPath('global', 'workdir', os.path.join(wdBase, 'work.' + confName),
			mustExist = False, markDefault = False) # "markDefault = False" forces writeout in both dumps

		# Determine and load stored config settings
		self._flatCfgPath = self.getWorkPath('current.conf') # Minimal config file
		self._oldCfgPath = self.getWorkPath('work.conf') # Config file with saved settings
		if os.path.exists(self._oldCfgPath):
			logging.getLogger('config.stored').propagate = False
			self._oldCfg = BasicConfigContainer('stored', configFile = self._oldCfgPath)

		# Get persistent variables - only possible after self._oldCfg was set!
		self.confName = self.get('global', 'config id', confName, persistent = True)
		# Specify variables to forward to scoped config instances
		self._forward = ['configFile', 'confName', 'opts']


	def freezeConfig(self, writeConfig = True):
		self._allowSet = False
		# Inform the user about unused options
		unused = list(self._curCfg.iterContent(accessed = False))
		self._logger.log(logging.INFO1, 'There are %s unused config options!' % len(unused))
		for entry in unused:
			self._logger.log(logging.INFO1, '\t%s' % entry.format(printSection = True))
		if writeConfig:
			# Write user friendly, flat config file and config file with saved settings
			self._curCfg.write(open(self._flatCfgPath, 'w'), printDefault = False, printUnused = False)
			fp_work = open(self._oldCfgPath, 'w')
			fp_work.write('; ==> DO NOT EDIT THIS FILE! <==\n; This file is used to find config changes!\n')
			self._curCfg.write(fp_work, printDefault = True, printUnused = True)


	def setChecked(self, resolver, value, override = True, append = False, source = '<dynamic>'):
		self._logger.log(logging.DEBUG2, 'Config set request from: %s' % fmtStack(inspect.stack()))
		(section, option) = resolver(self._curCfg)
		if not self._allowSet:
			raise APIError('Invalid runtime config override: [%s] %s = %s' % (section, option, value))
		if not override:
			option += '?'
		elif append:
			option += '+'
		self._logger.log(logging.INFO3, 'Setting dynamic key [%s] %s = %s' % (section, option, value))
		self._curCfg.setEntry(section, option, value, source, markAccessed = True)


	# Get a typed config value from the container
	def getTyped(self, desc, obj2str, str2obj, def2obj, resolver, default_obj = noDefault,
			onChange = changeImpossible, onValid = None, persistent = False, markDefault = True, caller = None):
		(section, option) = resolver(self._curCfg)
		# First transform default into string if applicable
		default_str = noDefault
		if default_obj != noDefault:
			try:
				if def2obj:
					default_obj = def2obj(default_obj)
			except:
				raise APIError('Unable to convert default object: %r' % default_obj)
			try:
				default_str = obj2str(default_obj)
			except:
				raise APIError('Unable to get string representation of default object: %r' % default_obj)

		old_entry = None
		if self._oldCfg:
			(section_old, option_old) = resolver(self._oldCfg)
			old_entry = self._oldCfg.getEntry(section_old, option_old, default_str, raiseMissing = False)
			if old_entry and persistent: # Override current default value with stored value
				default_str = old_entry.value
				self._logger.log(logging.INFO2, 'Applying persistent %s' % old_entry.format(printSection = True))
		cur_entry = self._curCfg.getEntry(section, option, default_str, markDefault = markDefault)
		try:
			cur_obj = str2obj(cur_entry.value)
			cur_entry.value = obj2str(cur_obj)
		except:
			raise RethrowError('Unable to parse %s: [%s] %s = %s' % (desc, section, option, cur_entry.value), ConfigError)

		# Notify about changes
		if onChange and old_entry:
			try:
				old_obj = str2obj(old_entry.value)
			except:
				raise RethrowError('Unable to parse stored %s: [%s] %s = %s' % (desc, section, option, old_entry.value), ConfigError)
			if not (old_obj == cur_obj):
				# Main reason for caller support is to localize reinits to affected modules
				caller = QM(caller, caller, self)
				cur_obj = onChange(caller, old_obj, cur_obj, cur_entry, obj2str)
				cur_entry.value = obj2str(cur_obj)
		if onValid:
			return onValid(section, option, cur_obj)
		return cur_obj


	def write(self, *args, **kwargs):
		self._curCfg.write(*args, **kwargs)


	# Return config class instance with given scope and the ability to return further specialized instances
	def getScoped(self, sections):
		return ResolvedConfigBase(self, sections, self._forward)


# For compatibility with old work directories
class Config(NewConfig):
	def __init__(self, configFile = None, configDict = {}, optParser = None, configHostSpecific = True):
		NewConfig.__init__(self, configFile, configDict, optParser, configHostSpecific)
		persistencyFile = self.getWorkPath('task.dat')
		# read old persistency file - and set appropriate config options
		if os.path.exists(persistencyFile):
			persistencyDict = utils.PersistentDict(persistencyFile, ' = ')
			def setPersistentSetting(section, key):
				if key in persistencyDict:
					value = persistencyDict.get(key)
					self._oldCfg.setEntry(section, key, value, '<persistency file>', markAccessed = True)
			setPersistentSetting('task', 'task id')
			setPersistentSetting('task', 'task date')
			setPersistentSetting('parameters', 'parameter hash')
			setPersistentSetting('jobs', 'seeds')
