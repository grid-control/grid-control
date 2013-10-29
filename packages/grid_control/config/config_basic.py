import os, inspect, logging, ConfigParser
from grid_control import *
from python_compat import *
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


def fmtStack(stack):
	for frame in stack:
		caller = frame[0].f_locals.get('self', None)
		if caller and caller.__class__.__name__ != 'Config':
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
		# Handle multi section get calls, use least specific setting for error message
		self._logger.log(logging.DEBUG1, 'Query config section(s) "%s" for option(s) "%s"' % (section, option))
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
	def __init__(self, config, scope):
		(self._config, self._scope) = (config, scope)
		def mySet(option, value, *args, **kwargs):
			return self._config.set(scope, option, value, *args, **kwargs)
		def myGet(desc, obj2str, str2obj, option, *args, **kwargs):
			primedResolver = lambda cc: self._config._resolver.getSource(cc, self._scope, option)
			return self._config.getTyped(desc, obj2str, str2obj, primedResolver, *args, **kwargs)
		def myIter():
			return self._config.getOptions(scope)
		ConfigBase.__init__(self, mySet, myGet, myIter, config._baseDir)

		# Factory for more specific instances
	def getScoped(self, scope_left = [], scope_right = []):
		return ResolvedConfigBase(self._config, scope_left + self._scope + scope_right)


# Main config interface
class NewConfig(ConfigBase):
	def __init__(self, configFile = None, configDict = {}, optParser = None, configHostSpecific = True):
		self._todo_remove_major_change = False
		(self._allowSet, self._oldCfg) = (True, None)
		# Read in the current configuration from config file, manual dictionary, command line and "config" dir
		self._curCfg = BasicConfigContainer('current', configFile, configDict, optParser, configHostSpecific)
		# Future: allow hooks / other classes derived from BaseConfigContainer to dynamically provide options?

		if configFile:
			# use the directory of the config file as base directory for file searches in getPath
			self._baseDir = utils.cleanPath(os.path.dirname(configFile))
			self.configFile = os.path.join(self._baseDir, os.path.basename(configFile))
		else: # self.configFile is only used to forward the filename to monitoring scripts
			(self._baseDir, self.configFile) = ('.', 'gc.conf')
		confName = utils.getRootName(self.configFile)
		self._logger = logging.getLogger(('config.%s' % confName).rstrip('.'))
		self._resolver = BaseConfigResolver(self._logger)

		# Setup config interface for following get*,... calls
		def mySet(section, option, value, *args, **kwargs):
			primedResolver = lambda cc: self._resolver.getTarget(cc, section, option)
			return self.setChecked(primedResolver, value, *args, **kwargs)
		def myGet(desc, obj2str, str2obj, section, option, *args, **kwargs):
			primedResolver = lambda cc: self._resolver.getSource(cc, section, option)
			return self.getTyped_compat(desc, obj2str, str2obj, primedResolver, *args, **kwargs)
		def myIter(section):
			return self._resolver.getOptions(self._curCfg, section)
		ConfigBase.__init__(self, mySet, myGet, myIter, self._baseDir)

		# Determine work directory 
		wdBase = self.getPath('global', 'workdir base', self._baseDir, mustExist = False)
		self.workDir = self.getPath('global', 'workdir', os.path.join(wdBase, 'work.' + confName),
			mustExist = False, markDefault = False) # "markDefault = False" forces writeout in both dumps

		# Determine and load stored config settings
		self._flatCfgPath = os.path.join(self.workDir, 'current.conf') # Minimal config file
		self._oldCfgPath = os.path.join(self.workDir, 'work.conf') # Config file with saved settings
		if os.path.exists(self._oldCfgPath):
			logging.getLogger('config.stored').propagate = False
			self._oldCfg = BasicConfigContainer('stored', configFile = self._oldCfgPath)

		# Get persistent variables - only possible after self._oldCfg was set!
		self.confName = self.get('global', 'config id', confName, persistent = True)


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
		self._logger.log(logging.INFO3, 'Overwrite of option [%s] %s = %s' % (section, option, value))
		self._curCfg.setEntry(section, option, value, source, markAccessed = True)


	# Get a typed config value from the container
	def getTyped(self, desc, obj2str, str2obj, resolver, default_obj,
			onChange = None, onValid = None, persistent = False, markDefault = True):
		(section, option) = resolver(self._curCfg)
		# First transform default into string if applicable
		default_str = noDefault
		if default_obj != noDefault:
			try:
				default_str = obj2str(default_obj)
			except:
				raise APIError('Unable to convert default object: %r' % default_obj)

		old_entry = None
		if self._oldCfg:
			(section_old, option_old) = resolver(self._oldCfg)
			old_entry = self._oldCfg.getEntry(section_old, option_old, default_str)
			if persistent: # Override current default value with stored value
				default_str = old_entry.value
		cur_entry = self._curCfg.getEntry(section, option, default_str, markDefault = markDefault)
		try:
			cur_obj = str2obj(cur_entry.value)
		except:
			raise RethrowError('Unable to parse %s: [%s] %s = %s' % (desc, section, option, cur_entry.value), ConfigError)

		# Notify about changes
		if onChange and old_entry:
			try:
				old_obj = str2obj(old_entry.value)
			except:
				raise RethrowError('Unable to parse stored %s: [%s] %s = %s' % (desc, section, option, old_entry.value), ConfigError)
			if old_obj != cur_obj:
				onChange(old_obj, cur_obj, cur_entry)
		if onValid:
			return onValid(section, option, cur_obj)
		return cur_obj


	def getTyped_compat(self, desc, obj2str, str2obj, resolver, default_obj = noDefault,
			mutable = False, noVar = True, persistent = False, markDefault = True):
		if mutable == False:
			def onChange(old_obj, cur_obj, cur_entry):
				if self.opts.init:
					return
				self._logger.warn('Found some changes in the config file, ' +
					'which will only apply to the current task after a reinitialization:')
				self._logger.warn('[%s] %s' % (cur_entry.section, cur_entry.option))
				self._logger.warn('\told: %r\n\tnew: %r' % (old_obj, cur_obj))
				self._todo_remove_major_change = True
		else:
			onChange = None
		if noVar == True:
			onValid = lambda section, option, obj: \
				utils.checkVar(obj, '[%s] "%s" may not contain variables.' % (section, option))
		else:
			onValid = None
		return self.getTyped(desc, obj2str, str2obj, resolver, default_obj,
			onChange = onChange, onValid = onValid, persistent = persistent, markDefault = markDefault)


	def write(self, *args, **kwargs):
		self._curCfg.write(*args, **kwargs)


	# Return config class instance with given scope and the ability to return further specialized instances
	def getScoped(self, section):
		return ResolvedConfigBase(self, section)


# For compatibility for old work directories
class Config(NewConfig):
	def __init__(self, configFile = None, configDict = {}, optParser = None, configHostSpecific = True):
		NewConfig.__init__(self, configFile, configDict, optParser, configHostSpecific)
		persistencyFile = os.path.join(self.workDir, 'task.dat')
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


# Change handler to notify about impossible changes
def changeImpossible(old_obj, cur_obj, cur_entry):
	raise ConfigError('It is *not* possible to change "[%s] %s" from %r to %r!' %
		(cur_entry.section, cur_entry.option, old_obj, cur_obj))
