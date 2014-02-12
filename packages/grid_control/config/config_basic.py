import os, inspect, logging, ConfigParser
from grid_control import *
from container_base import noDefault, standardConfigForm, ResolvingConfigContainer
from config_base import ConfigBase
from filler_base import FileConfigFiller, DefaultFilesConfigFiller, OptsConfigFiller, DictConfigFiller
from config_handlers import *

class TaggedConfig(ConfigBase):
	def __init__(self, *args, **kwargs):
		ConfigBase.__init__(self, *args, **kwargs)
		self._initSelector()

	def _initSelector(self, refClass = None, refNames = [], refTags = [], refSections = []):
		(self._selClass, self._selSections) = (refClass, refSections)
		(self._selNames, self._selTags) = (refNames, refTags)

	# Collect sections based on class hierarchie
	def _collectSections(self, clsCurrent):
		if clsCurrent and (clsCurrent != NamedObject):
			for section in clsCurrent.getConfigSections():
				yield section.lower()
			for clsBase in clsCurrent.__bases__:
				for section in self._collectSections(clsBase):
					yield section.lower()

	def _optimizeSectionList(self, sList):
		result = []
		for section in sList:
			if section not in result:
				result.append(section)
		if result and sList:
			if result[-1] != sList[-1]:
				result.append(sList[-1])
		return result

	# to be replaced by clone...
	def myclone(self, refClass, refNames, refTags, refSections):
		refSectionsCls = self._optimizeSectionList(list(self._collectSections(refClass)))
		refSections = self._optimizeSectionList(map(str.lower, refSections))
		def selectorFilter(option, *args, **kwargs):
			if not isinstance(option, list):
				option = [option]
			return ((refSectionsCls + refSections, option, map(str.lower, refNames), refTags), args, kwargs)
		tmp = self.clone(selectorFilter = selectorFilter, ClassTemplate = TaggedConfig)
		tmp._initSelector(refClass, refNames, refTags, refSections)
		tmp.confName = self.confName # FIXME: move into API
		tmp.opts = self.opts
		return tmp


	def newClass(self, refClass, refName):
		return self.myclone(refClass, [refName], self._selTags, self._selSections)

	def addSections(self, refSections):
		return self.myclone(self._selClass, self._selNames, self._selTags, refSections + self._selSections)
	def newSections(self, refSections):
		return self.myclone(self._selClass, self._selNames, self._selTags, refSections)

	def addTags(self, refTagInstances):
		refTags = map(lambda inst: (inst.tagName.lower(), inst.getObjectName().lower()), refTagInstances)
		selTags = filter(lambda (sk, sv): sk not in map(lambda t: t[0], refTags), self._selTags)
		return self.myclone(self._selClass, self._selNames, refTags + selTags, self._selSections)

	def addNames(self, refNames):
		return self.myclone(self._selClass, self._selNames + refNames, self._selTags, self._selSections)

	def __repr__(self): # FIXME: improve output
		return '%s(class = %r, sections = %r, names = %r, tags = %r)' % (self.__class__.__name__,
			self._optimizeSectionList(list(self._collectSections(self._selClass))),
			self._selSections, self._selNames, dict(self._selTags))


# Main config interface
class Config(TaggedConfig):
	def __init__(self, configFile = None, configDict = {}, optParser = None, configHostSpecific = True):
		self._allowSet = True
		# Read in the current configuration from config file, manual dictionary, command line and "config" dir
		curCfg = ResolvingConfigContainer('current')
		DefaultFilesConfigFiller(curCfg)
		if configFile:
			FileConfigFiller(curCfg, [configFile])
		if optParser:
			OptsConfigFiller(curCfg, optParser)
		DictConfigFiller(curCfg, configDict)
		# TODO: make container and filler chain somehow configurable (eg. to dynamically provide options)

		if configFile:
			# use the directory of the config file as base directory for file searches in getPath
			self._baseDir = os.path.dirname(utils.resolvePath(configFile))
			self.configFile = os.path.join(self._baseDir, os.path.basename(configFile))
		else: # self.configFile is only used to forward the filename to monitoring scripts
			(self._baseDir, self.configFile) = ('.', 'gc.conf')
		confName = utils.getRootName(self.configFile)
		self._logger = logging.getLogger(('config.%s' % confName).rstrip('.'))
		TaggedConfig.__init__(self, confName, curCfg, self._baseDir)

		# Determine work directory 
		wdBase = self.getPath('global', 'workdir base', self._baseDir, mustExist = False)
		pathWork = self.getPath('global', 'workdir', os.path.join(wdBase, 'work.' + confName),
			mustExist = False, markDefault = False) # "markDefault = False" forces writeout in both dumps
		self.init(pathWork = pathWork)

		# Determine and load stored config settings
		self._flatCfgPath = self.getWorkPath('current.conf') # Minimal config file
		self._oldCfgPath = self.getWorkPath('work.conf') # Config file with saved settings
		if os.path.exists(self._oldCfgPath):
			logging.getLogger('config.stored').propagate = False
			oldCfg = ResolvingConfigContainer('stored')
			FileConfigFiller(oldCfg, [self._oldCfgPath])
			self.init(oldCfg = oldCfg)

		# Get persistent variables - only possible after self._oldCfg was set!
		self.confName = self.get('global', 'config id', confName, persistent = True)
		# Specify variables to forward to scoped config instances
		self._forward = ['configFile', 'confName', 'opts']


	def freezeConfig(self, writeConfig = True):
		self._allowSet = False
		# Inform the user about unused options
		unused = list(filter(lambda entry: entry.accessed == False, self._curCfg.iterContent()))
		self._logger.log(logging.INFO1, 'There are %s unused config options!' % len(unused))
		for entry in unused:
			self._logger.log(logging.INFO1, '\t%s' % entry.format(printSection = True))
		if writeConfig:
			# Write user friendly, flat config file and config file with saved settings
			self.write(open(self._flatCfgPath, 'w'), printDefault = False, printUnused = False)
			fp_work = open(self._oldCfgPath, 'w')
			fp_work.write('; ==> DO NOT EDIT THIS FILE! <==\n; This file is used to find config changes!\n')
			self.write(fp_work, printDefault = True, printUnused = True)


# For compatibility with old work directories
class CompatConfig(Config):
	def __init__(self, configFile = None, configDict = {}, optParser = None, configHostSpecific = True):
		Config.__init__(self, configFile, configDict, optParser, configHostSpecific)
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
