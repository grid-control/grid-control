import os
from plugin_basic import *
from plugin_meta import *
from plugin_file import *
from grid_control import AbstractObject, QM, utils

class PluginManager(AbstractObject):
	def __init__(self, config, section):
		self.source = None
		self.paramPath = os.path.join(config.workDir, 'params.dat.gz')
		self.cachePath = os.path.join(config.workDir, 'params.map.gz')

		if os.path.exists(self.paramPath):
			if config.opts.init and not config.opts.resync:
				utils.eprint('Re-Initialization will overwrite the current mapping between jobs and parameter/dataset content! This can lead to invalid results!')
				if utils.getUserBool('Do you want to perform a syncronization between the current mapping and the new one to avoid this?', True):
					config.opts.resync = True
		elif config.opts.init and config.opts.resync:
			config.opts.resync = False


	def getSource(self, doInit, doResync):
		log = None
		if not self.source:
			self.source = RNGParaPlugin()
		if not doResync and not doInit and os.path.exists(self.cachePath): # Get old mapping
			log = utils.ActivityLog('Loading cached parameter information')
			self.source = GCCacheParaPlugin(self.cachePath, self.source)
		if doResync and os.path.exists(self.paramPath): # Perform sync
			log = utils.ActivityLog('Syncronizing parameter information')
			oldSource = GCDumpParaPlugin(self.paramPath)
			self.source = self.source.doResync(oldSource)
		if doResync or doInit: # Write current state
			log = utils.ActivityLog('Saving parameter information')
			GCDumpParaPlugin.write(self.paramPath, self.source)
			GCCacheParaPlugin.write(self.cachePath, self.source)
		# Display plugin structure
		def displayPlugin(plugin, level = 1):
			utils.vprint(('\t' * level) + plugin.__class__.__name__, 1)
			if hasattr(plugin, 'plugins'):
				for (n, p) in plugin.plugins:
					displayPlugin(p, level + 1)
		displayPlugin(self.source)
		return self.source
PluginManager.dynamicLoaderPath()


class BasicPluginManager(PluginManager):
	def __init__(self, config, section):
		self.plugins = []
		PluginManager.__init__(self, config, section)

		# Get constants from [constants]
		for cName in filter(lambda o: not o.endswith(' lookup'), config.getOptions('constants')):
			self.addConstantPlugin('constants', cName, cName.upper())
		# Get constants from [<Module>] constants
		for cName in map(str.strip, config.getList(section, 'constants', [])):
			self.addConstantPlugin(section, cName, cName)
		# Random number variables
		self.plugins.append(RNGParaPlugin())
		for (idx, seed) in enumerate(self.getSeeds(config)):
			self.plugins.append(CounterParaPlugin('SEED_%d' % idx, seed))
		self.repeat = config.getInt(section, 'repeat', 1)

	def addConstantPlugin(self, section, cName, varName):
		lookupVar = config.get(section, '%s lookup' % cName, '')
		if lookupVar:
			self.plugins.append(LookupParaPlugin(varName, config.getDict(section, cName, {}), lookupVar))
		else:
			self.plugins.append(ConstParaPlugin(varName, config.get(section, cName, '').strip()))

	def getSeeds(self, config):
		seeds = map(int, config.getList('jobs', 'seeds', []))
		nseeds = config.getInt('jobs', 'nseeds', 10)
		if len(seeds) == 0:
			# args specified => gen seeds
			newSeeds = str.join(' ', map(lambda x: str(random.randint(0, 10000000)), range(nseeds)))
			seeds = map(int, config.getTaskDict().get('seeds', newSeeds).split())
			utils.vprint('Using random seeds... %s' % seeds, once = True)
		config.getTaskDict().write({'seeds': str.join(' ', map(str, seeds))})
		return seeds

	def getSource(self, doInit, doResync):
		# Syncronize with existing parameters
		self.source = CombinePlugin(*(self.plugins + [RequirementParaPlugin()]))
		if self.repeat > 1:
			self.source = RepeatParaPlugin(self.source, self.repeat)
		# Sort variable dependencies
		self.source.resolveDeps()
		return PluginManager.getSource(self, doInit, doResync)
