import os
from plugin_basic import *
from plugin_meta import *
from plugin_file import *
from grid_control import AbstractObject, QM, utils

class PluginManager(AbstractObject):
	def __init__(self, config, section):
		self.source = None
		self.pPath = os.path.join(config.workDir, 'params.dat')

	def getSource(self, doInit, doResync):
		if not self.source:
			self.source = RNGParaPlugin()
		if doResync and os.path.exists(self.pPath):
			oldSource = GCDumpParaPlugin(self.pPath)
			self.source = self.source.resync(oldSource)
		if doResync or doInit:
			GCDumpParaPlugin.write(self.pPath, self.source)
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
			utils.vprint('Using random seeds... %s' % seeds, -1, once = True)
		config.getTaskDict().write({'seeds': str.join(' ', map(str, seeds))})
		return seeds

	def getSource(self, doInit, doResync):
		# Sort variable dependencies - trivial implementation for now
		self.plugins = sorted(self.plugins, key = lambda p: p.getParameterDeps())
		# Syncronize with existing parameters
		self.source = RequirementParaPlugin(CombinePlugin(*self.plugins))
		if self.repeat:
			self.source = RepeatParaPlugin(self.source, self.repeat)
		return PluginManager.getSource(self, doInit, doResync)
