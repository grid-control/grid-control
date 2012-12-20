import os
from psource_basic import *
from psource_meta import *
from psource_file import *
from psource_data import *
from grid_control import AbstractObject, QM, utils

class ParameterFactory(AbstractObject):
	def __init__(self, config, sections):
		self.static = config.getBool(sections, 'static parameters', False)
		self.paramPath = os.path.join(config.workDir, 'params.dat.gz')
		self.cachePath = os.path.join(config.workDir, 'params.map.gz')

		if config.opts.init and self.static:
			utils.eprint("WARNING: Static parameter mode switched on! Changes to parameter/dataset content can't be recovered!")
		elif os.path.exists(self.paramPath):
			if config.opts.init and not config.opts.resync:
				utils.eprint('Re-Initialization will overwrite the current mapping between jobs and parameter/dataset content! This can lead to invalid results!')
				if utils.getUserBool('Do you want to perform a syncronization between the current mapping and the new one to avoid this?', True):
					config.opts.resync = True
		elif config.opts.init and config.opts.resync:
			config.opts.resync = False


	def _getRawSource(self, parent):
		return parent


	def getSource(self, doInit, doResync):
		source = self._getRawSource(RNGParameterSource())
		if DataParameterSource.datasets and not DataParameterSource.datasetSources:
			source = CrossParameterSource(source, DataParameterSource.create())
		if not doResync and not doInit and os.path.exists(self.cachePath): # Get old mapping
			activity = utils.ActivityLog('Loading cached parameter information')
			source = GCCacheParameterSource(self.cachePath, source)
		elif not doInit and os.path.exists(self.paramPath): # Perform sync
			activity = utils.ActivityLog('Syncronizing parameter information')
			oldSource = GCDumpParameterSource(self.paramPath)
			source = source.doResync(oldSource)

		if (doResync or doInit) and not self.static: # Write current state
			activity = utils.ActivityLog('Saving parameter information')
			GCDumpParameterSource.write(self.paramPath, source)
			GCCacheParameterSource.write(self.cachePath, source)
		# Display plugin structure
		source.show()
		return source
ParameterFactory.dynamicLoaderPath()


class BasicParameterFactory(ParameterFactory):
	def __init__(self, config, sections):
		(self.constSources, self.lookupSources) = ([], [])
		ParameterFactory.__init__(self, config, sections)

		# Get constants from "[constants]"
		for cName in filter(lambda o: not o.endswith(' lookup'), config.getOptions('constants')):
			self._addConstantPlugin(config, 'constants', cName, cName.upper())
		# Get constants from "[<Module>] constants"
		for cName in map(str.strip, config.getList(sections, 'constants', [])):
			self._addConstantPlugin(config, sections, cName, cName)
		# Random number variables
		for (idx, seed) in enumerate(self._getSeeds(config)):
			self.constSources.append(CounterParameterSource('SEED_%d' % idx, seed))
		self.repeat = config.getInt(sections, 'repeat', 1)


	def _addConstantPlugin(self, config, sections, cName, varName):
		lookupVar = config.get(sections, '%s lookup' % cName, '')
		if lookupVar:
			self.lookupSources.append(LookupParameterSource(varName, config.getDict(sections, cName, {}), lookupVar))
		else:
			self.constSources.append(ConstParameterSource(varName, config.get(sections, cName, '').strip()))


	def _getSeeds(self, config):
		seeds = map(int, config.getList('jobs', 'seeds', []))
		nseeds = config.getInt('jobs', 'nseeds', 10)
		if len(seeds) == 0:
			# args specified => gen seeds
			newSeeds = str.join(' ', map(lambda x: str(random.randint(0, 10000000)), range(nseeds)))
			seeds = map(int, str(config.getTaskDict().get('seeds', newSeeds)).split())
			utils.vprint('Using random seeds... %s' % seeds, once = True)
		config.getTaskDict().write({'seeds': str.join(' ', map(str, seeds))})
		return seeds


	def _getRawSource(self, parent):
		source = ZipLongParameterSource(parent, RequirementParameterSource(), *self.constSources)
		if self.repeat > 1:
			source = RepeatParameterSource(source, self.repeat)
		return ParameterFactory._getRawSource(self, source)
