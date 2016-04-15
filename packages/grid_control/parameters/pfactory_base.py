# | Copyright 2012-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import random
from grid_control.config import Matcher
from grid_control.gc_plugin import NamedPlugin
from grid_control.parameters.config_param import ParameterConfig
from grid_control.parameters.padapter import ParameterAdapter
from grid_control.parameters.psource_base import ParameterSource
from hpfwk import Plugin
from python_compat import ifilter, irange, lmap

class ParameterFactory(NamedPlugin):
	configSections = NamedPlugin.configSections + ['parameters']
	tagName = 'parameters'

	def __init__(self, config, name):
		NamedPlugin.__init__(self, config, name)
		self.adapter = config.get('parameter adapter', 'TrackedParameterAdapter')
		self._paramConfig = ParameterConfig(config.changeView(setSections = ['parameters']), self.adapter != 'TrackedParameterAdapter')


	def _getRawSource(self, parent):
		return parent


	def getSource(self, config):
		DataParameterSource = Plugin.getClass('DataParameterSource')
		source = self._getRawSource(ParameterSource.createInstance('RNGParameterSource'))
		if DataParameterSource.datasetsAvailable and not DataParameterSource.datasetsUsed:
			source = ParameterSource.createInstance('CrossParameterSource', DataParameterSource.create(), source)
		return ParameterAdapter.createInstance(self.adapter, config, source)


class BasicParameterFactory(ParameterFactory):
	def __init__(self, config, name):
		(self.constSources, self.lookupSources) = ([], [])
		ParameterFactory.__init__(self, config, name)

		# Get constants from [constants <tags...>]
		configConstants = config.changeView(viewClass = 'TaggedConfigView',
			setClasses = None, setSections = ['constants'], addTags = [self])
		for cName in ifilter(lambda o: not o.endswith(' lookup'), configConstants.getOptions()):
			self._addConstantPSource(configConstants, cName, cName.upper())
		# Get constants from [<Module>] constants
		for cName in config.getList('constants', []):
			self._addConstantPSource(config, cName, cName)
		# Random number variables
		configJobs = config.changeView(addSections = ['jobs'])
		nseeds = configJobs.getInt('nseeds', 10)
		newSeeds = lmap(lambda x: str(random.randint(0, 10000000)), irange(nseeds))
		for (idx, seed) in enumerate(configJobs.getList('seeds', newSeeds, persistent = True)):
			ps = ParameterSource.createInstance('CounterParameterSource', 'SEED_%d' % idx, int(seed))
			self.constSources.append(ps)
		self.repeat = config.getInt('repeat', 1, onChange = None) # ALL config.x -> paramconfig.x !


	def _addConstantPSource(self, config, cName, varName):
		lookupVar = config.get('%s lookup' % cName, '', onChange = None)
		if lookupVar:
			matcher = Matcher.createInstance(config.get('%s matcher' % cName, 'start', onChange = None), config, cName)
			content = config.getDict(cName, {}, onChange = None)
			content_fixed = {}
			content_order = lmap(lambda x: (x,), content[1])
			for key in content[0]:
				content_fixed[(key,)] = (content[0][key],)
			ps = ParameterSource.createInstance('SimpleLookupParameterSource', varName, [lookupVar], [matcher], (content_fixed, content_order))
			self.lookupSources.append(ps)
		else:
			ps = ParameterSource.createInstance('ConstParameterSource', varName, config.get(cName).strip())
			self.constSources.append(ps)


	def _getRawSource(self, parent):
		source_list = self.constSources + [parent, ParameterSource.createInstance('RequirementParameterSource')] + self.lookupSources
		source = ParameterSource.createInstance('ZipLongParameterSource', *source_list)
		if self.repeat > 1:
			source = ParameterSource.createInstance('RepeatParameterSource', source, self.repeat)
		return ParameterFactory._getRawSource(self, source)
