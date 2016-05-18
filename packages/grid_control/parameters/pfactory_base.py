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
from grid_control.gc_plugin import NamedPlugin
from grid_control.parameters.config_param import ParameterConfig
from grid_control.parameters.padapter import ParameterAdapter
from grid_control.parameters.psource_base import ParameterSource
from grid_control.parameters.psource_lookup import createLookupHelper
from hpfwk import Plugin
from python_compat import identity, ifilter, imap, irange, lfilter, lmap

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
		(self.constSources, self.lookupSources, self.elevateSources) = ([], [], [])
		ParameterFactory.__init__(self, config, name)

		# Get constants from [constants <tags...>]
		constants_config = config.changeView(viewClass = 'TaggedConfigView',
			setClasses = None, setSections = ['constants'], addTags = [self])
		constants_pconfig = ParameterConfig(constants_config, self.adapter != 'TrackedParameterAdapter')
		for cName in ifilter(lambda o: not o.endswith(' lookup'), constants_config.getOptions()):
			constants_config.set('%s type' % cName, 'verbatim', '?=')
			self._registerPSource(constants_pconfig, cName.upper())
		# Get constants from [<Module>] constants
		task_pconfig = ParameterConfig(config, self.adapter != 'TrackedParameterAdapter')
		for cName in config.getList('constants', []):
			config.set('%s type' % cName, 'verbatim', '?=')
			self._registerPSource(task_pconfig, cName)
		# Random number variables
		jobs_config = config.changeView(addSections = ['jobs'])
		nseeds = jobs_config.getInt('nseeds', 10)
		newSeeds = lmap(lambda x: str(random.randint(0, 10000000)), irange(nseeds))
		for (idx, seed) in enumerate(jobs_config.getList('seeds', newSeeds, persistent = True)):
			ps = ParameterSource.createInstance('CounterParameterSource', 'SEED_%d' % idx, int(seed))
			self.constSources.append(ps)
		self.repeat = config.getInt('repeat', 1, onChange = None) # ALL config.x -> paramconfig.x !


	def _registerPSource(self, pconfig, varName):
		def replace_nonalnum(value):
			if str.isalnum(value):
				return value
			return ' '
		lookup_str = pconfig.get(varName, 'lookup', '')
		lookup_list = lfilter(identity, str.join('', imap(replace_nonalnum, lookup_str)).split())
		for (doElevate, PSourceClass, args) in createLookupHelper(pconfig, [varName], lookup_list):
			if doElevate: # switch needs elevation beyond local scope
				self.elevateSources.append((PSourceClass, args))
			else:
				ps = PSourceClass(*args)
				if ps.depends():
					self.lookupSources.append(ps)
				else:
					self.constSources.append(ps)


	def _getRawSource(self, parent):
		req_source = ParameterSource.createInstance('RequirementParameterSource')
		source_list = self.constSources + [parent] + self.lookupSources
		if not self.elevateSources:
			source_list.append(req_source)
		source = ParameterSource.createInstance('ZipLongParameterSource', *source_list)
		for (PSourceClass, args) in self.elevateSources:
			source = PSourceClass(source, *args)
		if self.elevateSources:
			source = ParameterSource.createInstance('ZipLongParameterSource', source, req_source)
		if self.repeat > 1:
			source = ParameterSource.createInstance('RepeatParameterSource', source, self.repeat)
		return ParameterFactory._getRawSource(self, source)
