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
from grid_control.parameters.psource_base import ParameterSource
from grid_control.parameters.psource_lookup import createLookupHelper
from hpfwk import AbstractError, Plugin
from python_compat import identity, ifilter, imap, irange, lfilter, lmap

class ParameterFactory(NamedPlugin):
	tagName = 'parameters'

	def __init__(self, config, name):
		NamedPlugin.__init__(self, config, name)
		self._paramConfig = ParameterConfig(config)

	def getSource(self):
		raise AbstractError


class BasicParameterFactory(ParameterFactory):
	def __init__(self, config, name):
		ParameterFactory.__init__(self, config, name)
		(self._constSources, self._lookupSources, self._nestedSources) = ([], [], [])

		# Random number variables
		jobs_config = config.changeView(addSections = ['jobs'])
		for name in jobs_config.getList('random variables', ['JOB_RANDOM'], onChange = None):
			self._constSources.append(ParameterSource.createInstance('RNGParameterSource', name))
		nseeds = jobs_config.getInt('nseeds', 10)
		newSeeds = lmap(lambda x: str(random.randint(0, 10000000)), irange(nseeds))
		for (idx, seed) in enumerate(jobs_config.getList('seeds', newSeeds, persistent = True)):
			ps = ParameterSource.createInstance('CounterParameterSource', 'SEED_%d' % idx, int(seed))
			self._constSources.append(ps)

		# Get constants from [constants <tags...>]
		constants_config = config.changeView(viewClass = 'TaggedConfigView',
			setClasses = None, setSections = ['constants'], setNames = None, addTags = [self])
		constants_pconfig = ParameterConfig(constants_config)
		for cName in ifilter(lambda o: not o.endswith(' lookup'), constants_config.getOptions()):
			constants_config.set('%s type' % cName, 'verbatim', '?=')
			self._registerPSource(constants_pconfig, cName.upper())

		param_config = config.changeView(addSections = ['parameters'])

		# Get constants from [<Module>] constants
		task_pconfig = ParameterConfig(param_config)
		for cName in param_config.getList('constants', []):
			config.set('%s type' % cName, 'verbatim', '?=')
			self._registerPSource(task_pconfig, cName)

		# Get global repeat value from 'parameters' section
		self._repeat = param_config.getInt('repeat', 1, onChange = None)
		self._req = config.getBool('translate requirements', True, onChange = None)

		self._pfactory = config.getPlugin('parameter factory', 'SimpleParameterFactory:parameters',
			cls = ParameterFactory, inherit = True)


	def _registerPSource(self, pconfig, varName):
		def replace_nonalnum(value):
			if str.isalnum(value):
				return value
			return ' '
		lookup_str = pconfig.get(varName, 'lookup', '')
		lookup_list = lfilter(identity, str.join('', imap(replace_nonalnum, lookup_str)).split())
		for (doElevate, PSourceClass, args) in createLookupHelper(pconfig, [varName], lookup_list):
			if doElevate: # switch needs elevation beyond local scope
				self._nestedSources.append((PSourceClass, args))
			else:
				ps = PSourceClass(*args)
				if ps.depends():
					self._lookupSources.append(ps)
				else:
					self._constSources.append(ps)


	def _useAvailableDataSource(self, source):
		DataParameterSource = Plugin.getClass('DataParameterSource')
		if DataParameterSource.datasetsAvailable and not DataParameterSource.datasetsUsed:
			if source is not None:
				return ParameterSource.createInstance('CrossParameterSource', DataParameterSource.create(), source)
			return DataParameterSource.create()
		return source


	def getSource(self):
		source_list = self._constSources + [self._pfactory.getSource()] + self._lookupSources
		source = ParameterSource.createInstance('ZipLongParameterSource', *source_list)
		for (PSourceClass, args) in self._nestedSources:
			source = PSourceClass(source, *args)
		if self._req:
			req_source = ParameterSource.createInstance('RequirementParameterSource')
			source = ParameterSource.createInstance('ZipLongParameterSource', source, req_source)
		source = self._useAvailableDataSource(source)
		return ParameterSource.createInstance('RepeatParameterSource', source, self._repeat)
