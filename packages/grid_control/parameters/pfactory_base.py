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

import random, logging
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.parameters.config_param import ParameterConfig
from grid_control.parameters.psource_base import NullParameterSource, ParameterError, ParameterSource
from grid_control.parameters.psource_lookup import createLookupHelper
from hpfwk import AbstractError
from python_compat import identity, ifilter, imap, irange, lfilter, lmap, sorted

class ParameterFactory(ConfigurablePlugin):
	tagName = 'parameters'

	def getSource(self):
		raise AbstractError


class UserParameterFactory(ParameterFactory):
	def __init__(self, config):
		ParameterFactory.__init__(self, config)
		self._log = logging.getLogger('parameters.factory')
		self._paramConfig = ParameterConfig(config)
		self._pExpr = config.get('parameters', '', onChange = None)

	def _getUserSource(self, pExpr, respository):
		raise AbstractError

	def getSource(self, respository):
		if not self._pExpr:
			return NullParameterSource()
		self._log.debug('Parsing parameter expression: %s', repr(self._pExpr))
		try:
			source = self._getUserSource(self._pExpr, respository)
		except:
			raise ParameterError('Unable to parse parameter expression %r' % self._pExpr)
		self._log.debug('Parsed parameter source: %s', repr(source))
		return source


class BasicParameterFactory(ParameterFactory):
	def __init__(self, config):
		ParameterFactory.__init__(self, config)
		(self._constSources, self._lookupSources, self._nestedSources) = ([], [], [])

		# Random number variables
		jobs_config = config.changeView(addSections = ['jobs'])
		self._randomVariables = jobs_config.getList('random variables', ['JOB_RANDOM'], onChange = None)
		nseeds = jobs_config.getInt('nseeds', 10)
		newSeeds = lmap(lambda x: str(random.randint(0, 10000000)), irange(nseeds))
		self._randomSeeds = jobs_config.getList('seeds', newSeeds, persistent = True)

		# Get constants from [constants <tags...>]
		constants_config = config.changeView(viewClass = 'TaggedConfigView',
			setClasses = None, setSections = ['constants'], setNames = None)
		constants_pconfig = ParameterConfig(constants_config)
		for cName in ifilter(lambda opt: ' ' not in opt, constants_config.getOptions()):
			constants_config.set('%s type' % cName, 'verbatim', '?=')
			self._registerPSource(constants_pconfig, cName.upper())

		param_config = config.changeView(viewClass = 'TaggedConfigView',
			setClasses = None, addSections = ['parameters'], inheritSections = True)

		# Get constants from [<Module>] constants
		task_pconfig = ParameterConfig(param_config)
		for cName in param_config.getList('constants', []):
			config.set('%s type' % cName, 'verbatim', '?=')
			self._registerPSource(task_pconfig, cName)

		# Get global repeat value from 'parameters' section
		self._repeat = param_config.getInt('repeat', 1, onChange = None)
		self._req = param_config.getBool('translate requirements', True, onChange = None)
		self._pfactory = param_config.getPlugin('parameter factory', 'SimpleParameterFactory', cls = ParameterFactory)


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
				ps = PSourceClass.createInstance(PSourceClass.__name__, *args)
				if ps.depends():
					self._lookupSources.append(ps)
				else:
					self._constSources.append(ps)


	def _useAvailableDataSource(self, source, repository):
		used_sources = source.getUsedSources()
		unused_data_sources = []
		for (srcName, dataSource) in sorted(repository.items()):
			if srcName.startswith('dataset:') and (dataSource not in used_sources):
				unused_data_sources.append(dataSource)
		if unused_data_sources:
			chained_data_sources = ParameterSource.createInstance('ChainParameterSource', *unused_data_sources)
			source = ParameterSource.createInstance('CrossParameterSource', chained_data_sources, source)
		return source


	def getSource(self, repository):
		source_list = []
		for name in self._randomVariables:
			source_list.append(ParameterSource.createInstance('RNGParameterSource', name))
		for (idx, seed) in enumerate(self._randomSeeds):
			source_list.append(ParameterSource.createInstance('CounterParameterSource', 'SEED_%d' % idx, int(seed)))
		source_list += self._constSources + [self._pfactory.getSource(repository)] + self._lookupSources
		source = ParameterSource.createInstance('ZipLongParameterSource', *source_list)
		for (PSourceClass, args) in self._nestedSources:
			source = PSourceClass(source, *args)
		if self._req:
			req_source = ParameterSource.createInstance('RequirementParameterSource')
			source = ParameterSource.createInstance('ZipLongParameterSource', source, req_source)
		source = self._useAvailableDataSource(source, repository)
		return ParameterSource.createInstance('RepeatParameterSource', source, self._repeat)
