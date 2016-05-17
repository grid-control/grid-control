# | Copyright 2010-2016 Karlsruhe Institute of Technology
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

import os
from grid_control import utils
from grid_control.config import ConfigError
from grid_control.datasets import DataProvider
from grid_control.utils.parsing import strDictLong
from grid_control_cms.cmssw import CMSSW
from grid_control_cms.lumi_tools import formatLumi, parseLumiFilter, strLumi
from python_compat import imap, lmap, set, sorted

class CMSSW_Advanced(CMSSW):
	configSections = CMSSW.configSections + ['CMSSW_Advanced']

	def __init__(self, config, name):
		self._name = name # needed for changeView calls before the constructor
		head = [(0, 'Nickname')]

		# Mapping between nickname and config files:
		self._nmCfg = config.getLookup('nickname config', {}, defaultMatcher = 'regex',
			parser = lambda x: lmap(str.strip, x.split(',')), strfun = lambda x: str.join(',', x))
		if not self._nmCfg.empty():
			if 'config file' in config.getOptions():
				raise ConfigError("Please use 'nickname config' instead of 'config file'")
			allConfigFiles = sorted(set(utils.flatten(self._nmCfg.get_values())))
			config.set('config file', str.join('\n', allConfigFiles))
			head.append((1, 'Config file'))

		# Mapping between nickname and constants - only display - work is handled by the 'normal' parameter factory
		nmCName = config.getList('nickname constants', [], onChange = None)
		param_config = config.changeView(viewClass = 'TaggedConfigView', setClasses = None, setNames = None, addSections = ['parameters'])
		param_config.set('constants', str.join(' ', nmCName), '+=')
		for cName in nmCName:
			param_config.set(cName + ' matcher', 'regex')
			param_config.set(cName + ' lookup', 'DATASETNICK')
			head.append((cName, cName))

		# Mapping between nickname and lumi filter - only display - work is handled by the 'normal' lumi filter
		if ('lumi filter' in config.getOptions()) and ('nickname lumi filter' in config.getOptions()):
			raise ConfigError('Please use "lumi filter" exclusively')
		config.set('lumi filter matcher', 'regex')
		config.set('lumi filter', strDictLong(config.getDict('nickname lumi filter', {}, onChange = None)))
		self._nmLumi = config.getLookup('lumi filter', {}, parser = parseLumiFilter, strfun = strLumi, onChange = None)
		if not self._nmLumi.empty():
			head.append((2, 'Lumi filter'))

		CMSSW.__init__(self, config, name)
		self._displaySetup(config.getWorkPath('datacache.dat'), head)


	def _displaySetup(self, dsPath, head):
		if os.path.exists(dsPath):
			nickNames = set()
			for block in DataProvider.loadFromFile(dsPath).getBlocks():
				nickNames.add(block[DataProvider.Nickname])
			utils.vprint('Mapping between nickname and other settings:', -1)
			report = []
			for nick in sorted(nickNames):
				lumi_filter_str = formatLumi(self._nmLumi.lookup(nick, '', is_selector = False))
				if len(lumi_filter_str) > 4:
					nice_lumi_filter = '%s ... %s (%d entries)' % (lumi_filter_str[0], lumi_filter_str[-1], len(lumi_filter_str))
				else:
					nice_lumi_filter = str.join(', ', lumi_filter_str)
				config_files = self._nmCfg.lookup(nick, '', is_selector = False)
				tmp = {0: nick, 1: str.join(', ', imap(os.path.basename, config_files)), 2: nice_lumi_filter}
				lookupvars = {'DATASETNICK': nick}
				for src in self._pm.lookupSources:
					src.fillParameterInfo(None, lookupvars)
				tmp.update(lookupvars)
				report.append(tmp)
			utils.printTabular(head, report, 'cl')


	def getTaskConfig(self):
		# Remove config file variable from the global settings
		data = CMSSW.getTaskConfig(self)
		data.pop('CMSSW_CONFIG')
		return data


	def getJobConfig(self, jobNum):
		data = CMSSW.getJobConfig(self, jobNum)
		configFiles = self._nmCfg.lookup(data.get('DATASETNICK'), [], is_selector = False)
		data['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, configFiles))
		return data
