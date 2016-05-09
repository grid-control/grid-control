# | Copyright 2015-2016 Karlsruhe Institute of Technology
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
from grid_control.datasets.provider_base import DatasetError
from grid_control.datasets.provider_scan import GCProvider
from grid_control.utils.parsing import strGuid

class DBSInfoProvider(GCProvider):
	alias = ['dbsinfo']

	def __init__(self, config, datasetExpr, datasetNick = None, datasetID = 0):
		tmp = ['OutputDirsFromConfig', 'MetadataFromTask']
		if os.path.isdir(datasetExpr):
			tmp = ['OutputDirsFromWork']
		tmp.extend(['JobInfoFromOutputDir', 'ObjectsFromCMSSW', 'FilesFromJobInfo', 'MetadataFromCMSSW',
			'ParentLookup', 'SEListFromPath', 'LFNFromPath', 'DetermineEvents', 'FilterEDMFiles'])
		config.set('scanner', str.join(' ', tmp))
		config.set('include config infos', 'True')
		config.set('parent keys', 'CMSSW_PARENT_LFN CMSSW_PARENT_PFN')
		config.set('events key', 'CMSSW_EVENTS_WRITE')
		GCProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		self._discovery = config.getBool('discovery', False)

	def generateDatasetName(self, key, data):
		if self._discovery:
			return GCProvider.generateDatasetName(self, key, data)
		if 'CMSSW_DATATIER' not in data:
			raise DatasetError('Incompatible data tiers in dataset: %s' % data)
		getPathComponents = lambda path: utils.QM(path, tuple(path.strip('/').split('/')), ())
		userPath = getPathComponents(self.nameDS)

		(primary, processed, tier) = (None, None, None)
		# In case of a child dataset, use the parent infos to construct new path
		for parent in data.get('PARENT_PATH', []):
			if len(userPath) == 3:
				(primary, processed, tier) = userPath
			else:
				try:
					(primary, processed, tier) = getPathComponents(parent)
				except Exception:
					pass
		if (primary is None) and (len(userPath) > 0):
			primary = userPath[0]
			userPath = userPath[1:]

		if len(userPath) == 2:
			(processed, tier) = userPath
		elif len(userPath) == 1:
			(processed, tier) = (userPath[0], data['CMSSW_DATATIER'])
		elif len(userPath) == 0:
			(processed, tier) = ('Dataset_%s' % key, data['CMSSW_DATATIER'])

		rawDS = '/%s/%s/%s' % (primary, processed, tier)
		if None in (primary, processed, tier):
			raise DatasetError('Invalid dataset name supplied: %r\nresulting in %s' % (self.nameDS, rawDS))
		return utils.replaceDict(rawDS, data)

	def generateBlockName(self, key, data):
		return strGuid(key)
