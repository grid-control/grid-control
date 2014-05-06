#!/usr/bin/env python
#-#  Copyright 2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from gcSupport import *
from grid_control.datasets import DataProvider, ListProvider

config = getConfig(cfgDict = {'nickname check collision': 'False'}, section = 'dataset')
provider = ListProvider(config, 'datacache.dat', None, None)

for blockInfo in provider.getBlocks():
	print '=' * 50
	print 'Dataset', blockInfo[DataProvider.Dataset]
	print 'BlockName', blockInfo[DataProvider.BlockName]
	print 'NEntries', blockInfo[DataProvider.NEntries]
	print 'Locations', blockInfo[DataProvider.Locations]
	print 'Metadata', blockInfo[DataProvider.Metadata] # Available metadata / job variables
	print '-' * 20
	for fileInfo in blockInfo[DataProvider.FileList]:
		print 'File', fileInfo[DataProvider.URL], fileInfo[DataProvider.NEntries]

		continue # remove to see metadata

		metadataInfo = dict(zip(blockInfo[DataProvider.Metadata], fileInfo[DataProvider.Metadata]))
		for metaKey in metadataInfo:
			value = repr(metadataInfo[metaKey])
			if len(value) > 50:
				value = value[:50] + '...'
			print '\t', metaKey, value
		print
