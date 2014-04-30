#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
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

from grid_control import datasets, monitoring

datasets.DataProvider.moduleMap['DASProvider'] = 'provider_das.DASProvider'
datasets.DataProvider.moduleMap['DBS2Provider'] = 'provider_cms.DBS2Provider'
datasets.DataProvider.moduleMap['DBS3Provider'] = 'provider_dbsv3.DBS3Provider'
datasets.DataProvider.moduleMap['DBSApiv2'] = datasets.DataProvider.moduleMap['DBS2Provider']
datasets.DataProvider.moduleMap['dbs'] = datasets.DataProvider.moduleMap['DBS3Provider']
datasets.DataProvider.providers.update({'DBSApiv3': 'dbs'})

monitoring.Monitoring.moduleMap['dashboard'] = 'dashboard.DashBoard'

datasets.InfoScanner.moduleMap.update(dict(map(lambda x: (x, 'scanner_cmssw.%s' % x),
	['ObjectsFromCMSSW', 'MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath', 'FilterEDMFiles'])))
datasets.GCProvider.stageDir.update({'CMSSW': ['ObjectsFromCMSSW'], 'CMSSW_Advanced': ['ObjectsFromCMSSW']})
datasets.GCProvider.stageFile.update({'CMSSW': ['MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath'],
	'CMSSW_Advanced': ['MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath']})
