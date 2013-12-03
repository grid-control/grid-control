from grid_control import datasets, monitoring

datasets.DataProvider.moduleMap['DBS2Provider'] = 'provider_dbsv2.DBSApiv2'
datasets.DataProvider.moduleMap['DBS3Provider'] = 'provider_dbsv3.DBS3Provider'
datasets.DataProvider.moduleMap['DASProvider'] = 'provider_das.DASProvider'
datasets.DataProvider.moduleMap['DBSApiv2'] = datasets.DataProvider.moduleMap['DBS2Provider']
datasets.DataProvider.moduleMap['dbs'] = datasets.DataProvider.moduleMap['DBS2Provider']
datasets.DataProvider.providers.update({'DBSApiv3': 'dbs'})

monitoring.Monitoring.moduleMap['dashboard'] = 'dashboard.DashBoard'

datasets.InfoScanner.moduleMap.update(dict(map(lambda x: (x, 'scanner_cmssw.%s' % x),
	['ObjectsFromCMSSW', 'MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath', 'FilterEDMFiles'])))
datasets.GCProvider.stageDir.update({'CMSSW': ['ObjectsFromCMSSW'], 'CMSSW_Advanced': ['ObjectsFromCMSSW']})
datasets.GCProvider.stageFile.update({'CMSSW': ['MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath'],
	'CMSSW_Advanced': ['MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath']})
