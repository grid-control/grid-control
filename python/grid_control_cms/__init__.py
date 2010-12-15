from grid_control import datasets, monitoring

datasets.DataProvider.providers.update({'DBSApiv2': 'dbs'})
datasets.DataProvider.moduleMap['DBSApiv2'] = 'provider_dbsv2.DBSApiv2'
monitoring.Monitoring.moduleMap['dashboard'] = 'dashboard.DashBoard'

datasets.InfoScanner.moduleMap.update(dict(map(lambda x: (x, 'scanner_cmssw.%s' % x),
	['ObjectsFromCMSSW', 'MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath'])))
datasets.GCProvider.stageDir.update({'CMSSW': ['ObjectsFromCMSSW'], 'CMSSW_Advanced': ['ObjectsFromCMSSW']})
datasets.GCProvider.stageFile.update({'CMSSW': ['MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath'],
	'CMSSW_Advanced': ['MetadataFromCMSSW', 'SEListFromPath', 'LFNFromPath']})
