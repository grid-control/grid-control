from grid_control import datasets, monitoring

datasets.DataProvider.providers.update({'DBSApiv2': 'dbs'})
datasets.DataProvider.moduleMap['DBSApiv2'] = 'provider_dbsv2.DBSApiv2'
monitoring.Monitoring.moduleMap['dashboard'] = 'dashboard.DashBoard'
