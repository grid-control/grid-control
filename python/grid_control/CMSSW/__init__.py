from grid_control import datasets, module, monitoring

module.Module.dynamicLoaderPath(["grid_control.CMSSW"])
datasets.DataProvider.dynamicLoaderPath(["grid_control.CMSSW"])
datasets.DataProvider.moduleMap["DBSApiv2"] = "provider_dbsv2.DBSApiv2"
monitoring.Monitoring.dynamicLoaderPath(["grid_control.CMSSW"])
monitoring.Monitoring.moduleMap["dashboard"] = "dashboard.DashBoardMonitoring"
