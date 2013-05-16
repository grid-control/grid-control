from grid_control import QM, utils, ConfigError, RethrowError
from wms import WMS
from wms_multi import MultiWMS

class WMSFactory:
	def __init__(self, config):
		self.config = config
		wmsUsrList = config.getList('global', 'backend', ['grid'], mutable = True)
		wmsSetupList = map(lambda wmsEntry: utils.optSplit(wmsEntry, ':', empty = None), wmsUsrList)
		try:
			wmsDict = {'grid': 'GliteWMS', 'inactive': 'InactiveWMS',
				'local': config.get('local', 'wms', self._guessWMS())}
			wmsSetupList = map(lambda (wmsClass, wmsName): (wmsDict.get(wmsClass, wmsClass), wmsName), wmsSetupList)
			self.wmsObjList = map(lambda (wmsClass, wmsName): WMS.open(wmsClass, config, wmsName), wmsSetupList)
		except:
			raise RethrowError('Invalid backend selected! (%s)' % str.join(",", wmsUsrList))
		if len(self.wmsObjList) > 1:
			self.wmsMultiClass = config.get('global', 'backend manager', 'MultiWMS', mutable = True)


	def getWMS(self):
		if len(self.wmsObjList) == 0:
			raise ConfigError('No backend selected!')
		elif len(self.wmsObjList) == 1:
			return self.wmsObjList[0]
		else:
			return MultiWMS.open(self.wmsMultiClass, self.config, self.wmsObjList[0], self.wmsObjList)


	def _guessWMS(self):
		wmsCmdList = [('OGE', 'sgepasswd'), ('PBS', 'pbs-config'), ('OGE', 'qsub'),
			('LSF', 'bsub'), ('SLURM', 'job_slurm'), ('PBS', 'sh')]
		for wms, cmd in wmsCmdList:
			try:
				utils.resolveInstallPath(cmd)
				return wms
			except:
				pass
