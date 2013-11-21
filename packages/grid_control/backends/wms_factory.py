from grid_control import QM, utils, ConfigError, RethrowError
from wms import WMS
from wms_multi import MultiWMS

class WMSFactory:
	def __init__(self, config):
		self.config = config
		wmsUsrList = config.getList('global', 'backend', ['grid'], onChange = None)
		def parseUserWMS(wmsEntry):
			wmsClass, wmsName = utils.optSplit(wmsEntry, ':', empty = None)
			wmsDict = {'grid': 'GliteWMS', 'inactive': 'InactiveWMS'}
			wmsClass = wmsDict.get(wmsClass, wmsClass)
			if wmsClass == 'local':
				wmsClass = config.get('local', 'wms', self._guessWMS())
			return (wmsClass, wmsName)
		wmsSetupList = map(parseUserWMS, wmsUsrList)
		try:
			self.wmsObjList = map(lambda (wmsClass, wmsName): WMS.open(wmsClass, config, wmsName), wmsSetupList)
		except:
			raise RethrowError('Invalid backend selected! (%s)' % str.join(",", wmsUsrList))
		if len(self.wmsObjList) > 1:
			self.wmsMultiClass = config.get('global', 'backend manager', 'MultiWMS', onChange = None)


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
