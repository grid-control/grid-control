from grid_control import utils
from grid_wms import GridWMS, jdlEscape

class LCG(GridWMS):
	def __init__(self, config, wmsName = None):
		utils.deprecated('Please use the GliteWMS backend for grid jobs!')
		GridWMS.__init__(self, config, wmsName)

		self._submitExec = utils.resolveInstallPath('edg-job-submit')
		self._statusExec = utils.resolveInstallPath('edg-job-status')
		self._outputExec = utils.resolveInstallPath('edg-job-get-output')
		self._cancelExec = utils.resolveInstallPath('edg-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })


	def storageReq(self, sites):
		fmt = lambda x: '(target.GlueSEUniqueID == %s)' % jdlEscape(x)
		if sites:
			return 'anyMatch(other.storage.CloseSEs, ' + str.join(' || ', map(fmt, sites)) + ')'
