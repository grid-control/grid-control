from grid_control import utils
from grid_wms import GridWMS

class Glite(GridWMS):
	def __init__(self, config, module, monitor):
		utils.deprecated('Please use the GliteWMS backend for grid jobs!')
		GridWMS.__init__(self, config, module, monitor, 'glite')

		self._submitExec = utils.resolveInstallPath('glite-job-submit')
		self._statusExec = utils.resolveInstallPath('glite-job-status')
		self._outputExec = utils.resolveInstallPath('glite-job-output')
		self._cancelExec = utils.resolveInstallPath('glite-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })
