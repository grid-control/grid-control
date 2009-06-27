from grid_control import ConfigError, Job, utils
from grid_wms import GridWMS

class Glite(GridWMS):
	def __init__(self, config, opts, module):
		utils.deprecated("Please use the GliteWMS backend for grid jobs!")
		GridWMS.__init__(self, config, opts, module, 'glite')

		self._submitExec = utils.searchPathFind('glite-job-submit')
		self._statusExec = utils.searchPathFind('glite-job-status')
		self._outputExec = utils.searchPathFind('glite-job-output')
		self._cancelExec = utils.searchPathFind('glite-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })
