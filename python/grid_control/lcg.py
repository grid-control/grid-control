import os
from grid_control import ConfigError, WMS, Glite, utils

class LCG(Glite):

	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, init)

		self._submitExec = utils.searchPathFind('edg-job-submit')
		self._statusExec = utils.searchPathFind('edg-job-status')
		self._outputExec = utils.searchPathFind('edg-job-get-output')

		self._configVO = config.getPath('lcg', 'config-vo', '')
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config-vo file '%s' does not exist." % self._configVO)

		try:
			self._ce = config.get('lcg', 'ce')
		except:
			self._ce = None
