import os
from grid_control import ConfigError, utils
from grid_wms import GridWMS
from glite import Glite

class LCG(Glite):

	def __init__(self, config, opts, module):
		utils.deprecated("Please use the GliteWMS backend for grid jobs!")
		GridWMS.__init__(self, config, opts, module)

		self._submitExec = utils.searchPathFind('edg-job-submit')
		self._statusExec = utils.searchPathFind('edg-job-status')
		self._outputExec = utils.searchPathFind('edg-job-get-output')
		self._cancelExec = utils.searchPathFind('edg-job-cancel')

		self._configVO = config.getPath('lcg', 'config-vo', '')
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config-vo file '%s' does not exist." % self._configVO)

		try:
			self._ce = config.get('lcg', 'ce')
		except:
			self._ce = None


	def storageReq(self, sites):
		def makeMember(member):
			return '(target.GlueSEUniqueID == %s)' \
			       % self._jdlEscape(member)
		if not len(sites):
			return None
		else:
			return 'anyMatch(other.storage.CloseSEs, ' + str.join(' || ', map(makeMember, sites)) + ')'
