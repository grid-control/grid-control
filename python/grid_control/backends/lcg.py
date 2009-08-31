import os
from grid_control import ConfigError, utils
from grid_wms import GridWMS
from glite import Glite

class LCG(Glite):
	def __init__(self, config, module, monitor):
		utils.deprecated("Please use the GliteWMS backend for grid jobs!")
		GridWMS.__init__(self, config, module, monitor, 'lcg')

		self._submitExec = utils.searchPathFind('edg-job-submit')
		self._statusExec = utils.searchPathFind('edg-job-status')
		self._outputExec = utils.searchPathFind('edg-job-get-output')
		self._cancelExec = utils.searchPathFind('edg-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config-vo': self._configVO })


	def storageReq(self, sites):
		def makeMember(member):
			return '(target.GlueSEUniqueID == %s)' \
			       % self._jdlEscape(member)
		if not len(sites):
			return None
		else:
			return 'anyMatch(other.storage.CloseSEs, ' + str.join(' || ', map(makeMember, sites)) + ')'
