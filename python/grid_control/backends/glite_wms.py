import tempfile
from grid_control import utils
from grid_wms import GridWMS

class GliteWMS(GridWMS):
	def __init__(self, config, module, monitor):
		GridWMS.__init__(self, config, module, monitor, 'glite-wms')

		self._delegateExec = utils.resolveInstallPath('glite-wms-job-delegate-proxy')
		self._submitExec = utils.resolveInstallPath('glite-wms-job-submit')
		self._statusExec = utils.resolveInstallPath('glite-wms-job-status')
		self._outputExec = utils.resolveInstallPath('glite-wms-job-output')
		self._cancelExec = utils.resolveInstallPath('glite-wms-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config': self._configVO })


	def bulkSubmissionBegin(self, jobs):
		self._submitParams.update({ '-d': None })
		log = tempfile.mktemp('.log')
		try:
			activity = utils.ActivityLog('creating delegate proxy for job submission')
			proc = utils.LoggedProcess(self._delegateExec, '%s -d %s --noint --logfile "%s"' %
				(utils.QM(self._configVO, '--config "%s"' % self._configVO, ''), self.module.taskID, log))
			retCode, output, error = proc.getAll()
			output = str.join('', output)
			if ('glite-wms-job-delegate-proxy Success' in output) and (self.module.taskID in output):
				self._submitParams.update({ '-d': self.module.taskID })
			del activity

			if retCode != 0:
				proc.logError(self.errorLog, log = log)
			return (self._submitParams.get('-d', None) != None)
		finally:
			utils.removeFiles([log])
