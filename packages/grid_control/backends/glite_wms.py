import tempfile
from grid_control import utils, RuntimeError
from grid_wms import GridWMS

class GliteWMS(GridWMS):
	def __init__(self, config, wmsName = 'glite-wms'):
		GridWMS.__init__(self, config, wmsName)

		self._delegateExec = utils.resolveInstallPath('glite-wms-job-delegate-proxy')
		self._submitExec = utils.resolveInstallPath('glite-wms-job-submit')
		self._statusExec = utils.resolveInstallPath('glite-wms-job-status')
		self._outputExec = utils.resolveInstallPath('glite-wms-job-output')
		self._cancelExec = utils.resolveInstallPath('glite-wms-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config': self._configVO})
		self._useDelegate = config.getBool(self._getSections('backend'), 'use delegate', True, mutable=True)


	def bulkSubmissionBegin(self):
		self._submitParams.update({ '-d': None })
		if not self._useDelegate:
			self._submitParams.update({ '-a': ' ' })
			return True
		log = tempfile.mktemp('.log')
		try:
			dID = 'GCD' + md5(str(time())).hexdigest()[:10]
			activity = utils.ActivityLog('creating delegate proxy for job submission')
			proc = utils.LoggedProcess(self._delegateExec, '%s -d %s --noint --logfile "%s"' %
				(utils.QM(self._configVO, '--config "%s"' % self._configVO, ''), dID, log))

			output = proc.getOutput(wait = True)
			if ('glite-wms-job-delegate-proxy Success' in output) and (dID in output):
				self._submitParams.update({ '-d': dID })
			del activity

			if proc.wait() != 0:
				proc.logError(self.errorLog, log = log)
			return (self._submitParams.get('-d', None) != None)
		finally:
			utils.removeFiles([log])


	def submitJobs(self, jobNumList, module):
		if self.bulkSubmissionBegin():
			for submitInfo in GridWMS.submitJobs(self, jobNumList, module):
				yield submitInfo
		else:
			raise RuntimeError('Unable to delegate proxy!')
