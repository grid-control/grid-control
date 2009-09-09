import sys, os, time, tempfile
from grid_control import ConfigError, Job, utils
from grid_wms import GridWMS
from glite import Glite

class GliteWMS(Glite):
	def __init__(self, config, module, monitor):
		GridWMS.__init__(self, config, module, monitor, 'glite-wms')

		self._delegateExec = utils.searchPathFind('glite-wms-job-delegate-proxy')
		self._submitExec = utils.searchPathFind('glite-wms-job-submit')
		self._statusExec = utils.searchPathFind('glite-wms-job-status')
		self._outputExec = utils.searchPathFind('glite-wms-job-output')
		self._cancelExec = utils.searchPathFind('glite-wms-job-cancel')
		self._submitParams.update({'-r': self._ce, '--config': self._configVO })


	def bulkSubmissionBegin(self, jobs):
		log = tempfile.mktemp('.log')
		try:
			params = ''
			if self._configVO != '':
				params += ' --config %s' % utils.shellEscape(self._configVO)

			self._submitParams.update({ '-d': None })
			activity = utils.ActivityLog('creating delegate proxy for job submission')
			proc = utils.LoggedProcess(self._delegateExec, "%s -a --noint --logfile %s" %
				(params, utils.shellEscape(log)))

			for line in map(str.strip, proc.iter(self.config.opts)):
				try:
					(left, right) = line.split(':', 1)
					if left.endswith('identifier') and not right.strip().startswith('-'):
						self._submitParams.update({ '-d': right.strip() })
				except:
					pass

			retCode = proc.wait()
			del activity

			if retCode != 0:
				self.logError(proc, log)
			if self._submitParams.get('-d', None) != None:
				return True
			return False
		finally:
			self.cleanup([log])
