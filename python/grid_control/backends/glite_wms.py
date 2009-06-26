from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO, md5
from grid_control import ConfigError, Job, utils
from grid_wms import GridWMS
from glite import Glite

class GliteWMS(Glite):
	def __init__(self, config, opts, module):
		GridWMS.__init__(self, config, opts, module)

		self._delegateExec = utils.searchPathFind('glite-wms-job-delegate-proxy')
		self._submitExec = utils.searchPathFind('glite-wms-job-submit')
		self._statusExec = utils.searchPathFind('glite-wms-job-status')
		self._outputExec = utils.searchPathFind('glite-wms-job-output')
		self._cancelExec = utils.searchPathFind('glite-wms-job-cancel')

		self._ce = config.get('glite-wms', 'ce', '')
		self._configVO = config.getPath('glite-wms', 'config', '')
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config file '%s' does not exist." % self._configVO)

		self._submitParams.update({
			'-r': self._ce,
			'--config': self._configVO,
		})


	def bulkSubmissionBegin(self, jobs):
		log = tempfile.mktemp('.log')

		try:
			params = ''

			if self._configVO != '':
				params += ' --config %s' % utils.shellEscape(self._configVO)

			activity = utils.ActivityLog('creating delegate proxy for job submission')

			proc = popen2.Popen4("%s%s -a --noint --logfile %s"
			                     % (self._delegateExec, params,
			                        utils.shellEscape(log)), True)

			self._submitParams.update({ '-d': None })
			lines = proc.fromchild.readlines()

			for line in lines:
				line = line.strip()
				try:
					(left, right) = line.split(':', 1)
					if left.endswith('identifier'):
						self._submitParams.update({ '-d': right.strip() })
				except:
					pass
			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: glite-wms-job-delegate-proxy failed (%d)" % retCode
				print >> sys.stderr, lines
			elif id == None:
				print >> sys.stderr, "WARNING: glite-wms-job-delegate-proxy did not yield a proxy id:"

			if id == None and os.path.exists(log):
				sys.stderr.write(open(log, 'r').read())

			# FIXME: glite-wms-job-delegate-proxy

		finally:
			self.cleanup([log])
