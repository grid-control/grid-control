import sys, os
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends.wms import WMS
from api import LocalWMSApi

class PBSGECommon(LocalWMSApi):
	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)
		self.submitExec = utils.resolveInstallPath('qsub')
		self.statusExec = utils.resolveInstallPath('qstat')
		self.cancelExec = utils.resolveInstallPath('qdel')
		self._group = config.get('local', 'group', '', volatile=True)
		self.delayOutput = config.getBool('local', 'delay output', False, volatile=True)


	def unknownID(self):
		return 'Unknown Job Id'


	def getJobArguments(self, jobNum, sandbox):
		return ''


	def getSubmitArguments(self, jobNum, reqs, sandbox, stdout, stderr, addAttr, reqMap):
		# Job name
		params = ' -N "%s"' % self.wms.getJobName(jobNum)
		# Job group
		if len(self._group):
			params += ' -W group_list=%s' % self._group
		# Job requirements
		if WMS.SITES in reqs:
			(queue, nodes) = reqs[WMS.SITES]
			if queue:
				params += ' -q %s' % queue
		# Process job requirements
		for req in reqMap:
			if self.checkReq(reqs, req):
				params += ' -l %s=%s' % (reqMap[req][0], reqMap[req][1](reqs[req]))
		# Sandbox, IO paths
		params += ' -v GC_SANDBOX="%s"' % sandbox
		if self.delayOutput:
			params += ' -v GC_DELAY_OUTPUT="%s" -v GC_DELAY_ERROR="%s" -j y -o /dev/null' % (stdout, stderr)
		else:
			params += ' -o "%s" -e "%s"' % (stdout, stderr)
		return params + str.join('', map(lambda kv: ' -l %s=%s' % kv, addAttr.items()))
