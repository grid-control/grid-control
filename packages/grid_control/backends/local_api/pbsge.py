import sys, os
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends import WMS, LocalWMS

class PBSGECommon(LocalWMS):
	def __init__(self, config, wmsName = None):
		LocalWMS.__init__(self, config, wmsName,
			submitExec = utils.resolveInstallPath('qsub'),
			statusExec = utils.resolveInstallPath('qstat'),
			cancelExec = utils.resolveInstallPath('qdel'))
		section = self._getSections('backend')
		self.group = config.get(section, 'group', '', mutable=True)
		self.shell = config.get(section, 'shell', '', mutable=True)
		self.delay = config.getBool(section, 'delay output', False, mutable=True)
		self.addAttr = config.getDict(section, 'submit options', {}, mutable=True) # TODO


	def unknownID(self):
		return 'Unknown Job Id'


	def getJobArguments(self, jobNum, sandbox):
		return ''


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr, reqMap):
		# Job name
		params = ' -N "%s"' % jobName
		# Job shell
		if self.shell:
			params += ' -S %s' % self.shell
		# Job group
		if self.group:
			params += ' -W group_list=%s' % self.group
		# Process job requirements
		for req in reqMap:
			if self.checkReq(reqs, req):
				params += ' -l %s=%s' % (reqMap[req][0], reqMap[req][1](reqs[req]))
		# Sandbox, IO paths
		params += ' -v GC_SANDBOX="%s"' % sandbox
		if self.delay:
			params += ' -v GC_DELAY_OUTPUT="%s" -v GC_DELAY_ERROR="%s" -o /dev/null -e /dev/null' % (stdout, stderr)
		else:
			params += ' -o "%s" -e "%s"' % (stdout, stderr)
		return params + str.join('', map(lambda kv: ' -l %s=%s' % kv, self.addAttr[0].items()))
