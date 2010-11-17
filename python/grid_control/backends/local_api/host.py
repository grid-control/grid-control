from python_compat import *
from grid_control import ConfigError, RethrowError, Job, utils
from api import LocalWMSApi

class Host(LocalWMSApi):
	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)
		self.submitExec = utils.pathGC('share', 'host.sh')
		self.statusExec = utils.resolveInstallPath('ps')
		self.cancelExec = utils.resolveInstallPath('kill')


	def unknownID(self):
		return 'Unknown Job Id'


	def getJobArguments(self, jobNum, sandbox):
		return ''


	def getSubmitArguments(self, jobNum, reqs, sandbox, stdout, stderr, addAttr):
		return '%d "%s" "%s" "%s"' % (jobNum, sandbox, stdout, stderr)


	def parseSubmitOutput(self, data):
		return data.strip()


	def parseStatus(self, status):
		head = map(lambda x: x.strip('%').lower(), next(status, '').split())
		for entry in map(str.strip, status):
			jobinfo = dict(zip(head, filter(lambda x: x != '', entry.split(None, len(head) - 1))))
			jobinfo.update(zip(['id', 'status', 'dest'], [jobinfo.get('pid'), 'R', 'localhost/localqueue']))
			yield jobinfo


	def parseJobState(self, state):
		return Job.RUNNING


	def getCheckArguments(self, wmsIds):
		return 'wwup %s' % str.join(' ', wmsIds)


	def getCancelArguments(self, wmsIds):
		return '-9 %s' % str.join(' ', wmsIds)
