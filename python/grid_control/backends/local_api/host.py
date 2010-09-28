from grid_control import ConfigError, RethrowError, Job, utils
from api import LocalWMSApi

class Host(LocalWMSApi):
	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)

		self.submitExec = utils.pathGC('share', 'host.sh')
		self.statusExec = utils.searchPathFind('ps')
		self.cancelExec = utils.searchPathFind('kill')


	def unknownID(self):
		return "Unknown Job Id"


	def getArguments(self, jobNum, sandbox):
		return ""


	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr, addAttr):
		return "%d %s %s %s" % (jobNum, sandbox, stdout, stderr)


	def parseSubmitOutput(self, data):
		return data.strip()


	def parseStatus(self, status):
		head = map(lambda x: x.strip("%").lower(), status.next().split())
		for entry in map(str.strip, status):
			try:
				jobinfo = dict(zip(head, filter(lambda x: x != '', entry.split(None, len(head) - 1))))
				jobinfo.update(zip(['id', 'status', 'dest'], [jobinfo['pid'], 'R', 'localhost/localqueue']))
			except:
				raise RethrowError("Error reading job info:\n%s" % entry)
			yield jobinfo


	def parseJobState(self, state):
		return Job.RUNNING


	def getCheckArgument(self, wmsIds):
		return "wwup %s" % str.join(" ", wmsIds)


	def getCancelArgument(self, wmsIds):
		return "-9 %s" % str.join(" ", wmsIds)
