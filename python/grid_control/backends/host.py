import sys, os, random
from grid_control import ConfigError, Job, utils
from local_wms import LocalWMSApi

class Host(LocalWMSApi):
	_statusMap = {
		'R': Job.RUNNING,   'Q': Job.QUEUED,
	}

	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)

		self.submitExec = utils.atRoot('share', 'host.sh')
		self.statusExec = utils.searchPathFind('ps')
		self.cancelExec = utils.searchPathFind('kill')

	def unknownID(self):
		return "Unknown Job Id"

	def getArguments(self, jobNum, sandbox):
		return ""

	def getSubmitArguments(self, jobNum, sandbox):
		return "%d %s %s %s" % (jobNum, sandbox,
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))

	def parseSubmitOutput(self, data):
		return "%s.localhost" % data.strip()

	def parseStatus(self, status):
		result = []
		lines = status.splitlines()
		head = map(lambda x: x.strip("%").lower(), lines[0].split())
		for entry in lines[1:]:
			try:
				jobinfo = dict(zip(head, filter(lambda x: x != '', entry.split(None, len(head) - 1))))
				jobinfo['id'] = "%s.localhost" % jobinfo['pid']
				jobinfo['status'] = 'R'
				jobinfo['dest'] = 'localhost/localqueue'
			except:
				print "Error reading job info\n", entry
				raise
			result.append(jobinfo)
		return result

	def getCheckArgument(self, wmsIds):
		return "wwup %s" % str.join(" ", wmsIds)

	def getCancelArgument(self, wmsIds):
		return "-9 %s" % str.join(" ", wmsIds)
