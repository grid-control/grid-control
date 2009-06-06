import sys, os, popen2, tempfile, shutil
import xml.dom.minidom
from grid_control import ConfigError, Job, utils
from local_wms import LocalWMS

class SGE(LocalWMS):
	_statusMap = {
		'qw': Job.QUEUED,
		'Eqw': Job.WAITING,
		'h': Job.WAITING,   'w': Job.WAITING,
		's': Job.QUEUED,    'r': Job.RUNNING,
		'S': Job.QUEUED,    'R': Job.RUNNING,
		'T': Job.QUEUED,    't': Job.RUNNING,
		'd': Job.ABORTED,   'E': Job.DONE,
	}

	def __init__(self, config, module, init):
		LocalWMS.__init__(self, config, module, init)

		self.submitExec = utils.searchPathFind('qsub')
		self.statusExec = utils.searchPathFind('qstat')
		self.cancelExec = utils.searchPathFind('qdel')

		self._queue = config.get('local', 'queue', '')

	def unknownID(self):
		return "Unknown Job Id"

	def getArguments(self, jobNum, sandbox):
		return ""


	def getSubmitArguments(self, jobNum, sandbox):
		# Job name
		params = ' -N %s' % self.getJobName(self.module.taskID, jobNum)
		# Job queue
		if len(self._queue):
			params += ' -q %s' % self._queue
		# Sandbox
		params += ' -v SANDBOX=%s' % sandbox
		# IO paths
		params += ' -o %s -e %s' % (
			utils.shellEscape(os.path.join(sandbox, 'stdout.txt')),
			utils.shellEscape(os.path.join(sandbox, 'stderr.txt')))
		return params


	def parseSubmitOutput(self, data):
		# Your job 424992 ("test.sh") has been submitted
		return "%s.sge" % data.split()[2]


	def parseStatus(self, status):
		result = []
		dom = xml.dom.minidom.parseString(status)
		for jobentry in dom.getElementsByTagName('job_list'):
			jobinfo = {}
			try:
				for node in jobentry.childNodes:
					if node.nodeType != xml.dom.minidom.Node.ELEMENT_NODE:
						continue
					if node.hasChildNodes():
						jobinfo[str(node.nodeName)] = str(node.childNodes[0].nodeValue)
				jobinfo['id'] = "%s.sge" % jobinfo['JB_job_number']
				jobinfo['status'] = jobinfo['state']
				jobinfo['dest'] = 'N/A'
				if jobinfo.has_key('queue_name'):
					tmp = jobinfo['queue_name'].split("@")
					jobinfo['dest'] = "%s/%s" % (tmp[1], tmp[0])
			except:
				print "Error reading job info\n", jobentry.toxml()
				raise
			result.append(jobinfo)
		return result


	def getCheckArgument(self, wmsIds):
		return " -xml"


	def getCancelArgument(self, wmsIds):
		return str.join(",", wmsIds)
