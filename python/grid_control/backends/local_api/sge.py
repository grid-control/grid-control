import sys, os, xml.dom.minidom
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends.wms import WMS
from api import LocalWMSApi

class SGE(LocalWMSApi):
	def __init__(self, config, wms):
		LocalWMSApi.__init__(self, config, wms)

		self.submitExec = utils.searchPathFind('qsub')
		self.statusExec = utils.searchPathFind('qstat')
		self.cancelExec = utils.searchPathFind('qdel')

	def unknownID(self):
		return "Unknown Job Id"

	def getArguments(self, jobNum, sandbox):
		return ""


	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr, addAttr):
		# Restart jobs = no, job name
		params = ' -r n -N %s' % self.wms.getJobName(jobNum)

		# Requirement based settings
		strTime = lambda s: "%02d:%02d:%02d" % (s / 3600, (s / 60) % 60, s % 60)
		reqs = dict(self.wms.getRequirements(jobNum))
		if WMS.SITES in reqs:
			(queue, nodes) = reqs[WMS.SITES]
			params += ' -q %s' % queue
			if nodes:
				params += ' -l site=%s' % str.join(",", nodes)
		if self.checkReq(reqs, WMS.WALLTIME):
			params += " -l s_rt=%s" % strTime(reqs[WMS.WALLTIME])
		if self.checkReq(reqs, WMS.CPUTIME):
			params += " -l h_cpu=%s" % strTime(reqs[WMS.CPUTIME])
		if self.checkReq(reqs, WMS.MEMORY):
			params += ' -l h_vmem=%dM' % reqs[WMS.MEMORY]

		# Sandbox, IO paths
		params += ' -v GC_SANDBOX=%s -o %s -e %s' % (sandbox, stdout, stderr)
		return params + str.join(' ', map(lambda kv: ' -l %s=%s' % kv, addAttr.items()))


	def parseSubmitOutput(self, data):
		# Your job 424992 ("test.sh") has been submitted
		return data.split()[2].strip()


	def parseStatus(self, status):
		dom = xml.dom.minidom.parseString(str.join('', status))
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
				if 'queue_name' in jobinfo:
					tmp = jobinfo['queue_name'].split("@")
					jobinfo['dest'] = "%s/%s" % (tmp[1], tmp[0])
			except:
				raise RethrowError("Error reading job info:\n%s\n" % jobentry.toxml())
			yield jobinfo


	def parseJobState(self, state):
		if True in map(lambda x: x in state, ['h', 's', 'S', 'T', 'w']):
			return Job.QUEUED
		if True in map(lambda x: x in state, ['r', 't']):
			return Job.RUNNING
		return Job.READY


	def getCheckArgument(self, wmsIds):
		return "-xml"


	def getCancelArgument(self, wmsIds):
		return str.join(",", wmsIds)
