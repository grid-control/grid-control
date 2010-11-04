import sys, os, xml.dom.minidom
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends.wms import WMS
from pbsge import PBSGECommon

class SGE(PBSGECommon):
	def getSubmitArguments(self, jobNum, sandbox, stdout, stderr, addAttr):
		timeStr = lambda s: "%02d:%02d:%02d" % (s / 3600, (s / 60) % 60, s % 60)
		reqMap = { WMS.MEMORY: ("h_vmem", lambda m: "%dM" % m),
			WMS.WALLTIME: ("s_rt", timeStr), WMS.CPUTIME: ("h_cpu", timeStr) }
		# Restart jobs = no
		return ' -r n' + PBSGECommon.getSubmitArguments(self, jobNum, sandbox, stdout, stderr, addAttr, reqMap)


	def parseSubmitOutput(self, data):
		# Your job 424992 ("test.sh") has been submitted
		print data
		print data.split()[2].strip()
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
				jobinfo['id'] = jobinfo['JB_job_number']
				jobinfo['status'] = jobinfo['state']
				jobinfo['dest'] = 'N/A'
				if 'queue_name' in jobinfo:
					tmp = jobinfo['queue_name'].split("@")
					jobinfo['dest'] = "%s/%s" % (tmp[1], tmp[0])
			except:
				raise RethrowError("Error reading job info:\n%s" % jobentry.toxml())
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
