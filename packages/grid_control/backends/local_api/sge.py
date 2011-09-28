import sys, os, xml.dom.minidom
from grid_control import ConfigError, RethrowError, Job, utils
from grid_control.backends.wms import WMS
from pbsge import PBSGECommon

class OGE(PBSGECommon):
	def __init__(self, config):
		PBSGECommon.__init__(self, config)
		self.configExec = utils.resolveInstallPath('qconf')


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr, addAttr):
		timeStr = lambda s: '%02d:%02d:%02d' % (s / 3600, (s / 60) % 60, s % 60)
		reqMap = { WMS.MEMORY: ('h_vmem', lambda m: '%dM' % m),
			WMS.WALLTIME: ('s_rt', timeStr), WMS.CPUTIME: ('h_cpu', timeStr) }
		# Restart jobs = no
		return ' -r n' + PBSGECommon.getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr, addAttr, reqMap)


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
				jobinfo['id'] = jobinfo['JB_job_number']
				jobinfo['status'] = jobinfo['state']
				jobinfo['dest'] = 'N/A'
				if 'queue_name' in jobinfo:
					queue, node = jobinfo['queue_name'].split('@')
					jobinfo['dest'] = '%s/%s' % (node, queue)
			except:
				raise RethrowError('Error reading job info:\n%s' % jobentry.toxml())
			yield jobinfo


	def parseJobState(self, state):
		if True in map(lambda x: x in state, ['h', 's', 'S', 'T', 'w']):
			return Job.QUEUED
		if True in map(lambda x: x in state, ['r', 't']):
			return Job.RUNNING
		return Job.READY


	def getCheckArguments(self, wmsIds):
		return '-xml'


	def getCancelArguments(self, wmsIds):
		return str.join(',', wmsIds)


	def getQueues(self):
		queues = {}
		tags = ['h_vmem', 'h_cpu', 's_rt']
		reqs = dict(zip(tags, [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]))
		parser = dict(zip(tags, [int, utils.parseTime, utils.parseTime]))

		for queue in map(str.strip, utils.LoggedProcess(self.configExec, '-sql').iter()):
			queues[queue] = dict()
			for line in utils.LoggedProcess(self.configExec, '-sq %s' % queue).iter():
				attr, value = map(str.strip, line.split(' ', 1))
				if (attr in tags) and (value != 'INFINITY'):
					queues[queue][reqs[attr]] = parser[attr](value)
		return queues


	def getNodes(self):
		(result, active) = ([], False)
		for line in utils.LoggedProcess(self.configExec, '-sep').iter():
			if line.startswith('===='):
				active = not active
			elif active:
				result.append(line.split()[0])
		if len(result) > 0:
			return result


class SGE(OGE):
	pass
