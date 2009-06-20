from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO
from grid_control import ConfigError, Job, utils
from wms import WMS

class GridWMS(WMS):
	_statusMap = {
		'ready':     Job.READY,
		'submitted': Job.SUBMITTED,
		'waiting':   Job.WAITING,
		'queued':    Job.QUEUED,
		'scheduled': Job.QUEUED,
		'running':   Job.RUNNING,
		'aborted':   Job.ABORTED,
		'cancelled': Job.CANCELLED,
		'failed':    Job.FAILED,
		'done':      Job.DONE,
		'cleared':   Job.SUCCESS
	}


	def __init__(self, workDir, config, opts, module):
		WMS.__init__(self, workDir, config, opts, module, 'grid')
		self.proxy = config.get('grid', 'proxy', 'VomsProxy')
		self._sites = config.get('grid', 'sites', '').split()
		self.vo = config.get('grid', 'vo', self.getProxy().getVO())


	def _jdlEscape(value):
		repl = { '\\': r'\\', '\"': r'\"', '\n': r'\n' }
		def replace(char):
			try:
				return repl[char]
			except:
				return char
		return '"' + str.join('', map(replace, value)) + '"'
	_jdlEscape = staticmethod(_jdlEscape)


	def storageReq(self, sites):
		def makeMember(member):
			return "Member(%s, other.GlueCESEBindGroupSEUniqueID)" % self._jdlEscape(member)
		if not len(sites):
			return None
		elif len(sites) == 1:
			return makeMember(sites[0])
		else:
			return '(' + str.join(' || ', map(makeMember, sites)) + ')'


	def sitesReq(self, sites):
		def appendSiteItem(list, site):
			if site[0] == ':':
				list.append(site[1:])
			else:
				list.append(site)
		blacklist = []
		whitelist = []
		for site in sites:
			if site[0] == '-':
				appendSiteItem(blacklist, site[1:])
			else:
				appendSiteItem(whitelist, site)

		sitereqs = []
		formatstring = "RegExp(%s, other.GlueCEUniqueID)"
		if len(blacklist):
			sitereqs.extend(map(lambda x: ("!" + formatstring % self._jdlEscape(x)), blacklist))
		if len(whitelist):
			sitereqs.append('(' + str.join(' || ', map(lambda x: (formatstring % self._jdlEscape(x)), whitelist)) + ')')
		if not len(sitereqs):
			return None
		else:
			return '( ' + str.join(' && ', sitereqs) + ' )'


	def formatRequirements(self, reqs):
		result = []
		for type, arg in reqs:
			if type == self.MEMBER:
				result.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._jdlEscape(arg))
			elif type == self.WALLTIME:
				result.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((arg + 59) / 60))
			elif type == self.CPUTIME:
				if arg == 0:
					continue
				result.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((arg + 59) / 60))
				# GlueCEPolicyMaxCPUTime: The maximum CPU time available to jobs submitted to this queue, in minutes.
			elif type == self.MEMORY:
				if arg == 0:
					continue
				result.append('(other.GlueHostMainMemoryRAMSize >= %d)' % arg)
			elif type == self.OTHER:
				result.append('other.GlueHostNetworkAdapterOutboundIP')
			elif type == self.STORAGE:
				result.append(self.storageReq(arg))
			elif type == self.SITES:
				result.append(self.sitesReq(arg))
			else:
				raise RuntimeError('unknown requirement type %d' % type)
		return str.join(' && ', result)


	def getRequirements(self, job):
		reqs = WMS.getRequirements(self, job)
		# WMS.OTHER => GlueHostNetworkAdapterOutboundIP
		reqs.append((WMS.OTHER, ()))
		# add site requirements
		if len(self._sites):
			reqs.append((self.SITES, self._sites))
		return reqs	


	def makeJDL(self, fp, job):
		contents = {
			'Executable': 'grid.sh',
			'Arguments': "%d %s" % (job, self.module.getJobArguments(job)),
			'Environment': utils.DictFormat().format(self.module.getJobConfig(job), format = '%s%s%s'),
			'StdOutput': 'stdout.txt',
			'StdError': 'stderr.txt',
			'InputSandbox': self.sandboxIn,
			'OutputSandbox': self.sandboxOut,
			'_Requirements': self.formatRequirements(self.getRequirements(job)),
			'VirtualOrganisation': self.vo,
			'RetryCount': 2
		}

		# JDL parameter formatter
		def jdlRep((key, delim, value)):
			# _KEY is marker for already formatted text
			if key[0] == '_':
				return (key[1:], delim, value)
			elif type(value) in (int, long):
				return (key, delim, value)
			elif type(value) in (tuple, list):
				recursiveResult = map(lambda x: jdlRep((key, delim, x)), value)
				return (key, delim, '{ ' + str.join(', ', map(lambda (k,d,v): v, recursiveResult)) + ' }')
			else:
				return (key, delim, '"%s"' % value)

		fp.writelines(utils.DictFormat().format(contents, format = '%s %s %s;\n', fkt = jdlRep))
