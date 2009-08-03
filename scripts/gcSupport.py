#!/usr/bin/env python
import sys, os, re

# add python subdirectory from where exec was started to search path
root = os.path.dirname(os.path.abspath(os.path.normpath(os.path.join(sys.argv[0], '..'))))
sys.path.insert(0, os.path.join(root, 'python'))

from grid_control import *
utils.verbosity.setting = 0
utils.atRoot.root = root

class DummyStream:
	def __init__(self, stream):
		self.__stream = stream
		self.log = []
	def write(self, data):
		self.log.append(data)
		return True
	def __getattr__(self, name):
		return self.__stream.__getattribute__(name)


class ConfigDummy(object):
	def get(self, x,y,z):
		return z
	def getPath(self, x,y,z):
		return z


def getWorkSEJobs(args):
	if len(args) == 2:
		(configFile, jobid) = args
		config = Config(configFile)
		workDir = config.getPath('global', 'workdir', config.workDirDefault)
		pathSE = config.get('storage', 'se path', '')
		jobList = [ jobid ]
	if len(args) == 1:
		configFile = args[0]
		idregex = re.compile(r'^job_([0-9]+)$')
		config = Config(configFile)
		workDir = config.getPath('global', 'workdir', config.workDirDefault)
		pathSE = config.get('storage', 'se path', '')
		jobList = map(lambda x: int(idregex.match(x).group(1)), os.listdir(os.path.join(workDir, 'output')))
	else:
		sys.stderr.write("Syntax: %s <config file> [<job id>]\n\n" % sys.argv[0])
		sys.exit(1)
	return (workDir, pathSE, jobList)


def getJobInfo(workDir, jobNum, retCodeFilter = lambda x: True):
	try:
		jobInfoPath = os.path.join(workDir, 'output', 'job_%d' % jobNum, 'jobinfo.txt')
		jobInfo = utils.DictFormat('=').parse(open(jobInfoPath))
		if retCodeFilter(jobInfo.get('exitcode', -1)):
			return jobInfo
	except:
		print "Unable to read job results from %s!" % jobInfoPath
	return None
