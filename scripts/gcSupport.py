#!/usr/bin/env python
import sys, os, re

# add python subdirectory from where exec was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, "..", 'python'))

from grid_control import *

class DummyStream:
	def __init__(self, stream):
		self.__stream = stream
	def write(self, data):
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
