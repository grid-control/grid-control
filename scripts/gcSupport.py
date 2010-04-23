#!/usr/bin/env python
import sys, os, re, fcntl, time

# add python subdirectory from where exec was started to search path
sys.path.insert(1, os.path.join(sys.path[0], '..', 'python'))

from grid_control import *
utils.verbosity.setting = 0

class DummyStream(object):
	def __init__(self, stream):
		self.__stream = stream
		self.log = []
	def write(self, data):
		self.log.append(data)
		return True
	def __getattr__(self, name):
		return self.__stream.__getattribute__(name)


class Silencer(object):
	def __init__(self):
		self.saved = (sys.stdout, sys.stderr)
		sys.stdout = DummyStream(sys.stdout)
		sys.stderr = DummyStream(sys.stderr)
	def __del__(self):
		del sys.stdout
		del sys.stderr
		(sys.stdout, sys.stderr) = self.saved


class ConfigDummy(object):
	def __init__(self, cfg = {}):
		self.cfg = cfg
	def get(self, x,y,z=None,volatile=None):
		return self.cfg.get(x, {}).get(y, z)
	def getPath(self, x,y,z=None,volatile=None):
		return self.cfg.get(x, {}).get(y, z)
	def getBool(self, x,y,z=None,volatile=None):
		return self.cfg.get(x, {}).get(y, z)


class FileMutex:
	def __init__(self, lockfile):
		first = time.time()
		self.lockfile = lockfile
		while os.path.exists(self.lockfile):
			if first and (time.time() - first > 10):
				print 'Trying to aquire lock file %s...' % lockfile
				first = False
			time.sleep(0.2)
		self.fd = open(self.lockfile, 'w')
		fcntl.flock(self.fd, fcntl.LOCK_EX)

	def __del__(self):
		fcntl.flock(self.fd, fcntl.LOCK_UN)
		try:
			if os.path.exists(self.lockfile):
				os.unlink(self.lockfile)
		except:
			pass


def getJobs(workDir):
	idregex = re.compile(r'^job_([0-9]+)$')
	jobFiles = filter(lambda x: x.startswith('job'), os.listdir(os.path.join(workDir, 'output')))
	return map(lambda x: int(idregex.match(x).group(1)), jobFiles)


def getWorkJobs(args):
	if len(args) == 2:
		(configFile, jobid) = args
		config = Config(configFile)
		workDir = config.getPath('global', 'workdir', config.workDirDefault)
		jobList = [ jobid ]
	elif len(args) == 1:
		configFile = args[0]
		config = Config(configFile)
		workDir = config.getPath('global', 'workdir', config.workDirDefault)
		jobList = getJobs(workDir)
	else:
		sys.stderr.write("Syntax: %s <config file> [<job id>, ...]\n\n" % sys.argv[0])
		sys.exit(1)
	return (workDir, map(int, jobList))


def getJobInfo(workDir, jobNum, retCodeFilter = lambda x: True):
	jobInfoPath = os.path.join(workDir, 'output', 'job_%d' % jobNum, 'job.info')
	try:
		jobInfo = utils.DictFormat('=').parse(open(jobInfoPath))
		if retCodeFilter(jobInfo.get('exitcode', -1)):
			return jobInfo
	except:
		print "Unable to read job results from %s!" % jobInfoPath
	return None


def getFileInfo(workDir, jobNum, retCodeFilter = lambda x: True, rejected = None):
	jobInfo = getJobInfo(workDir, jobNum, retCodeFilter)
	if not jobInfo:
		return rejected

	files = filter(lambda x: x[0].startswith('file'), jobInfo.items())
	return map(lambda (x,y): tuple(y.strip('"').split('  ')), files)


def prettySize(size):
	suffixes = [("B", 2**10), ("K", 2**20), ("M", 2**30), ("G", 2**40), ("T", 2**50)]
	for suf, lim in suffixes:
		if size > lim:
			continue
		else:
			return str(round(size / float(lim / 2**10), 2)) + suf
