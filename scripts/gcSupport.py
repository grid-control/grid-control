#!/usr/bin/env python
import sys, os, re, fcntl, time, optparse

# add python subdirectory from where exec was started to search path
sys.path.insert(1, os.path.join(sys.path[0], '..', 'packages'))
from gcPackage import *

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


def initGC(args):
	if len(args) > 0:
		configFile = args[0]
		config = Config(configFile)
		userSelector = None
		if len(args) != 1:
			userSelector = MultiJobSelector(args[1])
		return (config.getWorkPath(), config, JobDB(config, jobSelector = userSelector))
	sys.stderr.write("Syntax: %s <config file> [<job id>, ...]\n\n" % sys.argv[0])
	sys.exit(1)


def getWorkJobs(args, selector = None):
	(workDir, config, jobDB) = initGC(args)
	return (workDir, len(jobDB), jobDB.getJobs(selector))


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
	return map(lambda (x, y): tuple(y.strip('"').split('  ')), files)


def getCMSSWInfo(tarPath):
	import tarfile, xml.dom.minidom
	# Read framework report files to get number of events
	tarFile = tarfile.open(tarPath, "r:gz")
	fwkReports = filter(lambda x: os.path.basename(x.name) == 'report.xml', tarFile.getmembers())
	for fwkReport in map(lambda fn: tarFile.extractfile(fn), fwkReports):
		try:
			yield xml.dom.minidom.parse(fwkReport)
		except:
			print "Error while parsing %s" % tarPath
			raise


def prettySize(size):
	suffixes = [("B", 2**10), ("K", 2**20), ("M", 2**30), ("G", 2**40), ("T", 2**50)]
	for suf, lim in suffixes:
		if size > lim:
			continue
		else:
			return str(round(size / float(lim / 2**10), 2)) + suf


def parseOptions(parser):
	parser.add_option('',   '--parseable', dest='displaymode', const='parseable', action='store_const',
		help='Output tabular data in parseable format')
	parser.add_option('-P', '--pivot',     dest='displaymode', const='longlist',  action='store_const',
		help='Output pivoted tabular data')
	parser.add_option('',   '--textwidth', dest='textwidth',   default=100,
		help='Output tabular data with selected width')
	parser.add_option("-v", "--verbose",   dest="verbosity",   default=0,         action="count",
		help='Increase verbosity')
	(opts, args) = parser.parse_args()
	utils.verbosity(opts.verbosity)
	utils.printTabular.mode = opts.displaymode
	utils.printTabular.wraplen = int(opts.textwidth)
	return (opts, args)
