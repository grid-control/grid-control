#!/usr/bin/env python
import sys, os, fcntl, gzip, xml.dom.minidom

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, "..", 'python'))
from grid_control import *

_verbosity = 0

def main(args):
	if len(args) == 3:
		(jobid, wmsid, retcode) = args
		workDir = os.environ['GC_WORKDIR']
		pathSE = os.environ['GC_SE_PATH']
		jobList = [ jobid ]
	elif len(args) == 2:
		(configFile, jobid) = args
		config = Config(configFile)
		confName = str.join("", os.path.basename(configFile).split(".")[:-1])
		workDir = config.getPath('global', 'workdir', 'work.%s' % confName)
		pathSE = config.get('storage', 'se path', '')
		jobList = [ jobid ]
	elif len(args) == 1:
		configFile = args[0]
		idregex = re.compile(r'^job_([0-9]+)$')
		config = Config(configFile)
		confName = str.join("", os.path.basename(configFile).split(".")[:-1])
		workDir = config.getPath('global', 'workdir', 'work.%s' % confName)
		pathSE = config.get('storage', 'se path', '')
		jobList = map(lambda x: int(idregex.match(x).group(1)), os.listdir(os.path.join(workDir, 'output')))
	else:
		sys.stderr.write("Syntax: %s <config file> [<job id>]\n\n" % sys.argv[0])
		sys.exit(1)

	class ConfigDummy(object):
		def get(self, x,y,z):
			return z
		def getPath(self, x,y,z):
			return z

	lockfile = os.path.join(workDir, 'prod.lock')
	fd = open(lockfile, 'w')
	fcntl.flock(fd, fcntl.LOCK_EX)

	try:
		taskInfo = utils.DictFormat(" = ").parse(open(os.path.join(workDir, 'task.dat')))
		provider = DataProvider.loadState(ConfigDummy(), workDir, 'production.dbs')
		try:
			blocks = provider.getBlocks()
		except:
			blocks = []

		for jobid in jobList:
			outputDir = os.path.join(workDir, 'output', 'job_' + str(jobid))
			jobInfo = utils.DictFormat('=').parse(open(os.path.join(outputDir, 'jobinfo.txt')))

			files = filter(lambda x: x[0].startswith('file'), jobInfo.items())
			files = map(lambda (x,y): tuple(y.strip('"').split('  ')), files)

			for (hash, name_local, name_dest) in files:
				dataset = "/PRIVATE/%s" % (name_local.replace('.root', ''))
				blockname = "%s-%05d" % (taskInfo['task id'][2:], int(os.environ.get('GC_PARAM_ID', 0)))

				cblock = None
				for block in blocks:
					if (block[DataProvider.Dataset] == dataset) and (block[DataProvider.BlockName] == blockname):
						cblock = block
				if cblock == None:
					cblock = {
						DataProvider.Dataset: dataset,
						DataProvider.BlockName: blockname,
						DataProvider.NEvents: 0,
						DataProvider.SEList: ['localhost'],
						DataProvider.FileList: []
					}
					blocks.append(cblock)

				lfn = os.path.join(pathSE, name_dest)
				nevents = 0

				fwkreports = filter(lambda fn: fn.endswith('.xml.gz'), os.listdir(outputDir))
				for fwkreport in map(lambda fn: gzip.open(os.path.join(outputDir, fn)), fwkreports):
					for outfile in xml.dom.minidom.parse(fwkreport).getElementsByTagName("File"):
						pfn = outfile.getElementsByTagName("PFN")[0].childNodes[0].data
						if pfn == name_local:
							nevents = int(outfile.getElementsByTagName("TotalEvents")[0].childNodes[0].data)

				filelist = cblock[DataProvider.FileList]
				filelist.append({ DataProvider.lfn: lfn, DataProvider.NEvents: nevents })
				cblock[DataProvider.NEvents] = reduce(lambda x,y: x+y, map(lambda x: x[DataProvider.NEvents], filelist))

		provider.saveState(workDir, "prod.dbs", blocks)

	finally:
		fcntl.flock(fd, fcntl.LOCK_UN)
		if os.path.exists(lockfile):
			os.unlink(lockfile)

	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
