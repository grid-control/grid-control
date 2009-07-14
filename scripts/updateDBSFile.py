#!/usr/bin/env python
import sys, os, fcntl, gzip, xml.dom.minidom

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.insert(0, os.path.join(_root, "..", 'python'))
from grid_control import *

_verbosity = 0

def main(args):
	(jobid, wmsid, retcode) = args
	workDir = os.environ['GC_WORKDIR']

	class ConfigDummy(object):
		def get(self, x,y,z):
			return z
		def getPath(self, x,y,z):
			return z

	lockfile = os.path.join(workDir, 'prod.lock')
	fd = open(lockfile, 'w')
	fcntl.flock(fd, fcntl.LOCK_EX)

	provider = DataProvider.loadState(ConfigDummy(), workDir, 'prod.dbs')
	try:
		blocks = provider.getBlocks()
	except:
		blocks = []

	outputDir = os.path.join(workDir, 'output', 'job_' + str(jobid))
	jobinfo = open(os.path.join(outputDir, 'jobinfo.txt'),'r')
	files = filter(lambda x: x[0].startswith('file'), utils.DictFormat('=').parse(jobinfo.readlines()).items())
	files = map(lambda (x,y): tuple(y.strip('"').split('  ')), files)

	for (hash, name_local, name_dest) in files:
		dataset = "/%s/%s" % (os.environ['GC_GC_CONF'], name_local.replace('.root', ''))
		blockname = "%s-%05d" % (os.environ['GC_TASK_ID'][2:], int(os.environ.get('GC_PARAM_ID', 0)))
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

		lfn = os.path.join(os.environ['GC_SE_PATH'], name_dest)
		nevents = 0

		renameExt = lambda name: str.join('.', name.split('.')[:-1]) + '.xml.gz'
		fwkreports = map(renameExt, os.environ['GC_CMSSW_CONFIG'].split())
		for fwkreport in map(lambda x: gzip.open(os.path.join(outputDir, x), 'r'), fwkreports):
			for outfile in xml.dom.minidom.parse(fwkreport).getElementsByTagName("File"):
				pfn = outfile.getElementsByTagName("PFN")[0].childNodes[0].data
				if pfn == name_local:
					nevents = int(outfile.getElementsByTagName("TotalEvents")[0].childNodes[0].data)

		filelist = cblock[DataProvider.FileList]
		filelist.append({ DataProvider.lfn: lfn, DataProvider.NEvents: nevents })
		cblock[DataProvider.NEvents] = reduce(lambda x,y: x+y, map(lambda x: x[DataProvider.NEvents], filelist))

	provider.saveState(workDir, "prod.dbs", blocks)

	fcntl.flock(fd, fcntl.LOCK_UN)
	if os.path.exists(lockfile):
		os.unlink(lockfile)
	return 0

# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
