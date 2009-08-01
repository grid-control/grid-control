#!/usr/bin/env python
import sys, os, fcntl, gzip, xml.dom.minidom, gcSupport
from grid_control import *
_verbosity = 0

def main(args):

	if len(args) == 3:
		(jobid, wmsid, retcode) = args
		if retcode != '0':
			sys.exit(0)
		workDir = os.environ['GC_WORKDIR']
		pathSE = os.environ['GC_SE_PATH']
		jobList = [ jobid ]
	else:
		(workDir, pathSE, jobList) = gcSupport.getWorkSEJobs(args)

	lockfile = os.path.join(workDir, 'prod.lock')
	fd = open(lockfile, 'w')
	fcntl.flock(fd, fcntl.LOCK_EX)

	try:
		taskInfo = utils.DictFormat(" = ").parse(open(os.path.join(workDir, 'task.dat')))
		provider = DataProvider.loadState(gcSupport.ConfigDummy(), workDir, 'production.dbs')

		try:
			saved = (sys.stdout, sys.stderr)
			sys.stdout = gcSupport.DummyStream(sys.stdout)
			sys.stderr = gcSupport.DummyStream(sys.stderr)
			blocks = provider.getBlocks()
			sys.stdout, sys.stderr = saved
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

		provider.saveState(workDir, "production.dbs", blocks)

	finally:
		fcntl.flock(fd, fcntl.LOCK_UN)
		if os.path.exists(lockfile):
			os.unlink(lockfile)

	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
