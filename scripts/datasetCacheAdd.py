#!/usr/bin/env python
import gcSupport, sys, os, fcntl, gzip, xml.dom.minidom
from grid_control import *

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
	jobList = map(int, jobList)

	# Lock file in case several instances of this program are running
	lockfile = os.path.join(workDir, 'prod.lock')
	fd = open(lockfile, 'w')
	fcntl.flock(fd, fcntl.LOCK_EX)

	def unlock():
		fcntl.flock(fd, fcntl.LOCK_UN)
		try:
			if os.path.exists(lockfile):
				os.unlink(lockfile)
		except:
			pass

	try:
		taskInfo = utils.PersistentDict(os.path.join(workDir, 'task.dat'), ' = ')
		provider = DataProvider.loadState(gcSupport.ConfigDummy(), workDir, 'production.dbs')

		# Try to read all existing Blocks from production.dbs
		saved = (sys.stdout, sys.stderr)
		try:
			sys.stdout = gcSupport.DummyStream(sys.stdout)
			sys.stderr = gcSupport.DummyStream(sys.stderr)
			blocks = provider.getBlocks()
		except:
			blocks = []
		sys.stdout, sys.stderr = saved

		for jobid in jobList:
			outputDir = os.path.join(workDir, 'output', 'job_' + str(jobid))

			# Read specified job info files
			jobInfo = gcSupport.getJobInfo(workDir, jobid, lambda retCode: retCode == 0)
			if not jobInfo:
				continue

			files = filter(lambda x: x[0].startswith('file'), jobInfo.items())
			files = map(lambda (x,y): tuple(y.strip('"').split('  ')), files)

			for (hash, name_local, name_dest) in files:
				dataset = "/PRIVATE/%s" % (name_local.replace('.root', ''))
				blockname = "%s-%05d" % (taskInfo['task id'][2:], int(os.environ.get('GC_PARAM_ID', 0)))

				# Try to find block with given dataset + blockname
				cblock = None
				for block in blocks:
					if (block[DataProvider.Dataset] == dataset) and (block[DataProvider.BlockName] == blockname):
						cblock = block
				# No block found => Create new block
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

				# Read framework report files to get number of events
				fwkreports = filter(lambda fn: fn.endswith('.xml.gz'), os.listdir(outputDir))
				try:
					for fwkreport in map(lambda fn: gzip.open(os.path.join(outputDir, fn)), fwkreports):
						for outfile in xml.dom.minidom.parse(fwkreport).getElementsByTagName("File"):
							pfn = outfile.getElementsByTagName("PFN")[0].childNodes[0].data
							if pfn == name_local:
								nevents = int(outfile.getElementsByTagName("TotalEvents")[0].childNodes[0].data)
				except:
					print "Error while parsing framework output!"
					return 0

				if nevents == 0:
					continue

				# Add file to filelist of the current block
				filelist = cblock[DataProvider.FileList]
				if not lfn in map(lambda x: x[DataProvider.lfn], filelist):
					filelist.append({ DataProvider.lfn: lfn, DataProvider.NEvents: nevents })
				cblock[DataProvider.NEvents] = reduce(lambda x,y: x+y, map(lambda x: x[DataProvider.NEvents], filelist))

		provider.saveState(workDir, "production.dbs", blocks)
	except:
		unlock()
		raise
	unlock()
	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
