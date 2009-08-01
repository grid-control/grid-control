#!/usr/bin/env python
import sys, os, gcSupport
_verbosity = 0

def main(args):
	(workDir, pathSE, jobList) = gcSupport.getWorkSEJobs(args)

	for jobid in jobList:
		outputDir = os.path.join(workDir, 'output', 'job_' + str(jobid))
		jobInfo = utils.DictFormat('=').parse(open(os.path.join(outputDir, 'jobinfo.txt')))

		files = filter(lambda x: x[0].startswith('file'), jobInfo.items())
		files = map(lambda (x,y): tuple(y.strip('"').split('  ')), files)

		for (hash, name_local, name_dest) in files:
			print (hash, name_local, name_dest)

	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
