#!/usr/bin/env python
import gcSupport, sys, os, optparse, popen2
from grid_control import *

def main(args):
	# we need exactly one positional argument (config file)
	if len(args) != 1:
		utils.exitWithUsage("%s <config file>" % sys.argv[0])

	(workDir, jobList) = gcSupport.getWorkJobs(args)
	for jobNum in utils.sorted(jobList):
		files = gcSupport.getFileInfo(workDir, jobNum, lambda retCode: retCode == 0)
		for (hash, name_local, name_dest, pathSE) in files:
			pathSE = pathSE.replace("file://", "")
			pathSE = pathSE.replace("dir://", "")
			print "%s  %s/%s" % (hash, pathSE, name_dest)

	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
