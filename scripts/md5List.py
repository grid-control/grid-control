#!/usr/bin/env python
import sys
from gcSupport import *

(workDir, jobList) = getWorkJobs(sys.argv[1:])
for jobNum in sorted(jobList):
	files = getFileInfo(workDir, jobNum, lambda retCode: retCode == 0)
	if files:
		for (hash, name_local, name_dest, pathSE) in files:
			pathSE = pathSE.replace("file://", "").replace("dir://", "")
			print("%s  %s/%s" % (hash, pathSE, name_dest))
