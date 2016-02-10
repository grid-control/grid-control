#!/usr/bin/env python
#-#  Copyright 2010-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, sys
from gcSupport import FileInfoProcessor, JobInfoProcessor, getWorkJobs

def main():
	jip = JobInfoProcessor()
	fip = FileInfoProcessor()
	(workDir, nJobs, jobList) = getWorkJobs(sys.argv[1:])
	for jobNum in sorted(jobList):
		if jip.process(os.path.join(workDir, 'output', 'job_%d' % jobNum))[1] == 0:
			for fileInfo in fip.process(os.path.join(workDir, 'output', 'job_%d' % jobNum)):
				pathSE = fileInfo[FileInfoProcessor.Path].replace('file://', '').replace('dir://', '')
				print('%s  %s/%s' % (fileInfo[FileInfoProcessor.Hash], pathSE, fileInfo[FileInfoProcessor.NameDest]))

if __name__ == '__main__':
	sys.exit(main())
