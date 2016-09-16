#!/usr/bin/env python
# | Copyright 2010-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, sys
from gcSupport import FileInfoProcessor, JobInfoProcessor, JobResult, initGC
from python_compat import sorted


def main():
	jip = JobInfoProcessor()
	fip = FileInfoProcessor()
	(config, jobDB) = initGC(sys.argv[1:])
	workDir = config.getWorkPath()
	for jobNum in sorted(jobDB.getJobs()):
		if jip.process(os.path.join(workDir, 'output', 'job_%d' % jobNum))[JobResult.EXITCODE] == 0:
			for fileInfo in fip.process(os.path.join(workDir, 'output', 'job_%d' % jobNum)):
				pathSE = fileInfo[FileInfoProcessor.Path].replace('file://', '').replace('dir://', '')
				print('%s  %s/%s' % (fileInfo[FileInfoProcessor.Hash], pathSE, fileInfo[FileInfoProcessor.NameDest]))

if __name__ == '__main__':
	sys.exit(main())
