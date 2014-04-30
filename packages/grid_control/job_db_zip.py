#-#  Copyright 2013 Karlsruhe Institute of Technology
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

import os, utils, zipfile
from job_db import JobDB

class ZippedJobDB(JobDB):
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		JobDB.__init__(self, config, jobLimit, jobSelector)

	def readJobs(self, jobLimit):
		jobMap = {}
		maxJobs = 0
		if os.path.exists('%s.zip' % self.dbPath):
			tar = zipfile.ZipFile('%s.zip' % self.dbPath, 'r', zipfile.ZIP_DEFLATED)
			log = None
			maxJobs = len(tar.namelist())
			tMap = {}
			for idx, tarInfo in enumerate(tar.namelist()):
				(jobNum, tid) = tuple(map(lambda s: int(s[1:]), tarInfo.split('_', 1)))
				if tid < tMap.get(jobNum, 0):
					continue
				data = utils.DictFormat(escapeString = True).parse(tar.open(tarInfo).read())
				jobMap[jobNum] = Job.loadData(tarInfo, data)
				tMap[jobNum] = tid
				if idx % 100 == 0:
					del log
					log = utils.ActivityLog('Reading job transactions ... %d [%d%%]' % (idx, (100.0 * idx) / maxJobs))
		self.serial = maxJobs
		return jobMap


	def commit(self, jobNum, jobObj):
		tar = zipfile.ZipFile('%s.zip' % self.dbPath, 'a', zipfile.ZIP_DEFLATED)
		jobData = str.join('', utils.DictFormat(escapeString = True).format(jobObj.getAll()))
		tar.writestr('J%06d_T%06d' % (jobNum, self.serial), jobData)
		tar.close()
		self.serial += 1
