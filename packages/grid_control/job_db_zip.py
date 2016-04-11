# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, zipfile
from grid_control import utils
from grid_control.job_db import Job, JobDB
from python_compat import imap

class ZippedJobDB(JobDB):
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		self._dbFile = config.getWorkPath('jobs.zip')
		JobDB.__init__(self, config, jobLimit, jobSelector)

	def readJobs(self, jobLimit):
		jobMap = {}
		maxJobs = 0
		if os.path.exists(self._dbFile):
			try:
				tar = zipfile.ZipFile(self._dbFile, 'r', zipfile.ZIP_DEFLATED)
			except Exception: # Try to recover job archive
				utils.eprint('=' * 40 + '\nStarting recovery of broken job database')
				utils.eprint(' => Answer "y" if asked "Is this a single-disk archive?"!\n' + '=' * 40)
				os.system('zip -FF %s --out %s.tmp 2> /dev/null' % (self._dbFile, self._dbFile))
				os.rename(self._dbFile, self._dbFile + '.broken')
				os.rename(self._dbFile + '.tmp', self._dbFile)
				tar = zipfile.ZipFile(self._dbFile, 'r', zipfile.ZIP_DEFLATED)
				utils.removeFiles([self._dbFile + '.broken'])
				brokenList = []
				for idx, fnTarInfo in enumerate(tar.namelist()):
					(jobNum, tid) = tuple(imap(lambda s: int(s[1:]), fnTarInfo.split('_', 1)))
					try:
						fp = tar.open(fnTarInfo)
						try:
							fp.read()
						finally:
							fp.close()
					except Exception:
						pass
				for broken in brokenList:
					os.system('zip %s -d %s' % (self._dbFile, broken))
				utils.eprint('Recover completed!')
			activity = utils.ActivityLog('Reading job transactions ...')
			maxJobs = len(tar.namelist())
			tMap = {}
			for idx, fnTarInfo in enumerate(tar.namelist()):
				(jobNum, tid) = tuple(imap(lambda s: int(s[1:]), fnTarInfo.split('_', 1)))
				if tid < tMap.get(jobNum, 0):
					continue
				data = utils.DictFormat(escapeString = True).parse(tar.open(fnTarInfo).read())
				jobMap[jobNum] = Job.loadData(fnTarInfo, data)
				tMap[jobNum] = tid
				if idx % 100 == 0:
					activity.finish()
					activity = utils.ActivityLog('Reading job transactions ... %d [%d%%]' % (idx, (100.0 * idx) / maxJobs))

		self._serial = maxJobs
		return jobMap


	def commit(self, jobNum, jobObj):
		jobData = str.join('', utils.DictFormat(escapeString = True).format(jobObj.getAll()))
		tar = zipfile.ZipFile(self._dbFile, 'a', zipfile.ZIP_DEFLATED)
		try:
			tar.writestr('J%06d_T%06d' % (jobNum, self._serial), jobData)
		finally:
			tar.close()
		self._serial += 1


class Migrate2ZippedJobDB(ZippedJobDB):
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		dbPath = config.getWorkPath('jobs')
		self._dbFile = config.getWorkPath('jobs.zip')
		if os.path.exists(dbPath) and os.path.isdir(dbPath) and not os.path.exists(self._dbFile):
			activity = utils.ActivityLog('Converting job database...')
			self._serial = 0
			try:
				oldDB = JobDB(config)
				oldDB.readJobs(-1)
				for jobNum in oldDB.getJobs():
					self.commit(jobNum, oldDB.get(jobNum))
			except Exception:
				utils.removeFiles([self._dbFile])
				raise
			activity.finish()

		ZippedJobDB.__init__(self, config, jobLimit, jobSelector)
