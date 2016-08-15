# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

import os, time, fnmatch
from grid_control import utils
from grid_control.job_db import Job, JobDB, JobError
from grid_control.utils.activity import Activity
from grid_control.utils.file_objects import SafeFile
from python_compat import irange, sorted

class TextFileJobDB(JobDB):
	def __init__(self, config, jobLimit = -1, jobSelector = None):
		JobDB.__init__(self, config, jobLimit, jobSelector)
		self._dbPath = config.getWorkPath('jobs')
		self._fmt = utils.DictFormat(escapeString = True)
		try:
			self._jobMap = self._readJobs(self._jobLimit)
		except Exception:
			raise JobError('Unable to read stored job information!')
		if self._jobLimit < 0 and len(self._jobMap) > 0:
			self._jobLimit = max(self._jobMap) + 1


	def _serialize_job_obj(self, job_obj):
		data = dict(job_obj.dict)
		data['status'] = Job.enum2str(job_obj.state)
		data['attempt'] = job_obj.attempt
		data['submitted'] = job_obj.submitted
		data['changed'] = job_obj.changed
		for key, value in job_obj.history.items():
			data['history_' + str(key)] = value
		if job_obj.gcID is not None:
			data['id'] = job_obj.dict.get('legacy_gcID', None) or job_obj.gcID # store legacy gcID
		return data


	def _create_job_obj(self, name, data):
		try:
			job = Job()
			job.state = Job.str2enum(data.pop('status'), Job.UNKNOWN)

			if 'id' in data:
				gcID = data.pop('id')
				if not gcID.startswith('WMSID'): # Legacy support
					data['legacy_gcID'] = gcID
					if gcID.startswith('https'):
						gcID = 'WMSID.GLITEWMS.%s' % gcID
					else:
						wmsID, wmsName = tuple(gcID.split('.', 1))
						gcID = 'WMSID.%s.%s' % (wmsName, wmsID)
				job.gcID = gcID

			for key in ['attempt', 'submitted', 'changed']:
				if key in data:
					setattr(job, key, data[key])
			if 'runtime' not in data:
				if 'submitted' in data and (job.submitted > 0):
					data['runtime'] = time.time() - float(job.submitted)
				else:
					data['runtime'] = 0
			for key in irange(1, job.attempt + 1):
				if ('history_' + str(key)).strip() in data:
					job.history[key] = data['history_' + str(key)]
			job.dict = data
		except Exception:
			raise JobError('Unable to parse data in %s:\n%r' % (name, data))
		return job


	def _load_job(self, name):
		try:
			data = self._fmt.parse(open(name))
		except Exception:
			raise JobError('Invalid format in %s' % name)
		return self._create_job_obj(name, data)


	def _readJobs(self, jobLimit):
		utils.ensureDirExists(self._dbPath, 'job database directory', JobError)

		candidates = []
		for jobFile in fnmatch.filter(os.listdir(self._dbPath), 'job_*.txt'):
			try: # 2xsplit is faster than regex
				jobNum = int(jobFile.split(".")[0].split("_")[1])
			except Exception:
				continue
			candidates.append((jobNum, jobFile))

		(jobMap, maxJobs) = ({}, len(candidates))
		activity = Activity('Reading job infos')
		idx = 0
		for (jobNum, jobFile) in sorted(candidates):
			idx += 1
			if (jobLimit >= 0) and (jobNum >= jobLimit):
				self._log.info('Stopped reading job infos at job #%d out of %d available job files, since the limit of %d jobs is reached',
					jobNum, len(candidates), jobLimit)
				break
			jobObj = self._load_job(os.path.join(self._dbPath, jobFile))
			jobMap[jobNum] = jobObj
			if idx % 100 == 0:
				activity.update('Reading job infos %d [%d%%]' % (idx, (100.0 * idx) / maxJobs))
		activity.finish()
		return jobMap


	def getJob(self, jobNum):
		return self._jobMap.get(jobNum)


	def getJobTransient(self, jobNum):
		return self._jobMap.get(jobNum, self._defaultJob)


	def getJobPersistent(self, jobNum):
		return self._jobMap.get(jobNum, Job())


	def commit(self, jobNum, jobObj):
		fp = SafeFile(os.path.join(self._dbPath, 'job_%d.txt' % jobNum), 'w')
		fp.writelines(self._fmt.format(self._serialize_job_obj(jobObj)))
		fp.close()
		self._jobMap[jobNum] = jobObj
