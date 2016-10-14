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
	def __init__(self, config, job_limit=-1, job_selector=None):
		JobDB.__init__(self, config, job_limit, job_selector)
		self._path_db = config.get_work_path('jobs')
		self._fmt = utils.DictFormat(escape_strings=True)
		try:
			self._job_map = self._read_jobs(self._job_limit)
		except Exception:
			raise JobError('Unable to read stored job information!')
		if self._job_limit < 0 and len(self._job_map) > 0:
			self._job_limit = max(self._job_map) + 1

	def commit(self, jobnum, job_obj):
		fp = SafeFile(os.path.join(self._path_db, 'job_%d.txt' % jobnum), 'w')
		fp.writelines(self._fmt.format(self._serialize_job_obj(job_obj)))
		fp.close()
		self._job_map[jobnum] = job_obj

	def get_job(self, jobnum):
		return self._job_map.get(jobnum)

	def get_job_persistent(self, jobnum):
		return self._job_map.get(jobnum, Job())

	def get_job_transient(self, jobnum):
		return self._job_map.get(jobnum, self._default_job_obj)

	def _create_job_obj(self, name, data):
		try:
			job = Job()
			job.state = Job.str2enum(data.pop('status'), Job.UNKNOWN)

			if 'id' in data:
				gc_id = data.pop('id')
				if not gc_id.startswith('WMSID'):  # Legacy support
					data['legacy_gc_id'] = gc_id
					if gc_id.startswith('https'):
						gc_id = 'WMSID.GLITEWMS.%s' % gc_id
					else:
						wms_id, wms_name = tuple(gc_id.split('.', 1))
						gc_id = 'WMSID.%s.%s' % (wms_name, wms_id)
				job.gc_id = gc_id

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

	def _read_jobs(self, job_limit):
		utils.ensure_dir_exists(self._path_db, 'job database directory', JobError)

		candidates = []
		for job_fn in fnmatch.filter(os.listdir(self._path_db), 'job_*.txt'):
			try:  # 2xsplit is faster than regex
				jobnum = int(job_fn.split(".")[0].split("_")[1])
			except Exception:
				continue
			candidates.append((jobnum, job_fn))

		(job_map, max_job_len) = ({}, len(candidates))
		activity = Activity('Reading job infos')
		idx = 0
		for (jobnum, job_fn) in sorted(candidates):
			idx += 1
			if (job_limit >= 0) and (jobnum >= job_limit):
				self._log.info('Stopped reading job infos at job #%d out of %d available job files, ' +
					'since the limit of %d jobs is reached', jobnum, len(candidates), job_limit)
				break
			job_obj = self._load_job(os.path.join(self._path_db, job_fn))
			job_map[jobnum] = job_obj
			if idx % 100 == 0:
				activity.update('Reading job infos %d [%d%%]' % (idx, (100.0 * idx) / max_job_len))
		activity.finish()
		return job_map

	def _serialize_job_obj(self, job_obj):
		data = dict(job_obj.dict)
		data['status'] = Job.enum2str(job_obj.state)
		data['attempt'] = job_obj.attempt
		data['submitted'] = job_obj.submitted
		data['changed'] = job_obj.changed
		for key, value in job_obj.history.items():
			data['history_' + str(key)] = value
		if job_obj.gc_id is not None:
			data['id'] = job_obj.dict.get('legacy_gc_id', None) or job_obj.gc_id  # store legacy gc_id
		return data
