# | Copyright 2007-2017 Karlsruhe Institute of Technology
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
from grid_control.job_db import Job, JobDB, JobError
from grid_control.utils import DictFormat, ensure_dir_exists
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import SafeFile, with_file
from hpfwk import clear_current_exception
from python_compat import irange, sorted


class TextFileJobDB(JobDB):
	alias_list = ['textdb']

	def __init__(self, config, job_limit=-1, job_selector=None):
		JobDB.__init__(self, config, job_limit, job_selector)
		self._path_db = config.get_work_path('jobs')
		self._fmt = DictFormat(escape_strings=True)
		try:
			self._job_map = self._read_jobs(self._job_limit)
		except Exception:
			raise JobError('Unable to read stored job information!')
		if self._job_limit < 0 and len(self._job_map) > 0:
			self._job_limit = max(self._job_map) + 1

	def commit(self, jobnum, job_obj):
		with_file(SafeFile(os.path.join(self._path_db, 'job_%d.txt' % jobnum), 'w'),
			lambda fp: fp.writelines(self._fmt.format(self._serialize_job_obj(job_obj))))
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
			job.set_dict(data)
		except Exception:
			raise JobError('Unable to parse data in %s:\n%r' % (name, data))
		return job

	def _read_jobs(self, job_limit):
		ensure_dir_exists(self._path_db, 'job database directory', JobError)

		candidates = []
		for job_fn in fnmatch.filter(os.listdir(self._path_db), 'job_*.txt'):
			try:  # 2xsplit is faster than regex
				jobnum = int(job_fn.split(".")[0].split("_")[1])
			except Exception:
				clear_current_exception()
				continue
			candidates.append((jobnum, job_fn))

		(job_map, max_job_len) = ({}, len(candidates))
		activity = Activity('Reading job infos')
		idx = 0
		for (jobnum, job_fn) in sorted(candidates):
			idx += 1
			if jobnum >= job_limit >= 0:
				self._log.info('Stopped reading job infos at job #%d out of %d available job files, ' +
					'since the limit of %d jobs is reached', jobnum, len(candidates), job_limit)
				break
			try:
				job_fn_full = os.path.join(self._path_db, job_fn)
				data = self._fmt.parse(SafeFile(job_fn_full).iter_close())
				job_obj = self._create_job_obj(job_fn_full, data)
			except Exception:
				raise JobError('Unable to process job file %r' % job_fn_full)
			job_map[jobnum] = job_obj
			activity.update('Reading job infos %d [%d%%]' % (idx, (100.0 * idx) / max_job_len))
		activity.finish()
		return job_map

	def _serialize_job_obj(self, job_obj):
		data = job_obj.get_dict_full()
		for key, value in job_obj.history.items():
			data['history_' + str(key)] = value
		if job_obj.gc_id is not None:
			data['id'] = job_obj.get('legacy_gc_id') or job_obj.gc_id  # store legacy gc_id
		return data
