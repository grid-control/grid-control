# | Copyright 2013-2017 Karlsruhe Institute of Technology
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
from grid_control.job_db_text import TextFileJobDB
from grid_control.utils import remove_files, resolve_install_path
from grid_control.utils.activity import Activity
from grid_control.utils.process_base import LocalProcess
from hpfwk import clear_current_exception
from python_compat import imap


class ZippedJobDB(TextFileJobDB):
	alias_list = ['zipdb']

	def __init__(self, config, job_limit=-1, job_selector=None):
		self._db_fn = config.get_work_path('jobs.zip')
		TextFileJobDB.__init__(self, config, job_limit, job_selector)

	def commit(self, jobnum, job_obj):
		job_str = str.join('', self._fmt.format(self._serialize_job_obj(job_obj)))
		tar = zipfile.ZipFile(self._db_fn, 'a', zipfile.ZIP_DEFLATED)
		try:
			tar.writestr('J%06d_T%06d' % (jobnum, self._serial), job_str)
		finally:
			tar.close()
		self._serial += 1
		self._job_map[jobnum] = job_obj

	def _read_jobs(self, job_limit):
		job_map = {}
		max_job_len = 0
		if os.path.exists(self._db_fn):
			try:
				tar = zipfile.ZipFile(self._db_fn, 'r', zipfile.ZIP_DEFLATED)
				tar.testzip()
			except Exception:  # Try to recover job archive
				clear_current_exception()
				self._log.warning('Job database is corrupted - starting recovery')
				self._recover_jobs()
				self._log.info('Recover completed!')
			activity = Activity('Reading job transactions')
			max_job_len = len(tar.namelist())
			map_jobnum2tarfn = {}
			for idx, tar_info_fn in enumerate(tar.namelist()):
				(jobnum, tid) = tuple(imap(lambda s: int(s[1:]), tar_info_fn.split('_', 1)))
				if tid < map_jobnum2tarfn.get(jobnum, 0):
					continue
				try:
					data = self._fmt.parse(tar.open(tar_info_fn).read())
				except Exception:
					clear_current_exception()
					continue
				job_map[jobnum] = self._create_job_obj(tar_info_fn, data)
				map_jobnum2tarfn[jobnum] = tid
				if idx % 100 == 0:
					activity.update('Reading job transactions %d [%d%%]' % (idx, (100.0 * idx) / max_job_len))
			activity.finish()
		self._serial = max_job_len
		return job_map

	def _recover_jobs(self):
		proc = LocalProcess('zip', '-FF', self._db_fn, '--out', '%s.tmp' % self._db_fn)
		proc.stdin.write('y\n')
		proc.status(timeout=None)
		os.rename(self._db_fn, self._db_fn + '.broken')
		os.rename(self._db_fn + '.tmp', self._db_fn)
		tar = zipfile.ZipFile(self._db_fn, 'r', zipfile.ZIP_DEFLATED)
		remove_files([self._db_fn + '.broken'])
		broken_fn_list = []
		for tar_info_fn in tar.namelist():
			try:
				tuple(imap(lambda s: int(s[1:]), tar_info_fn.split('_', 1)))  # check name
				fp = tar.open(tar_info_fn)
				try:
					fp.read()
				finally:
					fp.close()
			except Exception:
				clear_current_exception()
				broken_fn_list.append(tar_info_fn)
		for broken in broken_fn_list:
			os.system('zip %s -d %s' % (self._db_fn, broken))


class Migrate2ZippedJobDB(ZippedJobDB):
	alias_list = ['migrate']

	def __new__(cls, config, job_limit=-1, job_selector=None):
		try:
			resolve_install_path('zip')
		except Exception:
			clear_current_exception()
			return TextFileJobDB.__new__(cls, config, job_limit, job_selector)
		path_db = config.get_work_path('jobs')
		db_fn = config.get_work_path('jobs.zip')
		if os.path.exists(path_db) and os.path.isdir(path_db) and not os.path.exists(db_fn):
			activity = Activity('Converting job database')
			new_db = ZippedJobDB(config)
			try:
				old_db = TextFileJobDB(config)
				for jobnum in old_db.get_job_list():
					new_db.commit(jobnum, old_db.get_job(jobnum))
			except Exception:
				remove_files([db_fn])
				raise
			activity.finish()
		return ZippedJobDB.__new__(cls, config, job_limit, job_selector)
