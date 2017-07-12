# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import os, gzip, logging
from grid_control.utils import DictFormat
from grid_control.utils.data_structures import make_enum
from grid_control.utils.file_tools import SafeFile
from hpfwk import AbstractError, NestedException, Plugin, clear_current_exception, get_current_exception  # pylint:disable=line-too-long
from python_compat import bytes2str, ifilter, izip


JobResult = make_enum(['JOBNUM', 'MESSAGE', 'EXITCODE', 'RAW'])  # pylint:disable=invalid-name
FileInfo = make_enum(['Hash', 'NameLocal', 'NameDest', 'Path', 'Size'])  # pylint:disable=invalid-name


class JobResultError(NestedException):
	pass


class OutputProcessor(Plugin):
	def process(self, dn):
		raise AbstractError


class TaskOutputProcessor(Plugin):
	def process(self, dn, task):
		raise AbstractError


class JobInfoProcessor(OutputProcessor):
	alias_list = ['jobinfo']

	def __init__(self):
		OutputProcessor.__init__(self)
		self._df = DictFormat()

	def process(self, dn):
		fn = os.path.join(dn, 'job.info')
		try:
			if not os.path.exists(fn):
				raise JobResultError('Job result file %r does not exist' % fn)
			try:
				info_content = SafeFile(fn).read_close()
			except Exception:
				raise JobResultError('Unable to read job result file %r' % fn)
			if not info_content:
				raise JobResultError('Job result file %r is empty' % fn)
			data = self._df.parse(info_content, key_parser={None: str})  # impossible to fail
			try:
				jobnum = data.pop('JOBID')
				exit_code = data.pop('EXITCODE')
				message = data.pop('MESSAGE', None)
				return {JobResult.JOBNUM: jobnum, JobResult.EXITCODE: exit_code,
					JobResult.MESSAGE: message, JobResult.RAW: data}
			except Exception:
				raise JobResultError('Job result file %r is incomplete' % fn)
		except Exception:
			raise JobResultError('Unable to process output directory %r' % dn)


class SandboxProcessor(TaskOutputProcessor):
	alias_list = ['null']

	def process(self, dn, task):
		return True


class DebugJobInfoProcessor(JobInfoProcessor):
	alias_list = ['debug']

	def __init__(self):
		JobInfoProcessor.__init__(self)
		self._log = logging.getLogger('jobs.output')
		self._display_files = ['gc.stdout', 'gc.stderr']

	def process(self, dn):
		result = JobInfoProcessor.process(self, dn)
		if result[JobResult.EXITCODE] != 0:
			def _display_logfile(dn, fn):
				full_fn = os.path.join(dn, fn)
				if os.path.exists(full_fn):
					try:
						if fn.endswith('.gz'):
							fp = gzip.open(full_fn)
							content = bytes2str(fp.read())
							fp.close()
						else:
							content = SafeFile(full_fn).read_close()
						self._log.error(fn + '\n' + content + '-' * 50)
					except Exception:
						self._log.exception('Unable to display %s', fn)
						clear_current_exception()
				else:
					self._log.error('Log file does not exist: %s', fn)
			for fn in self._display_files:
				_display_logfile(dn, fn)
		return result


class FileInfoProcessor(JobInfoProcessor):
	alias_list = ['fileinfo']

	def process(self, dn):
		job_info_dict = None
		try:
			job_info_dict = JobInfoProcessor.process(self, dn)
		except JobResultError:
			logger = logging.getLogger('jobs.results')
			logger.warning('Unable to process job information', exc_info=get_current_exception())
			clear_current_exception()
		if job_info_dict:
			job_data_dict = job_info_dict[JobResult.RAW]
			result = {}

			def _get_items_with_key(key_prefix):
				return ifilter(lambda key_value: key_value[0].startswith(key_prefix), job_data_dict.items())

			# parse old job info data format for files
			old_fmt_header = [FileInfo.Hash, FileInfo.NameLocal, FileInfo.NameDest, FileInfo.Path]
			for (file_key, file_data) in _get_items_with_key('FILE'):
				file_idx = file_key.replace('FILE', '') or '0'
				result[int(file_idx)] = dict(izip(old_fmt_header, file_data.strip('"').split('  ')))
			# parse new job info data format
			for (file_key, file_data) in _get_items_with_key('OUTPUT_FILE'):
				(file_idx, file_prop) = file_key.replace('OUTPUT_FILE_', '').split('_')
				if isinstance(file_data, str):
					file_data = file_data.strip('"')
				file_prop = file_prop.lower().replace('dest', 'namedest').replace('local', 'namelocal')
				result.setdefault(int(file_idx), {})[FileInfo.str2enum(file_prop)] = file_data
			return list(result.values())
