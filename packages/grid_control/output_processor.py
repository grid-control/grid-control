# | Copyright 2014-2016 Karlsruhe Institute of Technology
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
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, NestedException, Plugin, get_current_exception
from python_compat import bytes2str, ifilter, izip

class OutputProcessor(Plugin):
	def process(self, dn):
		raise AbstractError

class JobResultError(NestedException):
	pass

JobResult = makeEnum(['JOBNUM', 'EXITCODE', 'RAW'])

class JobInfoProcessor(OutputProcessor):
	def __init__(self):
		OutputProcessor.__init__(self)
		self._df = DictFormat()

	def process(self, dn):
		fn = os.path.join(dn, 'job.info')
		try:
			if not os.path.exists(fn):
				raise JobResultError('Job result file %r does not exist' % fn)
			try:
				info_content = open(fn, 'r').read()
			except Exception:
				raise JobResultError('Unable to read job result file %r' % fn)
			if not info_content:
				raise JobResultError('Job result file %r is empty' % fn)
			data = self._df.parse(info_content, keyParser = {None: str}) # impossible to fail
			try:
				jobNum = data.pop('JOBID')
				exitCode = data.pop('EXITCODE')
				return {JobResult.JOBNUM: jobNum, JobResult.EXITCODE: exitCode, JobResult.RAW: data}
			except Exception:
				raise JobResultError('Job result file %r is incomplete' % fn)
		except Exception:
			raise JobResultError('Unable to process output directory %r' % dn)


class DebugJobInfoProcessor(JobInfoProcessor):
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
						else:
							fp = open(full_fn)
							content = fp.read()
						self._log.error(fn + '\n' + content + '-' * 50)
						fp.close()
					except Exception:
						self._log.exception('Unable to display %s', fn)
				else:
					self._log.error('Log file does not exist: %s', fn)
			for fn in self._display_files:
				_display_logfile(dn, fn)
		return result


class FileInfoProcessor(JobInfoProcessor):
	def process(self, dn):
		jobInfo = None
		try:
			jobInfo = JobInfoProcessor.process(self, dn)
		except JobResultError:
			logging.getLogger('jobs.results').warning('Unable to process job information', exc_info = get_current_exception())
		if jobInfo:
			jobData = jobInfo[JobResult.RAW]
			result = {}
			# parse old job info data format for files
			oldFileFormat = [FileInfoProcessor.Hash, FileInfoProcessor.NameLocal, FileInfoProcessor.NameDest, FileInfoProcessor.Path]
			for (fileKey, fileData) in ifilter(lambda key_value: key_value[0].startswith('FILE'), jobData.items()):
				fileIdx = fileKey.replace('FILE', '').rjust(1, '0')
				result[int(fileIdx)] = dict(izip(oldFileFormat, fileData.strip('"').split('  ')))
			# parse new job info data format
			for (fileKey, fileData) in ifilter(lambda key_value: key_value[0].startswith('OUTPUT_FILE'), jobData.items()):
				(fileIdx, fileProperty) = fileKey.replace('OUTPUT_FILE_', '').split('_')
				if isinstance(fileData, str):
					fileData = fileData.strip('"')
				result.setdefault(int(fileIdx), {})[FileInfoProcessor.str2enum(fileProperty)] = fileData
			return list(result.values())
makeEnum(['Hash', 'NameLocal', 'NameDest', 'Path'], FileInfoProcessor)


class TaskOutputProcessor(Plugin):
	def process(self, dn, task):
		raise AbstractError


class SandboxProcessor(TaskOutputProcessor):
	def process(self, dn, task):
		return True
