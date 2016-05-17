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

import os, sys, gzip, logging
from grid_control.utils import DictFormat
from grid_control.utils.data_structures import makeEnum
from hpfwk import AbstractError, NestedException, Plugin
from python_compat import ifilter, izip

class OutputProcessor(Plugin):
	def process(self, dn):
		raise AbstractError

class JobResultError(NestedException):
	pass

JobResult = makeEnum(['JOBNUM', 'EXITCODE', 'RAW'], useHash = True)

class JobInfoProcessor(OutputProcessor):
	def __init__(self):
		OutputProcessor.__init__(self)
		self._df = DictFormat()

	def process(self, dn):
		fn = os.path.join(dn, 'job.info')
		if not os.path.exists(fn):
			raise JobResultError('Job result file %r does not exist' % fn)
		try:
			info_content = open(fn, 'r').read()
		except Exception:
			raise JobResultError('Unable to read job result file %r' % fn)
		if not info_content:
			raise JobResultError('Job result file %r is empty' % fn)
		try:
			data = self._df.parse(info_content, keyParser = {None: str})
		except Exception:
			raise JobResultError('Unable to parse job result file %r' % fn)
		try:
			jobNum = data.pop('JOBID')
			exitCode = data.pop('EXITCODE')
			return {JobResult.JOBNUM: jobNum, JobResult.EXITCODE: exitCode, JobResult.RAW: data}
		except Exception:
			raise JobResultError('Job result file %r is incomplete' % fn)


class DebugJobInfoProcessor(JobInfoProcessor):
	def __init__(self):
		JobInfoProcessor.__init__(self)
		self._display_files = ['gc.stdout', 'gc.stderr']

	def process(self, dn):
		result = JobInfoProcessor.process(self, dn)
		if result[JobResult.EXITCODE] != 0:
			def _display_logfile(dn, fn):
				try:
					full_fn = os.path.join(dn, fn)
					if os.path.exists(full_fn):
						if fn.endswith('.gz'):
							fp = gzip.open(full_fn)
						else:
							fp = open(full_fn)
						sys.stdout.write('%s\n' % fn)
						sys.stdout.write(fp.read())
						sys.stdout.write('-' * 50)
						fp.close()
				except Exception:
					sys.stdout.write('Unable to display %s\n' % fn)
			for fn in self._display_files:
				_display_logfile(dn, fn)
		return result


class FileInfoProcessor(JobInfoProcessor):
	def process(self, dn):
		try:
			jobInfo = JobInfoProcessor.process(self, dn)
		except Exception:
			logging.getLogger('wms').warning(sys.exc_info()[1])
			jobInfo = None
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


class TaskOutputProcessor(OutputProcessor):
	def __init__(self, task):
		self._task = task


class SandboxProcessor(TaskOutputProcessor):
	def process(self, dn):
		return True
