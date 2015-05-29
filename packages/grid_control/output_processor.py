#-#  Copyright 2014-2015 Karlsruhe Institute of Technology
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

import os
from grid_control import utils
from grid_control.abstract import LoadableObject
from grid_control.backends import WMS
from grid_control.exceptions import AbstractError

class OutputProcessor(LoadableObject):
	def process(self, dn):
		raise AbstractError

class JobInfoProcessor(OutputProcessor):
	def process(self, dn):
		return WMS.parseJobInfo(os.path.join(dn, 'job.info'))

class FileInfoProcessor(JobInfoProcessor):
	def process(self, dn):
		jobInfo = JobInfoProcessor.process(self, dn)
		if jobInfo:
			(jobNumStored, jobExitCode, jobData) = jobInfo
			result = {}
			# parse old job info data format for files
			oldFileFormat = [FileInfoProcessor.Hash, FileInfoProcessor.NameLocal, FileInfoProcessor.NameDest, FileInfoProcessor.Path]
			for (fileKey, fileData) in filter(lambda (key, value): key.startswith('FILE'), jobData.items()):
				fileIdx = fileKey.replace('FILE', '').rjust(1, '0')
				result[int(fileIdx)] = dict(zip(oldFileFormat, fileData.strip('"').split('  ')))
			# parse new job info data format
			for (fileKey, fileData) in filter(lambda (key, value): key.startswith('OUTPUT_FILE'), jobData.items()):
				(fileIdx, fileProperty) = fileKey.replace('OUTPUT_FILE_', '').split('_')
				if isinstance(fileData, str):
					fileData = fileData.strip('"')
				result.setdefault(int(fileIdx), {})[FileInfoProcessor.fromString(fileProperty)] = fileData
			return result.values()
utils.makeEnum(['Hash', 'NameLocal', 'NameDest', 'Path'], FileInfoProcessor)

class TaskOutputProcessor(OutputProcessor):
	def __init__(self, task):
		self._task = task

class SandboxProcessor(TaskOutputProcessor):
	def process(self, dn):
		return True
