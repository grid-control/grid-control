# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

from grid_control.datasets.provider_base import DataProvider, DatasetError
from grid_control.datasets.splitter_base import DataSplitter
from python_compat import next

class EventBoundarySplitter(DataSplitter):
	alias = ['events']

	def neededEnums(cls):
		return [DataSplitter.FileList, DataSplitter.Skipped, DataSplitter.NEntries]
	neededEnums = classmethod(neededEnums)


	def _splitJobs(self, fileList, eventsPerJob, firstEvent):
		nextEvent = firstEvent
		succEvent = nextEvent + eventsPerJob
		curEvent = 0
		lastEvent = 0
		curSkip = 0
		fileListIter = iter(fileList)
		job = { DataSplitter.Skipped: 0, DataSplitter.NEntries: 0, DataSplitter.FileList: [] }
		while True:
			if curEvent >= lastEvent:
				try:
					fileObj = next(fileListIter)
				except StopIteration:
					if job[DataSplitter.FileList]:
						yield job
					break

				nEvents = fileObj[DataProvider.NEntries]
				if nEvents < 0:
					raise DatasetError('EventBoundarySplitter does not support files with a negative number of events!')
				curEvent = lastEvent
				lastEvent = curEvent + nEvents
				curSkip = 0

			if nextEvent >= lastEvent:
				curEvent = lastEvent
				continue

			curSkip += nextEvent - curEvent
			curEvent = nextEvent

			available = lastEvent - curEvent
			if succEvent - nextEvent < available:
				available = succEvent - nextEvent

			if not len(job[DataSplitter.FileList]):
				job[DataSplitter.Skipped] = curSkip

			job[DataSplitter.NEntries] += available
			nextEvent += available

			job[DataSplitter.FileList].append(fileObj[DataProvider.URL])
			if DataProvider.Metadata in fileObj:
				job.setdefault(DataSplitter.Metadata, []).append(fileObj[DataProvider.Metadata])

			if nextEvent >= succEvent:
				succEvent += eventsPerJob
				yield job
				job = { DataSplitter.Skipped: 0, DataSplitter.NEntries: 0, DataSplitter.FileList: [] }


	def _initConfig(self, config):
		self._events_per_job = self._configQuery(config.getInt, 'events per job')


	def splitDatasetInternal(self, blocks, firstEvent = 0):
		for block in blocks:
			eventsPerJob = self._setup(self._events_per_job, block)
			for job in self._splitJobs(block[DataProvider.FileList], eventsPerJob, firstEvent):
				firstEvent = 0
				yield self.finaliseJobSplitting(block, job)
