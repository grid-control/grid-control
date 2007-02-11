from __future__ import generators
import sys, os
from grid_control import RuntimeError, utils

__all__ = ['DBSApi']

# import stuff from DLSAPI, DBSAPI temporarily, which are incorrectly packaged
for dir in ('DLSAPI', 'DBSAPI'):
	sys.path.append(os.path.join(utils.getRoot(), 'python', dir))

import dlsApi
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
import dlsClient
import dbsCgiApi


class DBSApi:
	def __init__(self, datasetPath):
		self.dlsIface = dlsClient.DLS_TYPE_LFC
		self.dlsEndpoint = 'prod-lfc-cms-central.cern.ch/grid/cms/DLS/LFC'
		self.dbsUrl = 'http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery'
		self.dbsInstance = 'MCGlobal/Writer'
		self.datasetPath = datasetPath

		self.dbs = dbsCgiApi.DbsCgiApi(self.dbsUrl, { 'instance': self.dbsInstance })
		self.dls = dlsClient.getDlsApi(self.dlsIface, self.dlsEndpoint)


	def _splitJobs(collectionList, eventsPerJob, firstEvent):
		nextEvent = firstEvent
		succEvent = nextEvent + eventsPerJob
		curEvent = 0
		lastEvent = 0
		curSkip = 0
		collectionIter = iter(collectionList)
		job = { 'skip': 0, 'events': 0, 'files': [] }
		while True:
			if curEvent >= lastEvent:
				try:
					collection = collectionIter.next()
				except StopIteration:
					if len(job['files']):
						yield job
					break

				nEvents = collection['numberOfEvents']
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

			if not len(job['files']):
				job['skip'] = curSkip

			job['events'] += int(available)
			nextEvent += available

			job['files'].append(collection['fileList'][0]['logicalFileName'])

			if nextEvent >= succEvent:
				succEvent += eventsPerJob
				yield job
				job = { 'skip': 0, 'events': 0, 'files': [] }

		else:
			if len(job['files']):
				yield job
	_splitJobs = staticmethod(_splitJobs)


	def run(self):
		datasets = self.dbs.listProcessedDatasets(self.datasetPath)
		if len(datasets) == 0:
			raise RuntimeError('No dataset found')
		elif len(datasets) > 1:
			raise RuntimeError('Ambiguous dataset name')

		datasetPath = datasets[0].get('datasetPathName')

		contents = self.dbs.getDatasetContents(datasetPath)

		self.sites = []
		self.collections = []

		for fileBlock in contents:
			blockName = fileBlock.get('blockName')
			for entry in self.dls.getLocations([ blockName ]):
				self.sites.extend(map(lambda x: str(x.host), entry.locations))

			self.collections.extend(fileBlock.get('eventCollectionList'))


	def query(self, nJobs, eventsPerJob, firstEvent):
		jobNr = 0
		for job in self._splitJobs(self.collections, eventsPerJob, firstEvent):
			if jobNr >= nJobs:
				break
			yield (job['skip'], job['events'], job['files'])
			jobNr += 1
