from __future__ import generators
import sys, os, copy
from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, datasetExpr, dbsapi):
		self.instances = []
		id = 0
		print('Using the following datasets:')

		print(' %6s | %15s | %s' % ('ID'.center(6), 'Nickname'.center(15), 'Dataset path'))
		print('=%6s=+=%15s=+=%s' % ('=' * 6, '=' * 15, '=' * 59))
		for datasetentry in datasetExpr.split('\n'):
			temp = map(lambda x: x.strip(), datasetentry.split(':'))
			nickname = None
			provider = dbsapi
			if len(temp) == 3:
				(nickname, provider, dataset) = temp
			elif len(temp) == 2:
				(nickname, dataset) = temp
			elif len(temp) == 1:
				(dataset) = temp
			if dataset[0] == '/':
				dataset = '/' + dataset.lstrip('/')
			if provider.lower() == 'dbs':
				provider = dbsapi
			providernice = provider
			if provider == dbsapi:
				providernice = 'dbs'
			print(' %6i | %s | %s://%s' % (id, nickname.center(15), providernice, dataset))

			instance = DataProvider.open(provider, dataset)
			self.instances.append((id, nickname, instance))
			id += 1
		print


	def run(self, eventsPerJob):
		self.jobFiles = []
		for (id, nickname, instance) in self.instances:
			instance.run(eventsPerJob)
			for job in instance.jobFiles:
				job['DatasetID'] = id
				job['DatasetNick'] = nickname
			self.jobFiles.extend(copy.deepcopy(instance.jobFiles))


	def printDataset(self):
		for (id, nickname, instance) in self.instances:
			instance.printDataset()
			print
