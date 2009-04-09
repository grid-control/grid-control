from __future__ import generators
import sys, os, copy
from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DataProvider

class DataMultiplexer(DataProvider):
	def __init__(self, datasetExpr, dbsapi):
		self.instances = []
		id = 0
		print('Using the following datasets:')

		print('%4s | %15s | %s' % ('ID'.center(4), 'Nickname'.center(15), 'Dataset path'))
		print('%4s=+=%15s=+=%s' % ('=' * 4, '=' * 15, '=' * 50))
		for datasetentry in datasetExpr.split('\n'):
			temp = datasetentry.split(':')
			if len(temp) == 3:
				nickname = temp[0].strip()
				provider = temp[1].strip()
				dataset = temp[2].strip()
			elif len(temp) == 2:
				nickname = temp[0].strip()
				provider = dbsapi
				dataset = temp[1].strip()
			elif len(temp) == 1:
				nickname = None
				provider = dbsapi
				dataset = temp[0].strip()
			if provider.lower() == 'dbs':
				provider = dbsapi
			providernice = provider
			if provider == dbsapi:
				providernice = 'dbs'
			print('%4i | %s | %s://%s' % (id, nickname.center(15), providernice, dataset))
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
