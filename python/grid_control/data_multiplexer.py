from __future__ import generators
import sys, os, copy
from grid_control import AbstractObject, RuntimeError, utils, ConfigError, DataDiscovery

class DataMultiplexer(DataDiscovery):
	def __init__(self, datasetExpr, dbsapi):
		self.instances = []
		id = 0
		for datasetentry in datasetExpr.split('\n'):
			temp = datasetentry.split(':')
			if len(temp) > 1:
				nickname = temp[0].strip()
				dataset = temp[1].strip()
			else:
				nickname = None
				dataset = temp[0].strip()
			instance = DataDiscovery.open(dbsapi, dataset)
			self.instances.append((id, nickname, instance))
			id += 1


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
