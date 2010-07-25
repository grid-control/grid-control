import os, cmssw
from python_compat import *
from grid_control import datasets, utils
from grid_control.datasets import DataSplitter

class CMSSW_Advanced(cmssw.CMSSW):
	def __init__(self, config):
		def parseMap(x):
			result = {}
			for entry in x.split('\n'):
				if ":" in entry:
					nick, value = map(str.strip, entry.split(':'))
				else:
					nick, value = (None, entry)
				result[nick] = filter(lambda x: x, map(str.strip, value.split(',')))
			return result

		# Mapping between nickname and config files:
		self.nickMapCfg = parseMap(config.get(self.__class__.__name__, 'nickname config', ''))
		if self.nickMapCfg:
			allConfigFiles = utils.flatten(self.nickMapCfg.values())
			config.set(self.__class__.__name__, 'config files', str.join('\n', allConfigFiles))

		# Mapping between nickname and constants:
		self.nickMapCName = map(str.strip, config.get(self.__class__.__name__, 'nickname constants', '').split())
		self.nickMapConstants = {}
		for var in self.nickMapCName:
			tmp = parseMap(config.get(self.__class__.__name__, var, ''))
			for (nick, value) in tmp.items():
				if value:
					self.nickMapConstants.setdefault(nick, {})[var] = value[0]

		print "Mapping between nickname and other settings:"
		def report():
			for nick in sorted(set(self.nickMapCfg.keys() + self.nickMapConstants.keys())):
				tmp = {0: nick, 1: str.join(', ', map(os.path.basename, self.nickMapCfg.get(nick, '')))}
				tmp.update(self.nickMapConstants[nick])
				yield tmp
		head = [(0, "Nickname"), (1, "Config file")] + zip(self.nickMapCName, self.nickMapCName)
		cmssw.utils.printTabular(head, report(), "cl")
		cmssw.CMSSW.__init__(self, config)


	def getTaskConfig(self):
		# Remove config file variable from the global settings
		data = cmssw.CMSSW.getTaskConfig(self)
		data.pop('CMSSW_CONFIG')
		return data


	def getJobConfig(self, jobNum):
		splitInfo = {}
		if self.dataSplitter:
			splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		nickname = splitInfo.get(DataSplitter.Nickname, None)

		# Put nickname dependent variables into job specific settings
		data = cmssw.CMSSW.getJobConfig(self, jobNum)
		cfg = self.nickMapCfg.get(nickname, '')
		data['CMSSW_CONFIG'] = str.join(' ', map(os.path.basename, cfg))
		for var in self.nickMapCName:
			data[var] = self.nickMapConstants.get(nickname, {}).get(var, '')

		print 'Nickname:', nickname
		print ' * Config files:', map(os.path.basename, cfg)
		print ' *    Variables:', self.nickMapConstants.get(nickname, {})
		return data
