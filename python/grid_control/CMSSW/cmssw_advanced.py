import os, re, cmssw
from python_compat import *
from grid_control import datasets, utils, ConfigError
from grid_control.datasets import DataSplitter, DataProvider
from lumi_tools import *

def fromNM(nm, nickname, default):
	for pattern in filter(lambda p: p and nickname and re.search(p, nickname), nm):
		return nm[pattern]
	return nm.get(None, default)


class CMSSW_Advanced(cmssw.CMSSW):
	def __init__(self, config):
		def parseMap(x, parser):
			result = {}
			for entry in x.split('\n'):
				if '=>' in entry:
					nick, value = map(str.strip, entry.split('=>'))
				else:
					nick, value = (None, entry)
				result[nick] = filter(lambda x: x, parser(value.strip()))
			return result
		head = [(0, 'Nickname')]

		# Mapping between nickname and config files:
		cfgList = config.get(self.__class__.__name__, 'nickname config', '')
		self.nmCfg = parseMap(cfgList, lambda x: map(str.strip, x.split(',')))
		if cfgList:
			if 'config file' in config.parser.options(self.__class__.__name__):
				raise ConfigError("Please use 'nickname config' instead of 'config file'")
			allConfigFiles = utils.flatten(self.nmCfg.values())
			config.set(self.__class__.__name__, 'config file', str.join('\n', allConfigFiles))
			head.append((1, 'Config file'))

		# Mapping between nickname and constants:
		self.nmCName = map(str.strip, config.get(self.__class__.__name__, 'nickname constants', '').split())
		self.nmConst = {}
		for var in self.nmCName:
			tmp = parseMap(config.get(self.__class__.__name__, var, ''), lambda x: [x.strip()])
			for (nick, value) in tmp.items():
				if value:
					self.nmConst.setdefault(nick, {})[var] = value[0]
				else:
					self.nmConst.setdefault(nick, {})[var] = ''
			head.append((var, var))

		# Mapping between nickname and lumi filter:
		if config.get(self.__class__.__name__, 'lumi filter', '') != '':
			raise ConfigError("Please use 'nickname lumi filter' instead of 'lumi filter'")
		lumiParse = lambda x: formatLumi(parseLumiFilter(x))
		self.nmLumi = parseMap(config.get(self.__class__.__name__, 'nickname lumi filter', ''), lumiParse)
		if self.nmLumi:
			for dataset in config.get(self.__class__.__name__, 'dataset', '').splitlines():
				(datasetNick, datasetProvider, datasetExpr) = DataProvider.parseDatasetExpr(config, dataset, None)
				config.set('dataset %s' % datasetNick, 'lumi filter', str.join(',', fromNM(self.nmLumi, datasetNick, [])))
			config.set(self.__class__.__name__, 'lumi filter', str.join(',', self.nmLumi.get(None, [])))
			head.append((2, 'Lumi filter'))

		utils.vprint('Mapping between nickname and other settings:\n', -1)
		def report():
			for nick in sorted(set(self.nmCfg.keys() + self.nmConst.keys() + self.nmLumi.keys())):
				tmp = {0: nick, 1: str.join(', ', map(os.path.basename, self.nmCfg.get(nick, ''))), 2: str.join(',', (self.nmLumi.get(nick, '')))}
				yield utils.mergeDicts([tmp, self.nmConst.get(nick, {})])
		utils.printTabular(head, report(), 'cl')
		utils.vprint(level = -1)
		cmssw.CMSSW.__init__(self, config)


	def neededVars(self):
		if self.nmLumi:
			return cmssw.CMSSW.neededVars(self) + ['LUMI_RANGE']
		return cmssw.CMSSW.neededVars(self)


	def getTaskConfig(self):
		# Remove config file variable from the global settings
		data = cmssw.CMSSW.getTaskConfig(self)
		data.pop('CMSSW_CONFIG')
		return data


	def getJobConfig(self, jobNum):
		splitInfo = {}
		if self.dataSplitter:
			splitInfo = self.dataSplitter.getSplitInfo(jobNum)
		nick = splitInfo.get(DataSplitter.Nickname, None)
		# Put nick dependent variables into job specific settings
		data = cmssw.CMSSW.getJobConfig(self, jobNum)
		data['CMSSW_CONFIG'] = str.join(' ', map(os.path.basename, fromNM(self.nmCfg, nick, '')))
		constants = utils.mergeDicts([fromNM(self.nmConst, None, {}), fromNM(self.nmConst, nick, {})])
		constants = dict(map(lambda var: (var, constants.get(var, '')), self.nmCName))
		data.update(constants)
		lumifilter = fromNM(self.nmLumi, nick, '')
		if lumifilter:
			data['LUMI_RANGE'] = self.getActiveLumiFilter(lumifilter)
		utils.vprint('Nickname: %s' % nick, 1)
		utils.vprint(' * Config files: %s' % data['CMSSW_CONFIG'], 1)
		utils.vprint(' *    Variables: %s' % constants, 1)
		utils.vprint(' *   Lumi range: %s' % str.join(',', lumifilter), 1)
		return data
