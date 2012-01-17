from grid_control import QM, utils, datasets
from grid_control.datasets import DataProvider, HybridSplitter, DataSplitter
from lumi_tools import *

# required format: <dataset path>[@<instance>][#<block>]
class CMSProvider(DataProvider):
	def __init__(self, config, section, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, section, datasetExpr, datasetNick, datasetID)
		# PhEDex blacklist: '-T1_DE_KIT', '-T1_US_FNAL' allow user jobs
		phedexBL = ['-T0_CH_CERN', '-T1_CH_CERN', '-T1_ES_PIC', '-T1_FR_CCIN2P3', '-T1_IT_CNAF', '-T1_TW_ASGC', '-T1_UK_RAL', '-T3_US_FNALLPC']
		self.phedexBL = config.getList(section, 'phedex sites', phedexBL)
		self.onlyComplete = config.getBool(section, 'only complete sites', True)

		(self.datasetPath, self.url, self.datasetBlock) = utils.optSplit(datasetExpr, '@#')
		self.url = QM(self.url, self.url, config.get(section, 'dbs instance', ''))
		self.datasetBlock = QM(self.datasetBlock, self.datasetBlock, 'all')
		self.onlyValid = config.getBool(section, 'only valid', True)

		# This works in tandem with active job module (cmssy.py supports only [section] lumi filter!)
		self.selectedLumis = parseLumiFilter(config.get(section, 'lumi filter', ''))
		if self.selectedLumis:
			utils.vprint('The following runs and lumi sections are selected:', -1, once = True)
			utils.vprint(utils.wrapList(formatLumi(self.selectedLumis), 65, ',\n\t'), -1, once = True)


	# Define how often the dataprovider can be queried automatically
	def queryLimit(self):
		return 2 * 60 * 60 # 2 hour delay minimum


	# Check if splitterClass is valid
	def checkSplitter(self, splitterClass):
		if self.selectedLumis and (DataSplitter.Skipped in splitterClass.neededVars()):
			utils.vprint('Active lumi section filter forced selection of HybridSplitter', -1, once = True)
			return HybridSplitter
		return splitterClass


	def blockFilter(self, blockname):
		return (self.datasetBlock == 'all') or (str.split(blockname, '#')[1] == self.datasetBlock)


	def lumiFilter(self, lumilist, runkey, lumikey):
		if self.selectedLumis:
			for lumi in lumilist:
				if selectLumi((lumi[runkey], lumi[lumikey]), self.selectedLumis):
					return True
		return self.selectedLumis == None
