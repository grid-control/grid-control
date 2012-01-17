from grid_control import QM, utils, DatasetError, RethrowError, datasets
from grid_control.datasets import DataProvider, HybridSplitter, DataSplitter
from provider_cms import CMSProvider
from python_compat import *
from cms_ws import *

# required format: <dataset path>[@<instance>][#<block>]
class DASProvider(CMSProvider):
	def getBlocksInternal(self, noFiles):
		api = createDBSAPI(self.url)
		def getWithPhedex(listBlockInfo, seList):
			# Get dataset list from PhEDex (concurrent with listFiles)
			phedexQuery = []
			for block in listBlockInfo:
				phedexQuery.extend(readJSON('https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas',
					{'block': block['Name']})['phedex']['block'])
			for phedexBlock in phedexQuery:
				phedexSelector = lambda x: (x['complete'] == 'y') or not self.onlyComplete
				phedexSites = dict(map(lambda x: (x['node'], x['se']), filter(phedexSelector, phedexBlock['replica'])))
				phedexSitesOK = utils.doBlackWhiteList(phedexSites.keys(), self.phedexBL)
				seList[phedexBlock['name']] = map(lambda x: phedexSites[x], phedexSitesOK)
		try:
			toProcess = [self.datasetPath]
			if '*' in self.datasetPath:
				pd, sd, dt = (self.datasetPath.lstrip("/") + "/*/*/*").split("/")[:3]
				toProcess = map(lambda x: x.get("PathList", [])[-1], api.listProcessedDatasets(pd, dt, sd))
				utils.vprint("DBS dataset wildcard selected:\n\t%s\n" % str.join("\n\t", toProcess), -1)
			# Start thread to retrieve list of files
			(listBlockInfo, listFileInfo, seList) = ([], [], {})
			def listFileInfoThread(self, api, path, result):
				result.extend(api.listFiles(path, retriveList=['retrive_status'] + QM(self.selectedLumis, ['retrive_lumi'], [])))
			for datasetPath in toProcess:
				thisBlockInfo = api.listBlocks(datasetPath, nosite = self.phedex)
				if not noFiles:
					tFile = utils.gcStartThread("Retrieval of file infos for %s" % datasetPath,
						listFileInfoThread, self, api, datasetPath, listFileInfo)
					if self.phedex:
						getWithPhedex(thisBlockInfo, seList)
					tFile.join()
				listBlockInfo.extend(thisBlockInfo)
		except:
			raise RethrowError('DBS exception')

		if len(listBlockInfo) == 0:
			raise DatasetError('Dataset %s has no registered blocks in dbs.' % self.datasetPath)
		if len(listFileInfo) == 0 and not noFiles:
			raise DatasetError('Dataset %s has no registered files in dbs.' % self.datasetPath)

		def blockFilter(block):
			return (self.datasetBlock == 'all') or (str.split(block['Name'], '#')[1] == self.datasetBlock)

		def lumiFilter(lumilist):
			if self.selectedLumis:
				for lumi in lumilist:
					if selectLumi((lumi['RunNumber'], lumi['LumiSectionNumber']), self.selectedLumis):
						return True
			return self.selectedLumis == None

		result = []
		for block in filter(lambda xblockFilter, listBlockInfo):
			blockInfo = dict()
			blockInfo[DataProvider.Dataset] = str.split(block['Name'], '#')[0]
			blockInfo[DataProvider.BlockName] = str.split(block['Name'], '#')[1]
			if self.phedex:
				blockInfo[DataProvider.SEList] = seList.get(block['Name'], [])
			else:
				blockInfo[DataProvider.SEList] = map(lambda x: x['Name'], block['StorageElementList'])

			dropped = 0
			blockInfo[DataProvider.FileList] = []
			if self.selectedLumis:
				blockInfo[DataProvider.Metadata] = ['Runs']
			for entry in listFileInfo:
				if block['Name'] == entry['Block']['Name']:
					if lumiFilter(entry['LumiList']) and (entry['Status'] == 'VALID' or not self.onlyValid):
						blockInfo[DataProvider.FileList].append({
							DataProvider.lfn      : entry['LogicalFileName'],
							DataProvider.NEvents  : entry['NumberOfEvents'],
							DataProvider.Metadata : [list(set(map(lambda x: int(x['RunNumber']), entry['LumiList'])))]
						})
					else:
						dropped += 1

			recordedFiles = len(blockInfo[DataProvider.FileList]) + dropped
			if recordedFiles != block['NumberOfFiles'] and not noFiles:
				utils.eprint('Inconsistency in dbs block %s: Number of files doesn\'t match (b:%d != f:%d)'
					% (block['Name'], block['NumberOfFiles'], recordedFiles))
			if dropped == 0:
				blockInfo[DataProvider.NEvents] = block['NumberOfEvents']
			if len(blockInfo[DataProvider.FileList]) > 0 or noFiles:
				result.append(blockInfo)

		if len(result) == 0:
			if self.selectedLumis:
				utils.eprint('Dataset %s does not contain the requested run/lumi sections!' % self.datasetPath)
			else:
				raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)
		return result
