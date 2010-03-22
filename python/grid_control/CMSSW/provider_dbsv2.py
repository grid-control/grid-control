import DBSAPI_v2.dbsApi
from grid_control import utils, DatasetError, datasets
from grid_control.datasets import DataProvider

def createDBSAPI(url):
	if url == '':
		url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
	if not 'http://' in url:
		url = 'http://cmsdbsprod.cern.ch/%s/servlet/DBSServlet' % url
	return DBSAPI_v2.dbsApi.DbsApi({'version': 'DBS_2_0_6', 'level': 'CRITICAL', 'url': url})


def parseLumiFilter(lumifilter):
	tmp = []
	for token in map(str.strip, lumifilter.split(",")):
		def mysplit(x, sep):
			if x == None:
				return (None, None)
			parts = map(str.strip, x.split(sep))
			a = None
			if len(parts) > 0 and parts[0] != "":
				a = parts[0]
			b = None
			if len(parts) > 1 and parts[1] != "":
				b = parts[1]
			if len(parts) > 2:
				raise
			return (a, b)
		def makeint(x):
			if x:
				return int(x)
			return x
		tmp.append(tuple(map(lambda x: map(makeint, mysplit(x, ":")), mysplit(token, "-"))))
	def cmpLumi(a,b):
		(start_a_run, start_a_lumi) = a[0]
		(start_b_run, start_b_lumi) = b[0]
		if start_a_run == start_b_run:
			return cmp(start_a_lumi, start_b_lumi)
		else:
			return cmp(start_a_run, start_b_run)
	tmp.sort(cmpLumi)
	return tmp


def selectLumi(run_lumi, lumifilter):
	(run, lumi) = run_lumi
	for (sel_start, sel_end) in lumifilter:
		(sel_start_run, sel_start_lumi) = sel_start
		(sel_end_run, sel_end_lumi) = sel_end
		if (sel_start_run == None) or (run >= sel_start_run):
			if (sel_start_lumi == None) or (lumi >= sel_start_lumi):
				if (sel_end_run == None) or (run <= sel_end_run):
					if (sel_end_lumi == None) or (lumi <= sel_end_lumi):
						return True
	return False


# required format: <dataset path>[@<instance>][#<block>][%<run-lumis>]
class DBSApiv2(DataProvider):
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		DataProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		DataProvider.providers.update({'DBSApiv2': 'dbs'})
		if config.getBool('CMSSW', 'dbs blacklist T1', True):
			T1SEs = ["-srmcms.pic.es", "-ccsrm.in2p3.fr", "-storm-fe-cms.cr.cnaf.infn.it",
				"-srm-cms.gridpp.rl.ac.uk", "-srm.grid.sinica.edu.tw", "-srm2.grid.sinica.edu.tw"]
			self.sitefilter.extend(T1SEs)

		self.url = config.get('CMSSW', 'dbs instance', '')
		(self.datasetPath, datasetUrl, self.datasetBlock) = utils.optSplit(datasetExpr, '@#')
		if not self.datasetBlock:
			self.datasetBlock = 'all'
		if datasetUrl != '':
			self.url = datasetUrl
		self.selectedLumis = parseLumiFilter(config.get('CMSSW', 'lumi filter', ''))


	def getBlocksInternal(self):
		api = createDBSAPI(self.url)
		try:
			listBlockInfo = api.listBlocks(self.datasetPath)
			listFileInfo = api.listFiles(self.datasetPath)
			listLumiInfo = {}
			for fileInfo in listFileInfo:
				lfn = fileInfo['LogicalFileName']
				listLumiInfo[lfn] = []
				for lumi in api.listFileLumis(lfn):
					listLumiInfo[lfn].append((lumi["RunNumber"], lumi["LumiSectionNumber"]))
		except DBSAPI_v2.dbsApiException.DbsException, ex:
			raise DatasetError('DBS exception\n%s: %s' % (ex.getClassName(), ex.getErrorMessage()))

		if len(listBlockInfo) == 0:
			raise DatasetError('Dataset %s has no registered blocks in dbs.' % self.datasetPath)
		if len(listFileInfo) == 0:
			raise DatasetError('Dataset %s has no registered files in dbs.' % self.datasetPath)

		def blockFilter(block):
			if (self.datasetBlock == 'all'):
				return True
			if (str.split(block['Name'], '#')[1] == self.datasetBlock) :
				return True
			return False

		def lumiFilter(lfn):
			for lumi in listLumiInfo[lfn]:
				if selectLumi(lumi, self.selectedLumis):
					return True
			return False

		result = []
		for block in filter(blockFilter, listBlockInfo):
			blockInfo = dict()
			blockInfo[DataProvider.Dataset] = str.split(block['Name'], '#')[0]
			blockInfo[DataProvider.BlockName] = str.split(block['Name'], '#')[1]

			blockInfo[DataProvider.SEList] = []
			for seName in block['StorageElementList']:
				blockInfo[DataProvider.SEList].append(seName['Name'])

			dropped = 0
			blockInfo[DataProvider.FileList] = []
			for entry in listFileInfo:
				if block['Name'] == entry['Block']['Name']:
					if lumiFilter(entry['LogicalFileName']):
						blockInfo[DataProvider.FileList].append({
							DataProvider.lfn     : entry['LogicalFileName'],
							DataProvider.NEvents : entry['NumberOfEvents']
						})
					else:
						dropped += 1

			recordedFiles = len(blockInfo[DataProvider.FileList]) + dropped
			if recordedFiles != block['NumberOfFiles']:
				print('Inconsistency in dbs block %s: Number of files doesn\'t match (b:%d != f:%d)'
					% (block['Name'], block['NumberOfFiles'], recordedFiles))
			if dropped == 0:
				blockInfo[DataProvider.NEvents] = block['NumberOfEvents']
			result.append(blockInfo)

		if len(result) == 0:
			raise DatasetError('Block %s not found in dbs.' % self.datasetBlock)
		return result
