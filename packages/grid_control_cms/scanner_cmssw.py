import os, re, tarfile, operator, xml.dom.minidom
from grid_control import QM, RethrowError
from grid_control.datasets import InfoScanner

class ObjectsFromCMSSW(InfoScanner):
	def __init__(self, setup, config, section):
		self.importParents = setup(config.getBool, section, 'include parent infos', False)
		self.mergeConfigs = setup(config.getBool, section, 'merge config infos', True)
		self.cfgStore = {}

	def getEntries(self, path, metadata, events, seList, objStore):
		def readTag(base, tag, default = None):
			try:
				return str(base.getElementsByTagName(tag)[0].childNodes[0].data)
			except:
				return default

		jobNum = metadata['GC_JOBNUM']
		tar = tarfile.open(os.path.join(path, 'cmssw.dbs.tar.gz'), 'r')
		try:
			tmpFiles = {}
			for rawdata in map(str.split, tar.extractfile('files').readlines()):
				tmpFiles[rawdata[2]] = {'SE_OUTPUT_HASH_CRC32': rawdata[0], 'SE_OUTPUT_SIZE': int(rawdata[1])}
			objStore['CMSSW_FILES'] = tmpFiles
		except:
			raise RethrowError('Could not read CMSSW file infos for job %d!' % jobNum)

		tmpCfg = {}
		cmsswVersion = tar.extractfile('version').read().strip()
		for cfg in filter(lambda x: not '/' in x and x not in ['version', 'files'], tar.getnames()):
			try:
				cfgContent = tar.extractfile('%s/config' % cfg).read()
				cfgHash = tar.extractfile('%s/hash' % cfg).readlines()[-1].strip()
				metadata.setdefault('CMSSW_CONFIG_JOBHASH', []).append(cfgHash)
				tmpCfg[cfg] = {'CMSSW_CONFIG_FILE': cfg, 'CMSSW_CONFIG_HASH': cfgHash, 'CMSSW_VERSION': cmsswVersion}
				tmpCfg[cfg]['CMSSW_CONFIG_CONTENT'] = self.cfgStore.setdefault(QM(self.mergeConfigs, cfgHash, cfg), cfgContent)
				# Get annotation from config content
				def searchConfigFile(key, regex, default):
					try:
						tmp = re.compile(regex).search(cfgContent.group(1).strip('\"\' '))
						tmpCfg[cfg][key] = QM(tmp, tmp, default)
					except:
						tmpCfg[cfg][key] = default
				searchConfigFile('CMSSW_ANNOTATION', '.*annotation.*=.*cms.untracked.string.*\((.*)\)', None)
				searchConfigFile('CMSSW_DATATIER', '.*dataTier.*=.*cms.untracked.string.*\((.*)\)', 'USER')
				cfgReport = xml.dom.minidom.parseString(tar.extractfile('%s/report.xml' % cfg).read())
				evRead = sum(map(lambda x: int(readTag(x, 'EventsRead')), cfgReport.getElementsByTagName('InputFile')))
			except:
				raise RethrowError('Could not read config infos about %s in job %d' % (cfg, jobNum))

			for outputFile in cfgReport.getElementsByTagName('File'):
				tmpOut = {'CMSSW_DATATYPE': readTag(outputFile, 'DataType'), 'CMSSW_CONFIG_FILE': cfg,
					'CMSSW_EVENTS_READ': evRead, 'CMSSW_EVENTS_WRITE': int(readTag(outputFile, 'TotalEvents'))}

				# Read parent infos
				if self.importParents:
					try:
						inputs = outputFile.getElementsByTagName('Inputs')[0].getElementsByTagName('Input')
					except:
						inputs = []
					tmpOut['CMSSW_PARENT_PFN'] = map(lambda x: readTag(x, 'PFN'), inputs)
					tmpOut['CMSSW_PARENT_LFN'] = map(lambda x: readTag(x, 'LFN'), inputs)

				# Read lumisection infos
				lumis = []
				for run in outputFile.getElementsByTagName('Runs')[0].getElementsByTagName('Run'):
					runId = int(run.getAttribute('ID'))
					for lumi in run.getElementsByTagName('LumiSection'):
						lumis.append((runId, int(lumi.getAttribute('ID'))))
				tmpOut['CMSSW_LUMIS'] = lumis

				tmpFiles.setdefault(readTag(outputFile, 'PFN'), {}).update(tmpOut)
		objStore.update({'CMSSW_CONFIG': tmpCfg, 'CMSSW_FILES': tmpFiles})
		tar.close()
		yield (path, metadata, events, seList, objStore)


class MetadataFromCMSSW(InfoScanner):
	def __init__(self, setup, config, section):
		self.includeConfig = setup(config.getBool, section, 'include config infos', False)

	def getEntries(self, path, metadata, events, seList, objStore):
		metadata.update(objStore['CMSSW_FILES'].get(metadata.get('SE_OUTPUT_FILE')))
		if self.includeConfig:
			metadata.update(objStore['CMSSW_CONFIG'].get(metadata.get('CMSSW_CONFIG_FILE'), {}))
		yield (path, metadata, events, seList, objStore)


class SEListFromPath(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		proto, fn = path.split(':', 1)
		if proto in ['dir', 'file']:
			yield (path, metadata, events, ['localhost'], objStore)
		elif proto in ['rfio']:
			if 'cern.ch' in seUrl:
				yield (path, metadata, events, ['caf.cern.ch'], objStore)
			else:
				yield (path, metadata, events, [fn.lstrip('/').split('/')[1]], objStore)
		elif proto in ['srm', 'gsiftp']:
			yield (path, metadata, events, [fn.split(':')[0].lstrip('/').split('/')[0]], objStore)
		else:
			yield (path, metadata, events, seList, objStore)


class LFNFromPath(InfoScanner):
	def __init__(self, setup, config, section):
		self.stripPath = setup(config.get, section, 'lfn marker', '/store/')

	def getEntries(self, path, metadata, events, seList, objStore):
		if self.stripPath and self.stripPath in path:
			yield (self.stripPath + path.split(self.stripPath, 1)[1], metadata, events, seList, objStore)
		else:
			yield (path, metadata, events, seList, objStore)


class FilterEDMFiles(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		if not (False in map(lambda x: x in metadata, ['CMSSW_EVENTS_WRITE', 'CMSSW_CONFIG_FILE'])):
			yield (path, metadata, events, seList, objStore)
