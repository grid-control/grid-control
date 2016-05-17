# | Copyright 2010-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

import os, re, xml.dom.minidom
from grid_control import utils
from grid_control.datasets import DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from python_compat import bytes2str, ifilter, imap, lfilter, tarfile

def readTag(base, tag, default = None):
	try:
		return str(base.getElementsByTagName(tag)[0].childNodes[0].data)
	except Exception:
		return default


class ObjectsFromCMSSW(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._importParents = config.getBool('include parent infos', False)
		self._mergeKey = 'CMSSW_CONFIG_FILE'
		if config.getBool('merge config infos', True):
			self._mergeKey = 'CMSSW_CONFIG_HASH'
		self._cfgStore = {}
		self._gtStore = {}

	def _processCfg(self, tar, cfg):
		cfgSummary = {}
		cfgContent = bytes2str(tar.extractfile('%s/config' % cfg).read())
		cfgHashResult = bytes2str(tar.extractfile('%s/hash' % cfg).read()).splitlines()
		cfgHash = cfgHashResult[-1].strip()
		cfgSummary = {'CMSSW_CONFIG_FILE': cfg, 'CMSSW_CONFIG_HASH': cfgHash}
		cfgSummary['CMSSW_CONFIG_CONTENT'] = self._cfgStore.setdefault(cfgSummary[self._mergeKey], cfgContent)
		# Read global tag from config file - first from hash file, then from config file
		if cfgHash not in self._gtStore:
			gtLines = lfilter(lambda x: x.startswith('globaltag:'), cfgHashResult)
			if gtLines:
				self._gtStore[cfgHash] = gtLines[-1].split(':')[1].strip()
		if cfgHash not in self._gtStore:
			try:
				cfgContentEnv = utils.execWrapper(cfgContent)
				self._gtStore[cfgHash] = cfgContentEnv['process'].GlobalTag.globaltag.value()
			except Exception:
				self._gtStore[cfgHash] = 'unknown:All'
		cfgSummary['CMSSW_GLOBALTAG'] = self._gtStore[cfgHash]
		# Get annotation from config content
		def searchConfigFile(key, regex, default):
			try:
				tmp = re.compile(regex).search(cfgContent.group(1).strip('\"\' '))
			except Exception:
				tmp = None
			if tmp:
				cfgSummary[key] = tmp
			else:
				cfgSummary[key] = default
		searchConfigFile('CMSSW_ANNOTATION', r'.*annotation.*=.*cms.untracked.string.*\((.*)\)', None)
		searchConfigFile('CMSSW_DATATIER', r'.*dataTier.*=.*cms.untracked.string.*\((.*)\)', 'USER')
		cfgReport = xml.dom.minidom.parseString(bytes2str(tar.extractfile('%s/report.xml' % cfg).read()))
		evRead = sum(imap(lambda x: int(readTag(x, 'EventsRead')), cfgReport.getElementsByTagName('InputFile')))
		return (cfgSummary, cfgReport, evRead)

	def _processOutputFile(self, cfgReport, outputFile):
		fileSummary = {'CMSSW_DATATYPE': readTag(outputFile, 'DataType'),
			'CMSSW_EVENTS_WRITE': int(readTag(outputFile, 'TotalEvents'))}
		# Read lumisection infos
		lumis = []
		for run in outputFile.getElementsByTagName('Runs')[0].getElementsByTagName('Run'):
			runId = int(run.getAttribute('ID'))
			for lumi in run.getElementsByTagName('LumiSection'):
				lumis.append((runId, int(lumi.getAttribute('ID'))))
		fileSummary['CMSSW_LUMIS'] = lumis

		# Read parent infos
		if self._importParents:
			try:
				inputs = outputFile.getElementsByTagName('Inputs')[0].getElementsByTagName('Input')
			except Exception:
				inputs = []

			fileSummary.update({'CMSSW_PARENT_PFN': [], 'CMSSW_PARENT_LFN': []})

			for inputFileElement in inputs:
				pfn = readTag(inputFileElement, 'PFN')
				lfn = readTag(inputFileElement, 'LFN')
				if not lfn:
					lfn = pfn[pfn.find('/store/'):]
				fileSummary['CMSSW_PARENT_PFN'].append(pfn)
				fileSummary['CMSSW_PARENT_LFN'].append(lfn)

		pfn = readTag(outputFile, 'PFN').split(':')[-1]
		return (fileSummary, pfn)

	def _processSteps(self, jobNum, tar, cfgSummaryMap, fileSummaryMap):
		cmsswVersion = bytes2str(tar.extractfile('version').read()).strip()
		for cfg in ifilter(lambda x: ('/' not in x) and (x not in ['version', 'files']), tar.getnames()):
			try:
				(cfgSummary, cfgReport, evRead) = self._processCfg(tar, cfg)
				cfgSummary['CMSSW_VERSION'] = cmsswVersion
				cfgSummaryMap[cfg] = cfgSummary
			except Exception:
				raise DatasetError('Could not read config infos about %s in job %d' % (cfg, jobNum))

			for outputFile in cfgReport.getElementsByTagName('File'):
				(fileSummary, pfn) = self._processOutputFile(cfgReport, outputFile)
				fileSummary['CMSSW_EVENTS_READ'] = evRead
				fileSummary['CMSSW_CONFIG_FILE'] = cfg
				fileSummaryMap.setdefault(pfn, {}).update(fileSummary)

	def getEntries(self, path, metadata, events, seList, objStore):
		jobNum = metadata['GC_JOBNUM']
		cmsRunLog = os.path.join(path, 'cmssw.dbs.tar.gz')
		if os.path.exists(cmsRunLog):
			tar = tarfile.open(cmsRunLog, 'r')
			# Collect infos about transferred files
			fileSummaryMap = {}
			try:
				for rawdata in imap(lambda value: bytes2str(value).split(), tar.extractfile('files').readlines()):
					fileSummaryMap[rawdata[2]] = {'SE_OUTPUT_HASH_CRC32': rawdata[0], 'SE_OUTPUT_SIZE': int(rawdata[1])}
				objStore['CMSSW_FILES'] = fileSummaryMap
			except Exception:
				raise DatasetError('Could not read CMSSW file infos for job %d!' % jobNum)
			# Collect infos about CMSSW processing steps
			cfgSummaryMap = {}
			self._processSteps(jobNum, tar, cfgSummaryMap, fileSummaryMap)
			for cfg in cfgSummaryMap:
				metadata.setdefault('CMSSW_CONFIG_JOBHASH', []).append(cfgSummaryMap[cfg]['CMSSW_CONFIG_HASH'])
			objStore.update({'CMSSW_CONFIG': cfgSummaryMap, 'CMSSW_FILES': fileSummaryMap})
			tar.close()
		yield (path, metadata, events, seList, objStore)


class MetadataFromCMSSW(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self.includeConfig = config.getBool('include config infos', False)

	def getEntries(self, path, metadata, events, seList, objStore):
		cmssw_files_dict = objStore.get('CMSSW_FILES', {})
		metadata.update(cmssw_files_dict.get(metadata.get('SE_OUTPUT_FILE'), {}))
		if self.includeConfig:
			cmssw_config_dict = objStore.get('CMSSW_CONFIG', {})
			metadata.update(cmssw_config_dict.get(metadata.get('CMSSW_CONFIG_FILE'), {}))
		yield (path, metadata, events, seList, objStore)


class SEListFromPath(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		tmp = path.split(':', 1)
		if len(tmp) == 1:
			tmp = ['dir', tmp[0]]
		proto, fn = tmp
		if proto in ['dir', 'file']:
			yield (path, metadata, events, ['localhost'], objStore)
		elif proto in ['rfio']:
			if 'cern.ch' in path:
				yield (path, metadata, events, ['caf.cern.ch'], objStore)
			else:
				yield (path, metadata, events, [fn.lstrip('/').split('/')[1]], objStore)
		elif proto in ['srm', 'gsiftp']:
			yield (path, metadata, events, [fn.split(':')[0].lstrip('/').split('/')[0]], objStore)
		else:
			yield (path, metadata, events, seList, objStore)


class LFNFromPath(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self.stripPath = config.get('lfn marker', '/store/')

	def getEntries(self, path, metadata, events, seList, objStore):
		if self.stripPath and self.stripPath in path:
			yield (self.stripPath + path.split(self.stripPath, 1)[1], metadata, events, seList, objStore)
		else:
			yield (path, metadata, events, seList, objStore)


class FilterEDMFiles(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		if not (False in imap(lambda x: x in metadata, ['CMSSW_EVENTS_WRITE', 'CMSSW_CONFIG_FILE'])):
			yield (path, metadata, events, seList, objStore)
