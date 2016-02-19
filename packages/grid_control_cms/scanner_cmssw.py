#-#  Copyright 2010-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, re, xml.dom.minidom
from grid_control import utils
from grid_control.datasets import DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from python_compat import ifilter, imap, lfilter, tarfile

class ObjectsFromCMSSW(InfoScanner):
	def __init__(self, config):
		self.importParents = config.getBool('include parent infos', False)
		self.mergeConfigs = config.getBool('merge config infos', True)
		self.cfgStore = {}
		self.gtStore = {}

	def getEntries(self, path, metadata, events, seList, objStore):
		def readTag(base, tag, default = None):
			try:
				return str(base.getElementsByTagName(tag)[0].childNodes[0].data)
			except Exception:
				return default

		jobNum = metadata['GC_JOBNUM']
		tar = tarfile.open(os.path.join(path, 'cmssw.dbs.tar.gz'), 'r')
		try:
			tmpFiles = {}
			for rawdata in imap(str.split, tar.extractfile('files').readlines()):
				tmpFiles[rawdata[2]] = {'SE_OUTPUT_HASH_CRC32': rawdata[0], 'SE_OUTPUT_SIZE': int(rawdata[1])}
			objStore['CMSSW_FILES'] = tmpFiles
		except Exception:
			raise DatasetError('Could not read CMSSW file infos for job %d!' % jobNum)

		tmpCfg = {}
		cmsswVersion = tar.extractfile('version').read().strip()
		for cfg in ifilter(lambda x: ('/' not in x) and (x not in ['version', 'files']), tar.getnames()):
			try:
				cfgContent = tar.extractfile('%s/config' % cfg).read()
				cfgHashResult = tar.extractfile('%s/hash' % cfg).readlines()
				cfgHash = cfgHashResult[-1].strip()
				metadata.setdefault('CMSSW_CONFIG_JOBHASH', []).append(cfgHash)
				tmpCfg[cfg] = {'CMSSW_CONFIG_FILE': cfg, 'CMSSW_CONFIG_HASH': cfgHash, 'CMSSW_VERSION': cmsswVersion}
				tmpCfg[cfg]['CMSSW_CONFIG_CONTENT'] = self.cfgStore.setdefault(utils.QM(self.mergeConfigs, cfgHash, cfg), cfgContent)
				# Read global tag from config file - first from hash file, then from config file
				if cfgHash not in self.gtStore:
					gtLines = lfilter(lambda x: x.startswith('globaltag:'), cfgHashResult)
					if gtLines:
						self.gtStore[cfgHash] = gtLines[-1].split(':')[1].strip()
				if cfgHash not in self.gtStore:
					try:
						cfgContentEnv = utils.execWrapper(cfgContent)
						self.gtStore[cfgHash] = cfgContentEnv['process'].GlobalTag.globaltag.value()
					except Exception:
						self.gtStore[cfgHash] = 'unknown:All'
				tmpCfg[cfg]['CMSSW_GLOBALTAG'] = self.gtStore[cfgHash]
				# Get annotation from config content
				def searchConfigFile(key, regex, default):
					try:
						tmp = re.compile(regex).search(cfgContent.group(1).strip('\"\' '))
					except Exception:
						tmp = None
					if tmp:
						tmpCfg[cfg][key] = tmp
					else:
						tmpCfg[cfg][key] = default
				searchConfigFile('CMSSW_ANNOTATION', '.*annotation.*=.*cms.untracked.string.*\((.*)\)', None)
				searchConfigFile('CMSSW_DATATIER', '.*dataTier.*=.*cms.untracked.string.*\((.*)\)', 'USER')
				cfgReport = xml.dom.minidom.parseString(tar.extractfile('%s/report.xml' % cfg).read())
				evRead = sum(imap(lambda x: int(readTag(x, 'EventsRead')), cfgReport.getElementsByTagName('InputFile')))
			except Exception:
				raise DatasetError('Could not read config infos about %s in job %d' % (cfg, jobNum))

			for outputFile in cfgReport.getElementsByTagName('File'):
				tmpOut = {'CMSSW_DATATYPE': readTag(outputFile, 'DataType'), 'CMSSW_CONFIG_FILE': cfg,
					'CMSSW_EVENTS_READ': evRead, 'CMSSW_EVENTS_WRITE': int(readTag(outputFile, 'TotalEvents'))}

				# Read parent infos
				if self.importParents:
					try:
						inputs = outputFile.getElementsByTagName('Inputs')[0].getElementsByTagName('Input')
					except Exception:
						inputs = []

					tmpOut.update({'CMSSW_PARENT_PFN': [], 'CMSSW_PARENT_LFN': []})

					for inputFileElement in inputs:
						pfn = readTag(inputFileElement, 'PFN')
						lfn = readTag(inputFileElement, 'LFN')
						if not lfn:
							lfn = pfn[pfn.find('/store/'):]
						tmpOut['CMSSW_PARENT_PFN'].append(pfn)
						tmpOut['CMSSW_PARENT_LFN'].append(lfn)

				# Read lumisection infos
				lumis = []
				for run in outputFile.getElementsByTagName('Runs')[0].getElementsByTagName('Run'):
					runId = int(run.getAttribute('ID'))
					for lumi in run.getElementsByTagName('LumiSection'):
						lumis.append((runId, int(lumi.getAttribute('ID'))))
				tmpOut['CMSSW_LUMIS'] = lumis

				pfn = readTag(outputFile, 'PFN').split(':')[-1]
				tmpFiles.setdefault(pfn, {}).update(tmpOut)
		objStore.update({'CMSSW_CONFIG': tmpCfg, 'CMSSW_FILES': tmpFiles})
		tar.close()
		yield (path, metadata, events, seList, objStore)


class MetadataFromCMSSW(InfoScanner):
	def __init__(self, config):
		self.includeConfig = config.getBool('include config infos', False)

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
