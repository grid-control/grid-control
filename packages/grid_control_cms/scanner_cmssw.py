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
from grid_control.datasets.provider_scan import GCProviderSetup
from grid_control.datasets.scanner_base import InfoScanner
from python_compat import all, bytes2str, ifilter, imap, lfilter, tarfile


class GCProviderSetup_CMSSW(GCProviderSetup):
	scan_pipeline = ['ObjectsFromCMSSW', 'JobInfoFromOutputDir', 'FilesFromJobInfo',
		'MatchOnFilename', 'MatchDelimeter', 'MetadataFromCMSSW', 'SEListFromPath',
		'LFNFromPath', 'Determineentries', 'AddFilePrefix']


def readTag(base, tag, default = None):
	try:
		return str(base.getElementsByTagName(tag)[0].childNodes[0].data)
	except Exception:
		return default


def readList(base, container, items):
	try:
		return base.getElementsByTagName(container)[0].getElementsByTagName(items)
	except Exception:
		return []


class ObjectsFromCMSSW(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._import_parents = config.get_bool('include parent infos', False)
		self._merge_key = 'CMSSW_CONFIG_FILE'
		if config.get_bool('merge config infos', True):
			self._merge_key = 'CMSSW_CONFIG_HASH'
		self._stored_config = {}
		self._stored_globaltag = {}
		self._regex_annotation = re.compile(r'.*annotation.*=.*cms.untracked.string.*\((.*)\)')
		self._regex_datatier = re.compile(r'.*dataTier.*=.*cms.untracked.string.*\((.*)\)')

	def _processCfg(self, tar, cfg):
		cfgSummary = {}
		cfgContent = bytes2str(tar.extractfile('%s/config' % cfg).read())
		cfgHashResult = bytes2str(tar.extractfile('%s/hash' % cfg).read()).splitlines()
		cfgHash = cfgHashResult[-1].strip()
		cfgSummary = {'CMSSW_CONFIG_FILE': cfg, 'CMSSW_CONFIG_HASH': cfgHash}
		cfgSummary['CMSSW_CONFIG_CONTENT'] = self._stored_config.setdefault(cfgSummary[self._merge_key], cfgContent)
		# Read global tag from config file - first from hash file, then from config file
		if cfgHash not in self._stored_globaltag:
			gtLines = lfilter(lambda x: x.startswith('globaltag:'), cfgHashResult)
			if gtLines:
				self._stored_globaltag[cfgHash] = gtLines[-1].split(':')[1].strip()
		if cfgHash not in self._stored_globaltag:
			try:
				cfgContentEnv = utils.exec_wrapper(cfgContent)
				self._stored_globaltag[cfgHash] = cfgContentEnv['process'].GlobalTag.globaltag.value()
			except Exception:
				self._stored_globaltag[cfgHash] = 'unknown:All'
		cfgSummary['CMSSW_GLOBALTAG'] = self._stored_globaltag[cfgHash]
		# Get annotation from config content
		def searchConfigFile(key, regex, default):
			try:
				cfgSummary[key] = regex.search(cfgContent).group(1).strip('\"\' ') or default
			except Exception:
				cfgSummary[key] = default
		searchConfigFile('CMSSW_ANNOTATION', self._regex_annotation, None)
		searchConfigFile('CMSSW_DATATIER', self._regex_datatier, 'USER')
		cfgReport = xml.dom.minidom.parseString(bytes2str(tar.extractfile('%s/report.xml' % cfg).read()))
		evRead = sum(imap(lambda x: int(readTag(x, 'entriesRead')), cfgReport.getElementsByTagName('InputFile')))
		return (cfgSummary, cfgReport, evRead)

	def _processOutputFile(self, cfgReport, outputFile):
		fileSummary = {'CMSSW_DATATYPE': readTag(outputFile, 'DataType'),
			'CMSSW_entries_WRITE': int(readTag(outputFile, 'Totalentries'))}
		# Read lumisection infos
		lumis = []
		for run in readList(outputFile, 'Runs', 'Run'):
			runId = int(run.getAttribute('ID'))
			for lumi in run.getElementsByTagName('LumiSection'):
				lumis.append((runId, int(lumi.getAttribute('ID'))))
		fileSummary['CMSSW_LUMIS'] = lumis

		# Read parent infos
		if self._import_parents:
			inputs = readList(outputFile, 'Inputs', 'Input')
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
				fileSummary['CMSSW_entries_READ'] = evRead
				fileSummary['CMSSW_CONFIG_FILE'] = cfg
				fileSummaryMap.setdefault(pfn, {}).update(fileSummary)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		jobNum = metadata_dict['GC_JOBNUM']
		cmsRunLog = os.path.join(item, 'cmssw.dbs.tar.gz')
		if os.path.exists(cmsRunLog):
			tar = tarfile.open(cmsRunLog, 'r')
			# Collect infos about transferred files
			fileSummaryMap = {}
			try:
				for rawdata in imap(lambda value: bytes2str(value).split(), tar.extractfile('files').readlines()):
					fileSummaryMap[rawdata[2]] = {'SE_OUTPUT_HASH_CRC32': rawdata[0], 'SE_OUTPUT_SIZE': int(rawdata[1])}
				obj_dict['CMSSW_FILES'] = fileSummaryMap
			except Exception:
				raise DatasetError('Could not read CMSSW file infos for job %d!' % jobNum)
			# Collect infos about CMSSW processing steps
			cfgSummaryMap = {}
			self._processSteps(jobNum, tar, cfgSummaryMap, fileSummaryMap)
			for cfg in cfgSummaryMap:
				metadata_dict.setdefault('CMSSW_CONFIG_JOBHASH', []).append(cfgSummaryMap[cfg]['CMSSW_CONFIG_HASH'])
			obj_dict.update({'CMSSW_CONFIG': cfgSummaryMap, 'CMSSW_FILES': fileSummaryMap})
			tar.close()
		yield (item, metadata_dict, entries, location_list, obj_dict)


class MetadataFromCMSSW(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._include_config = config.get_bool('include config infos', False)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		cmssw_files_dict = obj_dict.get('CMSSW_FILES', {})
		metadata_dict.update(cmssw_files_dict.get(metadata_dict.get('SE_OUTPUT_FILE'), {}))
		if self._include_config:
			cmssw_config_dict = obj_dict.get('CMSSW_CONFIG', {})
			metadata_dict.update(cmssw_config_dict.get(metadata_dict.get('CMSSW_CONFIG_FILE'), {}))
		yield (item, metadata_dict, entries, location_list, obj_dict)


class SEListFromPath(InfoScanner):
	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		tmp = item.split(':', 1)
		if len(tmp) == 1:
			tmp = ['dir', tmp[0]]
		proto, fn = tmp
		if proto in ['dir', 'file']:
			yield (item, metadata_dict, entries, ['localhost'], obj_dict)
		elif proto in ['rfio']:
			if 'cern.ch' in item:
				yield (item, metadata_dict, entries, ['caf.cern.ch'], obj_dict)
			else:
				yield (item, metadata_dict, entries, [fn.lstrip('/').split('/')[1]], obj_dict)
		elif proto in ['srm', 'gsiftp']:
			yield (item, metadata_dict, entries, [fn.split(':')[0].lstrip('/').split('/')[0]], obj_dict)
		else:
			yield (item, metadata_dict, entries, location_list, obj_dict)


class LFNFromPath(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._strip_path = config.get('lfn marker', '/store/')

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if self._strip_path and self._strip_path in item:
			yield (self._strip_path + item.split(self._strip_path, 1)[1], metadata_dict, entries, location_list, obj_dict)
		else:
			yield (item, metadata_dict, entries, location_list, obj_dict)


class FilterEDMFiles(InfoScanner):
	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if all(imap(lambda x: x in metadata_dict, ['CMSSW_entries_WRITE', 'CMSSW_CONFIG_FILE'])):
			yield (item, metadata_dict, entries, location_list, obj_dict)
