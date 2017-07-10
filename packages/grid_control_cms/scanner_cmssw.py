# | Copyright 2010-2017 Karlsruhe Institute of Technology
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
from grid_control.datasets import DatasetError
from grid_control.datasets.provider_scan import GCProviderSetup
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.utils import exec_wrapper
from hpfwk import clear_current_exception
from python_compat import all, bytes2str, ifilter, imap, lfilter, tarfile


class GCProviderSetupCMSSW(GCProviderSetup):
	alias_list = ['GCProviderSetup_CMSSW']
	scanner_list = ['ObjectsFromCMSSW', 'JobInfoFromOutputDir', 'FilesFromJobInfo',
		'MatchOnFilename', 'MatchDelimeter', 'MetadataFromCMSSW', 'SEListFromPath',
		'LFNFromPath', 'DetermineEvents', 'AddFilePrefix']


class FilterEDMFiles(InfoScanner):
	alias_list = ['edm']

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if all(imap(metadata_dict.__contains__, ['CMSSW_EVENTS_WRITE', 'CMSSW_CONFIG_FILE'])):
			yield (item, metadata_dict, entries, location_list, obj_dict)


class LFNFromPath(InfoScanner):
	alias_list = ['lfn']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._strip_path = config.get('lfn marker', '/store/')

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if self._strip_path and self._strip_path in item:
			yield (self._strip_path + item.split(self._strip_path, 1)[1],
				metadata_dict, entries, location_list, obj_dict)
		else:
			yield (item, metadata_dict, entries, location_list, obj_dict)


class MetadataFromCMSSW(InfoScanner):
	alias_list = ['cmssw_metadata']

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


class ObjectsFromCMSSW(InfoScanner):
	alias_list = ['cmssw_obj']

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

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		jobnum = metadata_dict['GC_JOBNUM']
		cms_log_fn = os.path.join(item, 'cmssw.dbs.tar.gz')
		if os.path.exists(cms_log_fn):
			tar = tarfile.open(cms_log_fn, 'r')
			# Collect infos about transferred files
			file_summary_map = {}
			try:
				file_info_str_list = tar.extractfile('files').readlines()
				for rawdata in imap(lambda value: bytes2str(value).split(), file_info_str_list):
					file_summary_map[rawdata[2]] = {
						'SE_OUTPUT_HASH_CRC32': rawdata[0],
						'SE_OUTPUT_SIZE': int(rawdata[1])
					}
				obj_dict['CMSSW_FILES'] = file_summary_map
			except Exception:
				raise DatasetError('Could not read CMSSW file infos for job %d!' % jobnum)
			# Collect infos about CMSSW processing steps
			config_summary_map = {}
			self._process_steps(jobnum, tar, config_summary_map, file_summary_map)
			for cfg in config_summary_map:
				job_hash_list = metadata_dict.setdefault('CMSSW_CONFIG_JOBHASH', [])
				job_hash_list.append(config_summary_map[cfg]['CMSSW_CONFIG_HASH'])
			obj_dict.update({'CMSSW_CONFIG': config_summary_map, 'CMSSW_FILES': file_summary_map})
			tar.close()
		yield (item, metadata_dict, entries, location_list, obj_dict)

	def _process_config(self, tar, cfg):
		config_summary = {}
		config_content = bytes2str(tar.extractfile('%s/config' % cfg).read())
		config_hash_result = bytes2str(tar.extractfile('%s/hash' % cfg).read()).splitlines()
		config_hash = config_hash_result[-1].strip()
		config_summary = {'CMSSW_CONFIG_FILE': cfg, 'CMSSW_CONFIG_HASH': config_hash}
		config_summary['CMSSW_CONFIG_CONTENT'] = self._stored_config.setdefault(
			config_summary[self._merge_key], config_content)
		# Read global tag from config file - first from hash file, then from config file
		if config_hash not in self._stored_globaltag:
			global_tag_lines = lfilter(lambda x: x.startswith('globaltag:'), config_hash_result)
			if global_tag_lines:
				self._stored_globaltag[config_hash] = global_tag_lines[-1].split(':')[1].strip()
		if config_hash not in self._stored_globaltag:
			try:
				config_content_env = exec_wrapper(config_content)
				self._stored_globaltag[config_hash] = config_content_env['process'].GlobalTag.globaltag.value()
			except Exception:
				clear_current_exception()
				self._stored_globaltag[config_hash] = 'unknown:All'
		config_summary['CMSSW_GLOBALTAG'] = self._stored_globaltag[config_hash]

		# Get annotation from config content
		def _search_config_file(key, regex, default):
			try:
				config_summary[key] = regex.search(config_content).group(1).strip('\"\' ') or default
			except Exception:
				clear_current_exception()
				config_summary[key] = default
		_search_config_file('CMSSW_ANNOTATION', self._regex_annotation, None)
		_search_config_file('CMSSW_DATATIER', self._regex_datatier, 'USER')
		config_report = xml.dom.minidom.parseString(
			bytes2str(tar.extractfile('%s/report.xml' % cfg).read()))
		events_read = sum(imap(lambda x: int(_read_tag(x, 'EventsRead')),
			config_report.getElementsByTagName('InputFile')))
		return (config_summary, config_report, events_read)

	def _process_output_file(self, config_report, output_file_node):
		file_summary = {
			'CMSSW_DATATYPE': _read_tag(output_file_node, 'DataType'),
			'CMSSW_EVENTS_WRITE': int(_read_tag(output_file_node, 'TotalEvents'))
		}
		# Read lumisection infos
		lumis = []
		for run in _read_list(output_file_node, 'Runs', 'Run'):
			run_id = int(run.getAttribute('ID'))
			for lumi in run.getElementsByTagName('LumiSection'):
				lumis.append((run_id, int(lumi.getAttribute('ID'))))
		file_summary['CMSSW_LUMIS'] = lumis

		# Read parent infos
		if self._import_parents:
			inputs = _read_list(output_file_node, 'Inputs', 'Input')
			file_summary.update({'CMSSW_PARENT_PFN': [], 'CMSSW_PARENT_LFN': []})

			for input_file_node in inputs:
				pfn = _read_tag(input_file_node, 'PFN')
				lfn = _read_tag(input_file_node, 'LFN')
				if not lfn:
					lfn = pfn[pfn.find('/store/'):]
				file_summary['CMSSW_PARENT_PFN'].append(pfn)
				file_summary['CMSSW_PARENT_LFN'].append(lfn)

		pfn = _read_tag(output_file_node, 'PFN').split(':')[-1]
		return (file_summary, pfn)

	def _process_steps(self, jobnum, tar, config_summary_map, file_summary_map):
		cmssw_version = bytes2str(tar.extractfile('version').read()).strip()
		for cfg in ifilter(lambda x: ('/' not in x) and (x not in ['version', 'files']), tar.getnames()):
			try:
				(config_summary, config_report, events_read) = self._process_config(tar, cfg)
				config_summary['CMSSW_VERSION'] = cmssw_version
				config_summary_map[cfg] = config_summary
			except Exception:
				raise DatasetError('Could not read config infos about %s in job %d' % (cfg, jobnum))

			for output_file_node in config_report.getElementsByTagName('File'):
				(file_summary, pfn) = self._process_output_file(config_report, output_file_node)
				file_summary['CMSSW_EVENTS_READ'] = events_read
				file_summary['CMSSW_CONFIG_FILE'] = cfg
				file_summary_map.setdefault(pfn, {}).update(file_summary)


class SEListFromPath(InfoScanner):
	alias_list = ['storage']

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


def _read_list(base, container, items):
	try:
		return base.getElementsByTagName(container)[0].getElementsByTagName(items)
	except Exception:
		clear_current_exception()
		return []


def _read_tag(base, tag, default=None):
	try:
		return str(base.getElementsByTagName(tag)[0].childNodes[0].data)
	except Exception:
		clear_current_exception()
		return default
