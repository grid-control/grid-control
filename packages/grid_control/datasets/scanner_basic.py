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

import os, logging
from grid_control import utils
from grid_control.config import ConfigError, create_config
from grid_control.datasets import DataProvider, DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.job_db import Job
from grid_control.job_selector import JobSelector
from grid_control.utils.activity import Activity
from grid_control.utils.parsing import parse_str
from python_compat import identity, ifilter, imap, irange, izip, lfilter, lmap, set, sorted


class OutputDirsFromConfig(InfoScanner):
	# Get output directories from external config file
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		ext_config_fn = config.get_path('source config')
		ext_config = create_config(ext_config_fn, use_default_files=True).change_view(setSections=['global'])
		self._ext_workdir = ext_config.get_work_path()
		logging.getLogger().disabled = True
		self._ext_workflow = ext_config.get_plugin('workflow', 'Workflow:global', cls='Workflow', pargs=('task',))
		logging.getLogger().disabled = False
		self._ext_task = self._ext_workflow.task
		selector = config.get('source job selector', '')
		ext_job_db = ext_config.get_plugin('job database', 'TextFileJobDB', cls='JobDB',
			pkwargs={'jobSelector': lambda jobNum, jobObj: jobObj.state == Job.SUCCESS}, on_change=None)
		self._selected = sorted(ext_job_db.getJobs(JobSelector.create(selector, task=self._ext_task)))

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		activity = Activity('Reading job logs')
		for jobNum in self._selected:
			activity.update('Reading job logs - [%d / %d]' % (jobNum, self._selected[-1]))
			metadata_dict['GC_JOBNUM'] = jobNum
			obj_dict.update({'GC_TASK': self._ext_task, 'GC_WORKDIR': self._ext_workdir})
			yield (os.path.join(self._ext_workdir, 'output', 'job_%d' % jobNum), metadata_dict, entries, location_list, obj_dict)
		activity.finish()


class OutputDirsFromWork(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._ext_workdir = config.get_path('source directory')
		self._ext_output_dir = os.path.join(self._ext_workdir, 'output')
		if not os.path.isdir(self._ext_output_dir):
			raise DatasetError('Unable to find task output directory %s' % repr(self._ext_output_dir))
		self._selector = JobSelector.create(config.get('source job selector', ''))

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		allDirs = lfilter(lambda fn: fn.startswith('job_'), os.listdir(self._ext_output_dir))
		activity = Activity('Reading job logs')
		for idx, dirName in enumerate(allDirs):
			activity.update('Reading job logs - [%d / %d]' % (idx, len(allDirs)))
			try:
				metadata_dict['GC_JOBNUM'] = int(dirName.split('_')[1])
			except Exception:
				continue
			obj_dict['GC_WORKDIR'] = self._ext_workdir
			if self._selector and not self._selector(metadata_dict['GC_JOBNUM'], None):
				continue
			yield (os.path.join(self._ext_output_dir, dirName), metadata_dict, entries, location_list, obj_dict)
		activity.finish()


class MetadataFromTask(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		ignoreDef = lmap(lambda x: 'SEED_%d' % x, irange(10)) + ['DOBREAK', 'FILE_NAMES',
			'GC_DEPFILES', 'GC_JOBID', 'GC_JOBNUM', 'GC_JOB_ID', 'GC_PARAM', 'GC_RUNTIME', 'GC_VERSION',
			'JOB_RANDOM', 'JOBID', 'LANDINGZONE_LL', 'LANDINGZONE_UL', 'MY_JOB', 'MY_JOBID', 'MY_RUNTIME',
			'SB_INPUT_FILES', 'SB_OUTPUT_FILES', 'SCRATCH_LL', 'SCRATCH_UL', 'SEEDS',
			'SE_INPUT_FILES', 'SE_INPUT_PATH', 'SE_INPUT_PATTERN', 'SE_MINFILESIZE',
			'SE_OUTPUT_FILES', 'SE_OUTPUT_PATH', 'SE_OUTPUT_PATTERN', 'SUBST_FILES']
		self._ignore_vars = config.get_list('ignore task vars', ignoreDef)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if 'GC_TASK' in obj_dict:
			tmp = dict(obj_dict['GC_TASK'].getTaskConfig())
			if 'GC_JOBNUM' in metadata_dict:
				tmp.update(obj_dict['GC_TASK'].getJobConfig(metadata_dict['GC_JOBNUM']))
			for (newKey, oldKey) in obj_dict['GC_TASK'].getVarMapping().items():
				tmp[newKey] = tmp.get(oldKey)
			metadata_dict.update(utils.filter_dict(tmp, key_filter=lambda k: k not in self._ignore_vars))
		yield (item, metadata_dict, entries, location_list, obj_dict)


class FilesFromLS(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._path = config.get('source directory', '.')
		self._recurse = config.get_bool('source recurse', False)
		if ('://' in self._path) and self._recurse:
			raise DatasetError('Recursion is not supported for URL: %s' % repr(self._path))
		elif '://' not in self._path:
			self._path = utils.clean_path(self._path)

	def _iter_path(self):
		if self._recurse:
			for (root, _, files) in os.walk(self._path):
				for fn in files:
					yield utils.clean_path(os.path.join(root, fn))
		else:
			from grid_control.backends.storage import se_ls
			proc = se_ls(self._path)
			for fn in proc.stdout.iter(timeout=60):
				yield fn
			if proc.status(timeout=0) != 0:
				self._log.log_process(proc)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		metadata_dict['GC_SOURCE_DIR'] = self._path
		counter = 0
		activity = Activity('Reading source directory')
		for fn in self._iter_path():
			activity.update('Reading source directory - [%d]' % counter)
			yield (os.path.join(self._path, fn.strip()), metadata_dict, entries, location_list, obj_dict)
			counter += 1
		activity.finish()


class JobInfoFromOutputDir(InfoScanner):
	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		jobInfoPath = os.path.join(item, 'job.info')
		try:
			jobInfo = utils.DictFormat('=').parse(open(jobInfoPath))
			if jobInfo.get('exitcode') == 0:
				obj_dict['JOBINFO'] = jobInfo
				yield (item, metadata_dict, entries, location_list, obj_dict)
		except Exception:
			self._log.log(logging.INFO2, 'Unable to parse job info file %r', jobInfoPath)


class FilesFromJobInfo(InfoScanner):
	def get_ds_block_class_keys(self):
		return (['SE_OUTPUT_FILE'], ['SE_OUTPUT_PATH'])

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if 'JOBINFO' not in obj_dict:
			raise DatasetError('Job information is not filled! Ensure that "JobInfoFromOutputDir" is scheduled!')
		try:
			jobInfo = obj_dict['JOBINFO']
			files = ifilter(lambda x: x[0].startswith('file'), jobInfo.items())
			fileInfos = imap(lambda x_y: tuple(x_y[1].strip('"').split('  ')), files)
			for (hashMD5, name_local, name_dest, pathSE) in fileInfos:
				metadata_dict.update({'SE_OUTPUT_HASH_MD5': hashMD5, 'SE_OUTPUT_FILE': name_local,
					'SE_OUTPUT_BASE': os.path.splitext(name_local)[0], 'SE_OUTPUT_PATH': pathSE})
				yield (os.path.join(pathSE, name_dest), metadata_dict, entries, location_list, obj_dict)
		except Exception:
			raise DatasetError('Unable to read file stageout information!')


class FilesFromDataProvider(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		dsPath = config.get('source dataset path')
		self._source = DataProvider.create_instance('ListProvider', config, 'source dataset', dsPath)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		for block in self._source.get_block_list_cached(show_stats=False):
			for fi in block[DataProvider.FileList]:
				metadata_dict.update({'SRC_DATASET': block[DataProvider.Dataset], 'SRC_BLOCK': block[DataProvider.BlockName]})
				metadata_dict.update(dict(izip(block.get(DataProvider.Metadata, []), fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata_dict, fi[DataProvider.NEntries], block[DataProvider.Locations], obj_dict)


class MatchOnFilename(InfoScanner):
	alias_list = ['match']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._match = config.get_matcher('filename filter', '*.root', default_matcher='shell')

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if self._match.match(item) > 0:
			yield (item, metadata_dict, entries, location_list, obj_dict)


class AddFilePrefix(InfoScanner):
	alias_list = ['prefix']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._prefix = config.get('filename prefix', '')

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		yield (self._prefix + item, metadata_dict, entries, location_list, obj_dict)


class MatchDelimeter(InfoScanner):
	alias_list = ['delimeter']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		# delimeter based selection
		match_delim_str = config.get('delimeter match', '')
		self._match_delim = match_delim_str.split(':')
		# delimeter based metadata setup
		self._setup_arg_list = []
		self._guard_ds = self._setup('DELIMETER_DS',
			config.get('delimeter dataset key', ''),
			config.get('delimeter dataset modifier', ''))
		self._guard_b = self._setup('DELIMETER_B',
			config.get('delimeter block key', ''),
			config.get('delimeter block modifier', ''))

	def _setup(self, setup_vn, setup_key, setup_mod):
		if setup_key:
			(delim, delim_start_str, delim_end_str) = utils.split_opt(setup_key, '::')
			modifier = identity
			if setup_mod and (setup_mod.strip() != 'value'):
				try:
					modifier = eval('lambda value: ' + setup_mod)  # pylint:disable=eval-used
				except Exception:
					raise ConfigError('Unable to parse delimeter modifier %r' % setup_mod)
			(delim_start, delim_end) = (parse_str(delim_start_str, int), parse_str(delim_end_str, int))
			self._setup_arg_list.append((setup_vn, delim, delim_start, delim_end, modifier))
			return [setup_vn]
		return []

	def get_ds_block_class_keys(self):
		return (self._guard_ds, self._guard_b)

	def _process(self, item, metadata_dict, key, delim, delim_start, delim_end, modifier_fun):
		value = str.join(delim, os.path.basename(item).split(delim)[delim_start:delim_end])
		try:
			metadata_dict[key] = str(modifier_fun(value))
		except Exception:
			raise DatasetError('Unable to modifiy %s: %r' % (key, value))

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		fn = os.path.basename(item)
		if (len(self._match_delim) != 2) or fn.count(self._match_delim[0]) == int(self._match_delim[1]):
			for setup in self._setup_arg_list:
				self._process(item, metadata_dict, *setup)
			yield (item, metadata_dict, entries, location_list, obj_dict)


class ParentLookup(InfoScanner):
	alias_list = ['parent']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._parent_source = config.get('parent source', '')
		self._parent_keys = config.get_list('parent keys', [])
		self._parent_match_level = config.get_int('parent match level', 1)
		self._parent_merge = config.get_bool('merge parents', False)
		# cached "parent lfn parts" (plfnp) to "parent dataset name" (pdn) maps
		self._plfnp2pdn_cache = {}  # the maps are stored for different parent_dataset_expr
		self._empty_config = create_config()
		self._read_plfnp_map(config, self._parent_source)  # read from configured parent source

	def get_ds_block_class_keys(self):
		return ([], utils.QM(self._parent_merge, [], ['PARENT_PATH']))

	def _get_lfn_part(self, lfn):
		if lfn and self._parent_match_level:  # return looseMatch path elements in reverse order
			# /store/local/data/file.root -> file.root (~ML1) | file.root/data/local (~ML3)
			tmp = lfn.split('/')
			tmp.reverse()
			return str.join('/', tmp[:self._parent_match_level])
		return lfn

	def _read_plfnp_map(self, config, parent_dataset_expr):
		if parent_dataset_expr and (parent_dataset_expr not in self._plfnp2pdn_cache):
			# read parent source and fill lfnMap with parent_lfn_parts -> parent dataset name mapping
			map_plfnp2pdn = self._plfnp2pdn_cache.setdefault(parent_dataset_expr, {})
			for block in DataProvider.iter_blocks_from_expr(config, parent_dataset_expr):
				for fi in block[DataProvider.FileList]:
					map_plfnp2pdn[self._get_lfn_part(fi[DataProvider.URL])] = block[DataProvider.Dataset]
		return self._plfnp2pdn_cache.get(parent_dataset_expr, {})  # return cached mapping

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		# if parent source is not defined, try to get datacache from GC_WORKDIR
		map_plfnp2pdn = dict(self._plfnp2pdn_cache.get(self._parent_source, {}))
		datacache_path = os.path.join(obj_dict.get('GC_WORKDIR', ''), 'datacache.dat')
		if os.path.exists(datacache_path):  # extend configured parent source with datacache if it exists
			map_plfnp2pdn.update(self._read_plfnp_map(self._empty_config, datacache_path))
		pdn_list = []  # list with parent dataset names
		for key in ifilter(lambda k: k in metadata_dict, self._parent_keys):
			parent_lfn_list = metadata_dict[key]
			if not isinstance(parent_lfn_list, list):
				parent_lfn_list = [metadata_dict[key]]
			for parent_lfn in parent_lfn_list:
				pdn_list.append(map_plfnp2pdn.get(self._get_lfn_part(parent_lfn)))
		metadata_dict['PARENT_PATH'] = lfilter(identity, set(pdn_list))
		yield (item, metadata_dict, entries, location_list, obj_dict)


class DetermineEntries(InfoScanner):
	alias_list = ['DetermineEvents', 'events']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._entries_cmd = config.get(['events command', 'entries command'], '')
		self._entries_key = config.get(['events key', 'entries key'], '')
		self._entries_key_scale = config.get_float(['events per key value', 'entries per key value'], 1.)
		self._entries_default = config.get_int(['events default', 'entries default'], -1)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if (entries is None) or (entries < 0):
			entries = self._entries_default
		if self._entries_key:
			entries_meta = int(metadata_dict.get(self._entries_key, entries))
			entries = max(1, int(entries_meta * self._entries_key_scale))
		if self._entries_cmd:
			try:
				entries = int(os.popen('%s %s' % (self._entries_cmd, item)).readlines()[-1])
			except Exception:
				self._log.log(logging.INFO2, 'Unable to determine entries with %r %r', self._entries_cmd, item)
		yield (item, metadata_dict, entries, location_list, obj_dict)
