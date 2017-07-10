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

import os, logging
from grid_control.backends.storage import se_ls
from grid_control.config import ConfigError, create_config
from grid_control.datasets import DataProvider, DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.job_db import JobClass
from grid_control.job_selector import AndJobSelector, ClassSelector, JobSelector
from grid_control.utils import DictFormat, clean_path, split_opt
from grid_control.utils.activity import ProgressActivity
from grid_control.utils.algos import filter_dict
from grid_control.utils.parsing import parse_str
from hpfwk import clear_current_exception
from python_compat import identity, ifilter, imap, irange, izip, lfilter, lidfilter, lmap, set, sorted  # pylint:disable=line-too-long


class AddFilePrefix(InfoScanner):
	alias_list = ['prefix']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._prefix = config.get('filename prefix', '')

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		yield (self._prefix + item, metadata_dict, entries, location_list, obj_dict)


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
				clear_current_exception()
		yield (item, metadata_dict, entries, location_list, obj_dict)


class FilesFromDataProvider(InfoScanner):
	alias_list = ['provider_files']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		source_dataset_path = config.get('source dataset path')
		self._source = DataProvider.create_instance('ListProvider', config,
			'source dataset', source_dataset_path)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		for block in self._source.get_block_list_cached(show_stats=False):
			metadata_keys = block.get(DataProvider.Metadata, [])
			for fi in block[DataProvider.FileList]:
				metadata_dict['SRC_DATASET'] = block[DataProvider.Dataset]
				metadata_dict['SRC_BLOCK'] = block[DataProvider.BlockName]
				metadata_dict.update(dict(izip(metadata_keys, fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata_dict, fi[DataProvider.NEntries],
					block[DataProvider.Locations], obj_dict)


class FilesFromJobInfo(InfoScanner):
	alias_list = ['jobinfo_files']

	def get_guard_keysets(self):
		return (['SE_OUTPUT_FILE'], ['SE_OUTPUT_PATH'])

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if 'JOBINFO' not in obj_dict:
			raise DatasetError('Job infos not available! Ensure that "JobInfoFromOutputDir" is selected!')
		try:
			job_info_dict = obj_dict['JOBINFO']
			file_info_str_iter = ifilter(lambda x: x[0].startswith('file'), job_info_dict.items())
			file_info_tuple_list = imap(lambda x_y: tuple(x_y[1].strip('"').split('  ')), file_info_str_iter)
			for (file_hash, fn_local, fn_dest, se_path) in file_info_tuple_list:
				metadata_dict.update({'SE_OUTPUT_HASH_MD5': file_hash, 'SE_OUTPUT_FILE': fn_local,
					'SE_OUTPUT_BASE': os.path.splitext(fn_local)[0], 'SE_OUTPUT_PATH': se_path})
				yield (os.path.join(se_path, fn_dest), metadata_dict, entries, location_list, obj_dict)
		except Exception:
			raise DatasetError('Unable to read file stageout information!')


class FilesFromLS(InfoScanner):
	alias_list = ['ls']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._path = config.get('source directory', '.')
		self._timeout = config.get_int('source timeout', 120)
		self._trim = config.get_bool('source trim local', True)
		self._recurse = config.get_bool('source recurse', False)
		if '://' not in self._path:
			self._path = 'file://' + self._path
		(prot, path) = self._path.split('://')
		self._path = prot + '://' + clean_path(path)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		metadata_dict['GC_SOURCE_DIR'] = self._path
		progress = ProgressActivity('Reading source directory')
		for counter, size_url in enumerate(self._iter_path(self._path)):
			progress.update_progress(counter)
			metadata_dict['FILE_SIZE'] = size_url[0]
			url = size_url[1]
			if self._trim:
				url = url.replace('file://', '')
			yield (url, metadata_dict, entries, location_list, obj_dict)
		progress.finish()

	def _iter_path(self, path):
		proc = se_ls(path)
		for size_basename in proc.stdout.iter(timeout=self._timeout):
			(size, basename) = size_basename.strip().split(' ', 1)
			size = int(size)
			if size >= 0:
				yield (size, os.path.join(path, basename))
			elif self._recurse:
				for result in self._iter_path(os.path.join(path, basename)):
					yield result
		if proc.status(timeout=0) != 0:
			self._log.log_process(proc)


class JobInfoFromOutputDir(InfoScanner):
	alias_list = ['dn_jobinfo']

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		job_info_path = os.path.join(item, 'job.info')
		try:
			job_info_dict = DictFormat('=').parse(open(job_info_path))
			if job_info_dict.get('exitcode') == 0:
				obj_dict['JOBINFO'] = job_info_dict
				yield (item, metadata_dict, entries, location_list, obj_dict)
		except Exception:
			self._log.log(logging.INFO2, 'Unable to parse job info file %r', job_info_path)
			clear_current_exception()


class MatchDelimeter(InfoScanner):
	alias_list = ['delimeter']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		# delimeter based selection
		match_delim_str = config.get('delimeter match', '')
		self._match_delim = match_delim_str.split(':')
		self._match_inactive = len(self._match_delim) != 2
		# delimeter based metadata setup
		self._setup_arg_list = []
		self._guard_ds = self._setup('DELIMETER_DS',
			config.get('delimeter dataset key', ''),
			config.get('delimeter dataset modifier', ''))
		self._guard_b = self._setup('DELIMETER_B',
			config.get('delimeter block key', ''),
			config.get('delimeter block modifier', ''))

	def get_guard_keysets(self):
		return (self._guard_ds, self._guard_b)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		fn_base = os.path.basename(item)
		if self._match_inactive or fn_base.count(self._match_delim[0]) == int(self._match_delim[1]):
			for setup in self._setup_arg_list:
				self._process(item, metadata_dict, *setup)
			yield (item, metadata_dict, entries, location_list, obj_dict)

	def _process(self, item, metadata_dict, key, delim, delim_start, delim_end, modifier_fun):
		value = str.join(delim, os.path.basename(item).split(delim)[delim_start:delim_end])
		try:
			metadata_dict[key] = str(modifier_fun(value))
		except Exception:
			raise DatasetError('Unable to modifiy %s: %r' % (key, value))

	def _setup(self, setup_vn, setup_key, setup_mod):
		if setup_key:
			(delim, delim_start_str, delim_end_str) = split_opt(setup_key, '::')
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


class MatchOnFilename(InfoScanner):
	alias_list = ['match']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._match = config.get_matcher('filename filter', '*.root', default_matcher='ShellStyleMatcher')
		self._relative = config.get_bool('filename filter relative', True)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		fn_match = item
		if self._relative:
			fn_match = os.path.basename(item)
		if self._match.match(fn_match) > 0:
			yield (item, metadata_dict, entries, location_list, obj_dict)


class MetadataFromTask(InfoScanner):
	alias_list = ['task_metadata']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		ignore_list_default = lmap(lambda x: 'SEED_%d' % x, irange(10)) + ['DOBREAK', 'FILE_NAMES',
			'GC_DEPFILES', 'GC_JOBID', 'GC_JOBNUM', 'GC_JOB_ID', 'GC_PARAM', 'GC_RUNTIME', 'GC_VERSION',
			'JOB_RANDOM', 'JOBID', 'LANDINGZONE_LL', 'LANDINGZONE_UL', 'MY_JOB', 'MY_JOBID', 'MY_RUNTIME',
			'SB_INPUT_FILES', 'SB_OUTPUT_FILES', 'SCRATCH_LL', 'SCRATCH_UL', 'SEEDS',
			'SE_INPUT_FILES', 'SE_INPUT_PATH', 'SE_INPUT_PATTERN', 'SE_MINFILESIZE',
			'SE_OUTPUT_FILES', 'SE_OUTPUT_PATH', 'SE_OUTPUT_PATTERN', 'SUBST_FILES']
		self._ignore_vars = config.get_list('ignore task vars', ignore_list_default)

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		if ('GC_TASK' in obj_dict) and ('GC_JOBNUM' in metadata_dict):
			job_env_dict = obj_dict['GC_TASK'].get_job_dict(metadata_dict['GC_JOBNUM'])
			for (key_new, key_old) in obj_dict['GC_TASK'].get_var_alias_map().items():
				job_env_dict[key_new] = job_env_dict.get(key_old)
			metadata_dict.update(filter_dict(job_env_dict, key_filter=lambda k: k not in self._ignore_vars))
		yield (item, metadata_dict, entries, location_list, obj_dict)


class OutputDirsFromConfig(InfoScanner):
	alias_list = ['config_dn']

	# Get output directories from external config file
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		ext_config_fn = config.get_fn('source config')
		ext_config_raw = create_config(ext_config_fn, load_only_old_config=True)
		ext_config = ext_config_raw.change_view(set_sections=['global'])
		self._ext_work_dn = ext_config.get_work_path()
		logging.getLogger().disabled = True
		ext_workflow = ext_config.get_plugin('workflow', 'Workflow:global', cls='Workflow',
			pkwargs={'backend': 'NullWMS'})
		logging.getLogger().disabled = False
		self._ext_task = ext_workflow.task
		job_selector = JobSelector.create(config.get('source job selector', ''), task=self._ext_task)
		self._selected = sorted(ext_workflow.job_manager.job_db.get_job_list(AndJobSelector(
			ClassSelector(JobClass.SUCCESS), job_selector)))

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		progress_max = None
		if self._selected:
			progress_max = self._selected[-1] + 1
		progress = ProgressActivity('Reading job logs', progress_max)
		for jobnum in self._selected:
			progress.update_progress(jobnum)
			metadata_dict['GC_JOBNUM'] = jobnum
			obj_dict.update({'GC_TASK': self._ext_task, 'GC_WORKDIR': self._ext_work_dn})
			job_output_dn = os.path.join(self._ext_work_dn, 'output', 'job_%d' % jobnum)
			yield (job_output_dn, metadata_dict, entries, location_list, obj_dict)
		progress.finish()


class OutputDirsFromWork(InfoScanner):
	alias_list = ['work_dn']

	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._ext_work_dn = config.get_dn('source directory')
		self._ext_output_dir = os.path.join(self._ext_work_dn, 'output')
		if not os.path.isdir(self._ext_output_dir):
			raise DatasetError('Unable to find task output directory %s' % repr(self._ext_output_dir))
		self._selector = JobSelector.create(config.get('source job selector', ''))

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		dn_list = lfilter(lambda fn: fn.startswith('job_'), os.listdir(self._ext_output_dir))
		progress = ProgressActivity('Reading job logs', len(dn_list))
		for idx, dn in enumerate(dn_list):
			progress.update_progress(idx)
			try:
				metadata_dict['GC_JOBNUM'] = int(dn.split('_')[1])
			except Exception:
				clear_current_exception()
				continue
			obj_dict['GC_WORKDIR'] = self._ext_work_dn
			if self._selector and not self._selector(metadata_dict['GC_JOBNUM'], None):
				continue
			job_output_dn = os.path.join(self._ext_output_dir, dn)
			yield (job_output_dn, metadata_dict, entries, location_list, obj_dict)
		progress.finish()


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
		self._empty_config = create_config(use_default_files=False, load_old_config=False)
		self._read_plfnp_map(config, self._parent_source)  # read from configured parent source

	def get_guard_keysets(self):
		if self._parent_merge:
			return ([], [])
		return ([], ['PARENT_PATH'])

	def _get_lfnp(self, lfn):  # get lfn parts (lfnp)
		if lfn and self._parent_match_level:  # return looseMatch path elements in reverse order
			# /store/local/data/file.root -> file.root (~ML1) | file.root/data/local (~ML3)
			tmp = lfn.split('/')
			tmp.reverse()
			return str.join('/', tmp[:self._parent_match_level])
		return lfn

	def _iter_datasource_items(self, item, metadata_dict, entries, location_list, obj_dict):
		# if parent source is not defined, try to get datacache from GC_WORKDIR
		map_plfnp2pdn = dict(self._plfnp2pdn_cache.get(self._parent_source, {}))
		datacache_fn = os.path.join(obj_dict.get('GC_WORKDIR', ''), 'datacache.dat')
		if os.path.exists(datacache_fn):  # extend configured parent source with datacache if it exists
			map_plfnp2pdn.update(self._read_plfnp_map(self._empty_config, datacache_fn))
		pdn_list = []  # list with parent dataset names
		for key in ifilter(metadata_dict.__contains__, self._parent_keys):
			parent_lfn_list = metadata_dict[key]
			if not isinstance(parent_lfn_list, list):
				parent_lfn_list = [metadata_dict[key]]
			for parent_lfn in parent_lfn_list:
				pdn_list.append(map_plfnp2pdn.get(self._get_lfnp(parent_lfn)))
		metadata_dict['PARENT_PATH'] = lidfilter(set(pdn_list))
		yield (item, metadata_dict, entries, location_list, obj_dict)

	def _read_plfnp_map(self, config, parent_dataset_expr):
		if parent_dataset_expr and (parent_dataset_expr not in self._plfnp2pdn_cache):
			# read parent source and fill lfnMap with parent_lfn_parts -> parent dataset name mapping
			map_plfnp2pdn = self._plfnp2pdn_cache.setdefault(parent_dataset_expr, {})
			for block in DataProvider.iter_blocks_from_expr(self._empty_config, parent_dataset_expr):
				for fi in block[DataProvider.FileList]:
					map_plfnp2pdn[self._get_lfnp(fi[DataProvider.URL])] = block[DataProvider.Dataset]
		return self._plfnp2pdn_cache.get(parent_dataset_expr, {})  # return cached mapping
