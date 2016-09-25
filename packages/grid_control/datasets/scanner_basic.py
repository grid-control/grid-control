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
from grid_control.config import ConfigError, create_config, triggerResync
from grid_control.datasets import DataProvider, DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.job_db import Job
from grid_control.job_selector import JobSelector
from grid_control.utils.activity import Activity
from grid_control.utils.parsing import parse_str
from python_compat import identity, ifilter, imap, irange, izip, lfilter, lmap, set, sorted


class NullScanner(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		yield (path, metadata, events, seList, objStore)

triggerDataResync = triggerResync(['datasets', 'parameters'])

class OutputDirsFromConfig(InfoScanner):
	# Get output directories from external config file
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		ext_config_fn = config.getPath('source config', onChange = triggerDataResync)
		ext_config = create_config(ext_config_fn, useDefaultFiles = True).changeView(setSections = ['global'])
		self._ext_workdir = ext_config.getWorkPath()
		logging.getLogger().disabled = True
		self._ext_workflow = ext_config.getPlugin('workflow', 'Workflow:global', cls = 'Workflow', pargs = ('task',))
		logging.getLogger().disabled = False
		self._ext_task = self._ext_workflow.task
		selector = config.get('source job selector', '', onChange = triggerDataResync)
		ext_job_db = ext_config.getPlugin('job database', 'TextFileJobDB', cls = 'JobDB',
			pkwargs = {'jobSelector': lambda jobNum, jobObj: jobObj.state == Job.SUCCESS}, onChange = None)
		self._selected = sorted(ext_job_db.getJobs(JobSelector.create(selector, task = self._ext_task)))

	def getEntries(self, path, metadata, events, seList, objStore):
		activity = Activity('Reading job logs')
		for jobNum in self._selected:
			activity.update('Reading job logs - [%d / %d]' % (jobNum, self._selected[-1]))
			metadata['GC_JOBNUM'] = jobNum
			objStore.update({'GC_TASK': self._ext_task, 'GC_WORKDIR': self._ext_workdir})
			yield (os.path.join(self._ext_workdir, 'output', 'job_%d' % jobNum), metadata, events, seList, objStore)
		activity.finish()


class OutputDirsFromWork(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._ext_workdir = config.getPath('source directory', onChange = triggerDataResync)
		self._ext_output_dir = os.path.join(self._ext_workdir, 'output')
		if not os.path.isdir(self._ext_output_dir):
			raise DatasetError('Unable to find task output directory %s' % repr(self._ext_output_dir))
		self._selector = JobSelector.create(config.get('source job selector', '', onChange = triggerDataResync))

	def getEntries(self, path, metadata, events, seList, objStore):
		allDirs = lfilter(lambda fn: fn.startswith('job_'), os.listdir(self._ext_output_dir))
		activity = Activity('Reading job logs')
		for idx, dirName in enumerate(allDirs):
			activity.update('Reading job logs - [%d / %d]' % (idx, len(allDirs)))
			try:
				metadata['GC_JOBNUM'] = int(dirName.split('_')[1])
			except Exception:
				continue
			objStore['GC_WORKDIR'] = self._ext_workdir
			if self._selector and not self._selector(metadata['GC_JOBNUM'], None):
				continue
			yield (os.path.join(self._ext_output_dir, dirName), metadata, events, seList, objStore)
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
		self._ignore_vars = config.getList('ignore task vars', ignoreDef, onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		if 'GC_TASK' in objStore:
			tmp = dict(objStore['GC_TASK'].getTaskConfig())
			if 'GC_JOBNUM' in metadata:
				tmp.update(objStore['GC_TASK'].getJobConfig(metadata['GC_JOBNUM']))
			for (newKey, oldKey) in objStore['GC_TASK'].getVarMapping().items():
				tmp[newKey] = tmp.get(oldKey)
			metadata.update(utils.filter_dict(tmp, key_filter = lambda k: k not in self._ignore_vars))
		yield (path, metadata, events, seList, objStore)


class FilesFromLS(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._path = config.get('source directory', '.', onChange = triggerDataResync)
		self._recurse = config.getBool('source recurse', False, onChange = triggerDataResync)
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
			for fn in proc.stdout.iter(timeout = 60):
				yield fn
			if proc.status(timeout = 0) != 0:
				self._log.log_process(proc)

	def getEntries(self, path, metadata, events, seList, objStore):
		metadata['GC_SOURCE_DIR'] = self._path
		counter = 0
		activity = Activity('Reading source directory')
		for fn in self._iter_path():
			activity.update('Reading source directory - [%d]' % counter)
			yield (os.path.join(self._path, fn.strip()), metadata, events, seList, objStore)
			counter += 1
		activity.finish()


class JobInfoFromOutputDir(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		jobInfoPath = os.path.join(path, 'job.info')
		try:
			jobInfo = utils.DictFormat('=').parse(open(jobInfoPath))
			if jobInfo.get('exitcode') == 0:
				objStore['JOBINFO'] = jobInfo
				yield (path, metadata, events, seList, objStore)
		except Exception:
			self._log.log(logging.INFO2, 'Unable to parse job info file %r', jobInfoPath)


class FilesFromJobInfo(InfoScanner):
	def getGuards(self):
		return (['SE_OUTPUT_FILE'], ['SE_OUTPUT_PATH'])

	def getEntries(self, path, metadata, events, seList, objStore):
		if 'JOBINFO' not in objStore:
			raise DatasetError('Job information is not filled! Ensure that "JobInfoFromOutputDir" is scheduled!')
		try:
			jobInfo = objStore['JOBINFO']
			files = ifilter(lambda x: x[0].startswith('file'), jobInfo.items())
			fileInfos = imap(lambda x_y: tuple(x_y[1].strip('"').split('  ')), files)
			for (hashMD5, name_local, name_dest, pathSE) in fileInfos:
				metadata.update({'SE_OUTPUT_HASH_MD5': hashMD5, 'SE_OUTPUT_FILE': name_local,
					'SE_OUTPUT_BASE': os.path.splitext(name_local)[0], 'SE_OUTPUT_PATH': pathSE})
				yield (os.path.join(pathSE, name_dest), metadata, events, seList, objStore)
		except Exception:
			raise DatasetError('Unable to read file stageout information!')


class FilesFromDataProvider(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		dsPath = config.get('source dataset path', onChange = triggerDataResync)
		self._source = DataProvider.create_instance('ListProvider', config, 'source dataset', dsPath)

	def getEntries(self, path, metadata, events, seList, objStore):
		for block in self._source.getBlocks(show_stats = False):
			for fi in block[DataProvider.FileList]:
				metadata.update({'SRC_DATASET': block[DataProvider.Dataset], 'SRC_BLOCK': block[DataProvider.BlockName]})
				metadata.update(dict(izip(block.get(DataProvider.Metadata, []), fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata, fi[DataProvider.NEntries], block[DataProvider.Locations], objStore)


class MatchOnFilename(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._match = config.getMatcher('filename filter', '*.root', defaultMatcher = 'shell', onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		if self._match.match(path) > 0:
			yield (path, metadata, events, seList, objStore)


class AddFilePrefix(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._prefix = config.get('filename prefix', '', onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		yield (self._prefix + path, metadata, events, seList, objStore)


class MatchDelimeter(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		matchDelim = config.get('delimeter match', '', onChange = triggerDataResync)
		self._match_delim = matchDelim.split(':')

		ds_key = config.get('delimeter dataset key', '', onChange = triggerDataResync)
		ds_mod = config.get('delimeter dataset modifier', '', onChange = triggerDataResync)
		b_key = config.get('delimeter block key', '', onChange = triggerDataResync)
		b_mod = config.get('delimeter block modifier', '', onChange = triggerDataResync)
		self._ds = self._setup(ds_key, ds_mod)
		self._b = self._setup(b_key, b_mod)

	def _setup(self, setup_key, setup_mod):
		if setup_key:
			(delim, ds, de) = utils.split_opt(setup_key, '::')
			modifier = identity
			if setup_mod and (setup_mod.strip() != 'value'):
				try:
					modifier = eval('lambda value: ' + setup_mod) # pylint:disable=eval-used
				except Exception:
					raise ConfigError('Unable to parse delimeter modifier %r' % setup_mod)
			return (delim, parse_str(ds, int), parse_str(de, int), modifier)

	def getGuards(self):
		return (utils.QM(self._ds, ['DELIMETER_DS'], []), utils.QM(self._b, ['DELIMETER_B'], []))

	def _process(self, key, setup, path, metadata):
		if setup is not None:
			(delim, ds, de, mod) = setup
			value = str.join(delim, os.path.basename(path).split(delim)[ds:de])
			try:
				metadata[key] = str(mod(value))
			except Exception:
				raise DatasetError('Unable to modifiy %s: %r' % (key, value))

	def getEntries(self, path, metadata, events, seList, objStore):
		if (len(self._match_delim) != 2) or os.path.basename(path).count(self._match_delim[0]) == int(self._match_delim[1]):
			self._process('DELIMETER_DS', self._ds, path, metadata)
			self._process('DELIMETER_B', self._b, path, metadata)
			yield (path, metadata, events, seList, objStore)


class ParentLookup(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._parent_source = config.get('parent source', '', onChange = triggerDataResync)
		self._parent_keys = config.getList('parent keys', [], onChange = triggerDataResync)
		self._parent_match_level = config.getInt('parent match level', 1, onChange = triggerDataResync)
		self._parent_merge = config.getBool('merge parents', False, onChange = triggerDataResync)
		self._parent_lfn_map_cache = {}
		self._empty_config = create_config()
		self._read_parents(config, self._parent_source)

	def getGuards(self):
		return ([], utils.QM(self._parent_merge, [], ['PARENT_PATH']))

	def _translate_lfn(self, lfn):
		if lfn and self._parent_match_level: # return looseMatch path elements in reverse order
			tmp = lfn.split('/') # /store/local/data/file.root -> file.root (~ML1) | file.root/data/local (~ML3)
			tmp.reverse()
			return str.join('/', tmp[:self._parent_match_level])
		return lfn

	def _read_parents(self, config, source):
		# read parent source and fill lfnMap with parent_lfn_refs -> parent dataset name mapping
		if source and (source not in self._parent_lfn_map_cache):
			block_iter = DataProvider.getBlocksFromExpr(config, source)
			for (dsName, fl) in imap(lambda b: (b[DataProvider.Dataset], b[DataProvider.FileList]), block_iter):
				self._parent_lfn_map_cache.setdefault(source, {}).update(dict(imap(lambda fi: (self._translate_lfn(fi[DataProvider.URL]), dsName), fl)))
		return self._parent_lfn_map_cache.get(source, {})

	def getEntries(self, path, metadata, events, seList, objStore):
		# if parent source is not defined, try to get datacache from GC_WORKDIR
		lfnMap = dict(self._parent_lfn_map_cache.get(self._parent_source, {}))
		datacachePath = os.path.join(objStore.get('GC_WORKDIR', ''), 'datacache.dat')
		if os.path.exists(datacachePath):
			lfnMap.update(self._read_parents(self._empty_config, datacachePath))
		parent_datasets = set()
		for key in ifilter(lambda k: k in metadata, self._parent_keys):
			parent_refs = metadata[key]
			if not isinstance(parent_refs, list):
				parent_refs = [metadata[key]]
			for parent_ref in parent_refs:
				parent_datasets.add(lfnMap.get(self._translate_lfn(parent_ref)))
		metadata['PARENT_PATH'] = lfilter(identity, parent_datasets)
		yield (path, metadata, events, seList, objStore)


class DetermineEvents(InfoScanner):
	def __init__(self, config, datasource_name):
		InfoScanner.__init__(self, config, datasource_name)
		self._events_cmd = config.get('events command', '', onChange = triggerDataResync)
		self._events_key = config.get('events key', '', onChange = triggerDataResync)
		self._events_key_scale = config.getFloat('events per key value', 1., onChange = triggerDataResync)
		self._events_default = config.getInt('events default', -1, onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		if (events is None) or (events < 0):
			events = self._events_default
		if self._events_key:
			events = max(1, int(int(metadata.get(self._events_key, events)) * self._events_key_scale))
		if self._events_cmd:
			try:
				events = int(os.popen('%s %s' % (self._events_cmd, path)).readlines()[-1])
			except Exception:
				self._log.log(logging.INFO2, 'Unable to determine events with %r %r', self._events_cmd, path)
		yield (path, metadata, events, seList, objStore)
