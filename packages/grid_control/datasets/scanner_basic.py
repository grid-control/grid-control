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
from grid_control.utils.parsing import parseStr
from python_compat import identity, ifilter, imap, irange, izip, lfilter, lmap, set, sorted

class NullScanner(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		yield (path, metadata, events, seList, objStore)

triggerDataResync = triggerResync(['datasets', 'parameters'])

# Get output directories from external config file
class OutputDirsFromConfig(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		ext_config_fn = config.getPath('source config', onChange = triggerDataResync)
		ext_config = create_config(ext_config_fn, useDefaultFiles = True).changeView(setSections = ['global'])
		self._extWorkDir = ext_config.getWorkPath()
		logging.getLogger().disabled = True
		self._extWorkflow = ext_config.getPlugin('workflow', 'Workflow:global', cls = 'Workflow',
			pargs = ('task',))
		logging.getLogger().disabled = False
		self._extTask = self._extWorkflow.task
		selector = config.get('source job selector', '', onChange = triggerDataResync)
		ext_job_db = ext_config.getPlugin('job database', 'TextFileJobDB', cls = 'JobDB',
			pkwargs = {'jobSelector': lambda jobNum, jobObj: jobObj.state == Job.SUCCESS}, onChange = None)
		self._selected = sorted(ext_job_db.getJobs(JobSelector.create(selector, task = self._extTask)))

	def getEntries(self, path, metadata, events, seList, objStore):
		activity = Activity('Reading job logs')
		for jobNum in self._selected:
			activity.update('Reading job logs - [%d / %d]' % (jobNum, self._selected[-1]))
			metadata['GC_JOBNUM'] = jobNum
			objStore.update({'GC_TASK': self._extTask, 'GC_WORKDIR': self._extWorkDir})
			yield (os.path.join(self._extWorkDir, 'output', 'job_%d' % jobNum), metadata, events, seList, objStore)
		activity.finish()


class OutputDirsFromWork(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._extWorkDir = config.getPath('source directory', onChange = triggerDataResync)
		self._extOutputDir = os.path.join(self._extWorkDir, 'output')
		if not os.path.isdir(self._extOutputDir):
			raise DatasetError('Unable to find task output directory %s' % repr(self._extOutputDir))
		self._selector = JobSelector.create(config.get('source job selector', '', onChange = triggerDataResync))

	def getEntries(self, path, metadata, events, seList, objStore):
		allDirs = lfilter(lambda fn: fn.startswith('job_'), os.listdir(self._extOutputDir))
		activity = Activity('Reading job logs')
		for idx, dirName in enumerate(allDirs):
			activity.update('Reading job logs - [%d / %d]' % (idx, len(allDirs)))
			try:
				metadata['GC_JOBNUM'] = int(dirName.split('_')[1])
			except Exception:
				continue
			objStore['GC_WORKDIR'] = self._extWorkDir
			if self._selector and not self._selector(metadata['GC_JOBNUM'], None):
				continue
			yield (os.path.join(self._extOutputDir, dirName), metadata, events, seList, objStore)
		activity.finish()


class MetadataFromTask(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		ignoreDef = lmap(lambda x: 'SEED_%d' % x, irange(10)) + ['DOBREAK', 'FILE_NAMES',
			'GC_DEPFILES', 'GC_JOBID', 'GC_JOBNUM', 'GC_JOB_ID', 'GC_PARAM', 'GC_RUNTIME', 'GC_VERSION',
			'JOB_RANDOM', 'JOBID', 'LANDINGZONE_LL', 'LANDINGZONE_UL', 'MY_JOB', 'MY_JOBID', 'MY_RUNTIME',
			'SB_INPUT_FILES', 'SB_OUTPUT_FILES', 'SCRATCH_LL', 'SCRATCH_UL', 'SEEDS',
			'SE_INPUT_FILES', 'SE_INPUT_PATH', 'SE_INPUT_PATTERN', 'SE_MINFILESIZE',
			'SE_OUTPUT_FILES', 'SE_OUTPUT_PATH', 'SE_OUTPUT_PATTERN', 'SUBST_FILES']
		self._ignoreVars = config.getList('ignore task vars', ignoreDef, onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		if 'GC_TASK' in objStore:
			tmp = dict(objStore['GC_TASK'].getTaskConfig())
			if 'GC_JOBNUM' in metadata:
				tmp.update(objStore['GC_TASK'].getJobConfig(metadata['GC_JOBNUM']))
			for (newKey, oldKey) in objStore['GC_TASK'].getVarMapping().items():
				tmp[newKey] = tmp.get(oldKey)
			metadata.update(utils.filterDict(tmp, kF = lambda k: k not in self._ignoreVars))
		yield (path, metadata, events, seList, objStore)


class FilesFromLS(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._path = config.get('source directory', '.', onChange = triggerDataResync)
		self._recurse = config.getBool('source recurse', False, onChange = triggerDataResync)
		if ('://' in self._path) and self._recurse:
			raise DatasetError('Recursion is not supported for URL: %s' % repr(self._path))
		elif '://' not in self._path:
			self._path = utils.cleanPath(self._path)

	def _iter_path(self):
		if self._recurse:
			for (root, _, files) in os.walk(self._path):
				for fn in files:
					yield utils.cleanPath(os.path.join(root, fn))
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
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		dsPath = config.get('source dataset path', onChange = triggerDataResync)
		self._source = DataProvider.createInstance('ListProvider', config, dsPath)

	def getEntries(self, path, metadata, events, seList, objStore):
		for block in self._source.getBlocks(show_stats = False):
			for fi in block[DataProvider.FileList]:
				metadata.update({'SRC_DATASET': block[DataProvider.Dataset], 'SRC_BLOCK': block[DataProvider.BlockName]})
				metadata.update(dict(izip(block.get(DataProvider.Metadata, []), fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata, fi[DataProvider.NEntries], block[DataProvider.Locations], objStore)


class MatchOnFilename(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._match = config.getMatcher('filename filter', '*.root', defaultMatcher = 'shell', onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		if self._match.match(path) > 0:
			yield (path, metadata, events, seList, objStore)


class AddFilePrefix(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._prefix = config.get('filename prefix', '', onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		yield (self._prefix + path, metadata, events, seList, objStore)


class MatchDelimeter(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		matchDelim = config.get('delimeter match', '', onChange = triggerDataResync)
		self._matchDelim = matchDelim.split(':')

		ds_key = config.get('delimeter dataset key', '', onChange = triggerDataResync)
		ds_mod = config.get('delimeter dataset modifier', '', onChange = triggerDataResync)
		b_key = config.get('delimeter block key', '', onChange = triggerDataResync)
		b_mod = config.get('delimeter block modifier', '', onChange = triggerDataResync)
		self._ds = self._setup(ds_key, ds_mod)
		self._b = self._setup(b_key, b_mod)

	def _setup(self, setup_key, setup_mod):
		if setup_key:
			(delim, ds, de) = utils.optSplit(setup_key, '::')
			modifier = identity
			if setup_mod and (setup_mod.strip() != 'value'):
				try:
					modifier = eval('lambda value: ' + setup_mod) # pylint:disable=eval-used
				except Exception:
					raise ConfigError('Unable to parse delimeter modifier %r' % setup_mod)
			return (delim, parseStr(ds, int), parseStr(de, int), modifier)

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
		if (len(self._matchDelim) != 2) or os.path.basename(path).count(self._matchDelim[0]) == int(self._matchDelim[1]):
			self._process('DELIMETER_DS', self._ds, path, metadata)
			self._process('DELIMETER_B', self._b, path, metadata)
			yield (path, metadata, events, seList, objStore)


class ParentLookup(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._source = config.get('parent source', '', onChange = triggerDataResync)
		self._parentKeys = config.getList('parent keys', [], onChange = triggerDataResync)
		self._looseMatch = config.getInt('parent match level', 1, onChange = triggerDataResync)
		self._merge = config.getBool('merge parents', False, onChange = triggerDataResync)
		self._lfnMapCache = {}
		self._empty_config = create_config()
		self._readParents(config, self._source)

	def getGuards(self):
		return ([], utils.QM(self._merge, [], ['PARENT_PATH']))

	def _lfnTrans(self, lfn):
		if lfn and self._looseMatch: # return looseMatch path elements in reverse order
			tmp = lfn.split('/') # /store/local/data/file.root -> file.root (~ML1) | file.root/data/local (~ML3)
			tmp.reverse()
			return str.join('/', tmp[:self._looseMatch])
		return lfn

	def _readParents(self, config, source):
		# read parent source and fill lfnMap with parent_lfn_refs -> parent dataset name mapping
		if source and (source not in self._lfnMapCache):
			block_iter = DataProvider.getBlocksFromExpr(config, source)
			for (dsName, fl) in imap(lambda b: (b[DataProvider.Dataset], b[DataProvider.FileList]), block_iter):
				self._lfnMapCache.setdefault(source, {}).update(dict(imap(lambda fi: (self._lfnTrans(fi[DataProvider.URL]), dsName), fl)))
		return self._lfnMapCache.get(source, {})

	def getEntries(self, path, metadata, events, seList, objStore):
		# if parent source is not defined, try to get datacache from GC_WORKDIR
		lfnMap = dict(self._lfnMapCache.get(self._source, {}))
		datacachePath = os.path.join(objStore.get('GC_WORKDIR', ''), 'datacache.dat')
		if os.path.exists(datacachePath):
			lfnMap.update(self._readParents(self._empty_config, datacachePath))
		parent_datasets = set()
		for key in ifilter(lambda k: k in metadata, self._parentKeys):
			parent_refs = metadata[key]
			if not isinstance(parent_refs, list):
				parent_refs = [metadata[key]]
			for parent_ref in parent_refs:
				parent_datasets.add(lfnMap.get(self._lfnTrans(parent_ref)))
		metadata['PARENT_PATH'] = lfilter(identity, parent_datasets)
		yield (path, metadata, events, seList, objStore)


class DetermineEvents(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._eventsCmd = config.get('events command', '', onChange = triggerDataResync)
		self._eventsKey = config.get('events key', '', onChange = triggerDataResync)
		self._eventsKeyScale = config.getFloat('events per key value', 1., onChange = triggerDataResync)
		self._eventsDefault = config.getInt('events default', -1, onChange = triggerDataResync)

	def getEntries(self, path, metadata, events, seList, objStore):
		if (events is None) or (events < 0):
			events = self._eventsDefault
		if self._eventsKey:
			events = max(1, int(int(metadata.get(self._eventsKey, events)) * self._eventsKeyScale))
		if self._eventsCmd:
			try:
				events = int(os.popen('%s %s' % (self._eventsCmd, path)).readlines()[-1])
			except Exception:
				self._log.log(logging.INFO2, 'Unable to determine events with %r %r', self._eventsCmd, path)
		yield (path, metadata, events, seList, objStore)
