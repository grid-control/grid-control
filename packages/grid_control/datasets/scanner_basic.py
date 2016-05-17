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

import os, sys, logging
from grid_control import utils
from grid_control.config import ConfigError, createConfig
from grid_control.datasets import DataProvider, DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.job_db import Job, JobDB
from grid_control.job_selector import JobSelector
from grid_control.utils.parsing import parseStr
from python_compat import identity, ifilter, imap, irange, izip, lfilter, lmap, reduce, set, sorted

def splitParse(opt):
	(delim, ds, de) = utils.optSplit(opt, '::')
	return (delim, parseStr(ds, int), parseStr(de, int))

class NullScanner(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		yield (path, metadata, events, seList, objStore)

# Get output directories from external config file
class OutputDirsFromConfig(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		ext_config_fn = config.getPath('source config')
		ext_config = createConfig(ext_config_fn).changeView(setSections = ['global'])
		self._extWorkDir = ext_config.getWorkPath()
		logging.getLogger('user').disabled = True
		self._extWorkflow = ext_config.getPlugin('workflow', 'Workflow:global', cls = 'Workflow',
			pargs = ('task',))
		logging.getLogger('user').disabled = False
		self._extTask = self._extWorkflow.task
		selector = config.get('source job selector', '')
		ext_job_db = JobDB(ext_config, jobSelector = lambda jobNum, jobObj: jobObj.state == Job.SUCCESS)
		self._selected = sorted(ext_job_db.getJobs(JobSelector.create(selector, task = self._extTask)))

	def getEntries(self, path, metadata, events, seList, objStore):
		for jobNum in self._selected:
			log = utils.ActivityLog('Reading job logs - [%d / %d]' % (jobNum, self._selected[-1]))
			metadata['GC_JOBNUM'] = jobNum
			objStore.update({'GC_TASK': self._extTask, 'GC_WORKDIR': self._extWorkDir})
			yield (os.path.join(self._extWorkDir, 'output', 'job_%d' % jobNum), metadata, events, seList, objStore)
			log.finish()


class OutputDirsFromWork(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._extWorkDir = config.getPath('source directory')
		self._extOutputDir = os.path.join(self._extWorkDir, 'output')
		self._selector = JobSelector.create(config.get('source job selector', ''))

	def getEntries(self, path, metadata, events, seList, objStore):
		allDirs = lfilter(lambda fn: fn.startswith('job_'), os.listdir(self._extOutputDir))
		for idx, dirName in enumerate(allDirs):
			log = utils.ActivityLog('Reading job logs - [%d / %d]' % (idx, len(allDirs)))
			try:
				metadata['GC_JOBNUM'] = int(dirName.split('_')[1])
			except Exception:
				continue
			objStore['GC_WORKDIR'] = self._extWorkDir
			log.finish()
			if self._selector and not self._selector(metadata['GC_JOBNUM'], None):
				continue
			elif self._selector is not None:
				continue
			yield (os.path.join(self._extOutputDir, dirName), metadata, events, seList, objStore)


class MetadataFromTask(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		ignoreDef = lmap(lambda x: 'SEED_%d' % x, irange(10)) + ['FILE_NAMES',
			'SB_INPUT_FILES', 'SE_INPUT_FILES', 'SE_INPUT_PATH', 'SE_INPUT_PATTERN',
			'SB_OUTPUT_FILES', 'SE_OUTPUT_FILES', 'SE_OUTPUT_PATH', 'SE_OUTPUT_PATTERN',
			'SE_MINFILESIZE', 'DOBREAK', 'MY_RUNTIME', 'GC_RUNTIME', 'MY_JOBID', 'GC_JOB_ID',
			'GC_VERSION', 'GC_DEPFILES', 'SUBST_FILES', 'SEEDS',
			'SCRATCH_LL', 'SCRATCH_UL', 'LANDINGZONE_LL', 'LANDINGZONE_UL']
		self._ignoreVars = config.getList('ignore task vars', ignoreDef)

	def getEntries(self, path, metadata, events, seList, objStore):
		newVerbosity = utils.verbosity(utils.verbosity() - 3)
		if 'GC_TASK' in objStore:
			tmp = dict(objStore['GC_TASK'].getTaskConfig())
			if 'GC_JOBNUM' in metadata:
				tmp.update(objStore['GC_TASK'].getJobConfig(metadata['GC_JOBNUM']))
			for (newKey, oldKey) in objStore['GC_TASK'].getVarMapping().items():
				tmp[newKey] = tmp.get(oldKey)
			metadata.update(utils.filterDict(tmp, kF = lambda k: k not in self._ignoreVars))
		utils.verbosity(newVerbosity + 3)
		yield (path, metadata, events, seList, objStore)


class FilesFromLS(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._path = config.get('source directory', '.')
		self._path = utils.QM('://' in self._path, self._path, utils.cleanPath(self._path))

	def getEntries(self, path, metadata, events, seList, objStore):
		metadata['GC_SOURCE_DIR'] = self._path
		counter = 0
		from grid_control.backends.storage import se_ls
		proc = se_ls(self._path)
		for fn in proc.stdout.iter(timeout = 60):
			log = utils.ActivityLog('Reading source directory - [%d]' % counter)
			yield (os.path.join(self._path, fn.strip()), metadata, events, seList, objStore)
			counter += 1
			log.finish()
		if proc.status(timeout = 0) != 0:
			self._log.log_process(proc)


class JobInfoFromOutputDir(InfoScanner):
	def getEntries(self, path, metadata, events, seList, objStore):
		jobInfoPath = os.path.join(path, 'job.info')
		try:
			jobInfo = utils.DictFormat('=').parse(open(jobInfoPath))
			if jobInfo.get('exitcode') == 0:
				objStore['JOBINFO'] = jobInfo
				yield (path, metadata, events, seList, objStore)
		except Exception:
			pass


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
		except KeyboardInterrupt:
			sys.exit(os.EX_TEMPFAIL)
		except Exception:
			raise DatasetError('Unable to read file stageout information!')


class FilesFromDataProvider(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		dsPath = config.get('source dataset path')
		self._source = DataProvider.createInstance('ListProvider', config, dsPath)

	def getEntries(self, path, metadata, events, seList, objStore):
		for block in self._source.getBlocks():
			for fi in block[DataProvider.FileList]:
				metadata.update({'SRC_DATASET': block[DataProvider.Dataset], 'SRC_BLOCK': block[DataProvider.BlockName]})
				metadata.update(dict(izip(block.get(DataProvider.Metadata, []), fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata, fi[DataProvider.NEntries], block[DataProvider.Locations], objStore)


class MatchOnFilename(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._match = config.getList('filename filter', ['*.root'])

	def getEntries(self, path, metadata, events, seList, objStore):
		if utils.matchFileName(path, self._match):
			yield (path, metadata, events, seList, objStore)


class AddFilePrefix(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._prefix = config.get('filename prefix', '')

	def getEntries(self, path, metadata, events, seList, objStore):
		yield (self._prefix + path, metadata, events, seList, objStore)


class MatchDelimeter(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		matchDelim = config.get('delimeter match', '')
		self._matchDelim = matchDelim.split(':')
		self._delimDS = config.get('delimeter dataset key', '')
		self._delimB = config.get('delimeter block key', '')

	def getGuards(self):
		return (utils.QM(self._delimDS, ['DELIMETER_DS'], []), utils.QM(self._delimB, ['DELIMETER_B'], []))

	def getEntries(self, path, metadata, events, seList, objStore):
		if len(self._matchDelim) == 2:
			if os.path.basename(path).count(self._matchDelim[0]) != int(self._matchDelim[1]):
				raise StopIteration
		def getVar(d, s, e):
			return str.join(d, os.path.basename(path).split(d)[s:e])
		if self._delimDS:
			metadata['DELIMETER_DS'] = getVar(*splitParse(self._delimDS))
		if self._delimB:
			metadata['DELIMETER_B'] = getVar(*splitParse(self._delimB))
		yield (path, metadata, events, seList, objStore)


class ParentLookup(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._parentKeys = config.getList('parent keys', [])
		self._looseMatch = config.getInt('parent match level', 1)
		self._source = config.get('parent source', '')
		self._merge = config.getBool('merge parents', False)
		self._lfnMap = {}

	def getGuards(self):
		return ([], utils.QM(self._merge, [], ['PARENT_PATH']))

	def lfnTrans(self, lfn):
		if lfn and self._looseMatch: # return looseMatch path elements in reverse order
			def trunkPath(x, y):
				return (lambda s: (s[0], os.path.join(x[1], s[1])))(os.path.split(x[0]))
			return reduce(trunkPath, irange(self._looseMatch), (lfn, ''))[1]
		return lfn

	def getEntries(self, path, metadata, events, seList, objStore):
		datacachePath = os.path.join(objStore.get('GC_WORKDIR', ''), 'datacache.dat')
		source = utils.QM((self._source == '') and os.path.exists(datacachePath), datacachePath, self._source)
		if source and (source not in self._lfnMap):
			pSource = DataProvider.createInstance('ListProvider', createConfig(), source)
			for (n, fl) in imap(lambda b: (b[DataProvider.Dataset], b[DataProvider.FileList]), pSource.getBlocks()):
				self._lfnMap.setdefault(source, {}).update(dict(imap(lambda fi: (self.lfnTrans(fi[DataProvider.URL]), n), fl)))
		pList = set()
		for key in ifilter(lambda k: k in metadata, self._parentKeys):
			pList.update(imap(lambda pPath: self._lfnMap.get(source, {}).get(self.lfnTrans(pPath)), metadata[key]))
		metadata['PARENT_PATH'] = lfilter(identity, pList)
		yield (path, metadata, events, seList, objStore)


class DetermineEvents(InfoScanner):
	def __init__(self, config):
		InfoScanner.__init__(self, config)
		self._eventsCmd = config.get('events command', '')
		self._eventsKey = config.get('events key', '')
		ev_per_kv = parseStr(config.get('events per key value', ''), float, 1)
		kv_per_ev = parseStr(config.get('key value per events', ''), float, -1)
		if self._eventsKey:
			if ev_per_kv * kv_per_ev >= 0: # one is zero or both are negative/positive
				raise ConfigError('Invalid value for "events per key value" or "key value per events"!')
			elif ev_per_kv > 0:
				self._eventsKeyScale = ev_per_kv
			else:
				self._eventsKeyScale = 1.0 / kv_per_ev
		self._eventsDefault = config.getInt('events default', -1)

	def getEntries(self, path, metadata, events, seList, objStore):
		if (events is None) or (events < 0):
			events = self._eventsDefault
		if self._eventsKey:
			events = max(1, int(int(metadata.get(self._eventsKey, events)) * self._eventsKeyScale))
		if self._eventsCmd:
			try:
				events = int(os.popen('%s %s' % (self._eventsCmd, path)).readlines()[-1])
			except Exception:
				pass
		yield (path, metadata, events, seList, objStore)
