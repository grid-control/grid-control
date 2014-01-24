import os, sys
from grid_control import QM, utils, ConfigError, storage, JobSelector, LoadableObject, Config, JobDB, JobSelector, Job, RethrowError
from scanner_base import InfoScanner
from grid_control.datasets import DataProvider
from python_compat import *

def splitParse(opt):
	(delim, ds, de) = utils.optSplit(opt, '::')
	return (delim, utils.parseInt(ds), utils.parseInt(de))

# Get output directories from external config file
class OutputDirsFromConfig(InfoScanner):
	def __init__(self, config, section):
		from grid_control import TaskModule
		newVerbosity = utils.verbosity(utils.verbosity() - 3)
		extConfig = Config(config.getPath(section, 'source config'))
		extConfig.opts = type('DummyType', (), {'init': False, 'resync': False})
		self.extWorkDir = extConfig.workDir
		self.extTask = TaskModule.open(extConfig.get('global', ['task', 'module']), extConfig)
		selector = config.get(section, 'source job selector', '')
		extJobDB = JobDB(extConfig, jobSelector = lambda jobNum, jobObj: jobObj.state == Job.SUCCESS)
		self.selected = sorted(extJobDB.getJobs(JobSelector.create(selector, task = self.extTask)))
		utils.verbosity(newVerbosity + 3)

	def getEntries(self, path, metadata, events, seList, objStore):
		log = None
		for jobNum in self.selected:
			del log
			log = utils.ActivityLog('Reading job logs - [%d / %d]' % (jobNum, self.selected[-1]))
			metadata['GC_JOBNUM'] = jobNum
			objStore.update({'GC_TASK': self.extTask, 'GC_WORKDIR': self.extWorkDir})
			yield (os.path.join(self.extWorkDir, 'output', 'job_%d' % jobNum), metadata, events, seList, objStore)


class OutputDirsFromWork(InfoScanner):
	def __init__(self, config, section):
		self.extWorkDir = config.get(section, 'source directory')
		self.extOutputDir = os.path.join(self.extWorkDir, 'output')

	def getEntries(self, path, metadata, events, seList, objStore):
		log = None
		allDirs = filter(lambda fn: fn.startswith('job_'), os.listdir(self.extOutputDir))
		for idx, dirName in enumerate(allDirs):
			try:
				metadata['GC_JOBNUM'] = int(dirName.split('_')[1])
				objStore['GC_WORKDIR'] = self.extWorkDir
				del log
				log = utils.ActivityLog('Reading job logs - [%d / %d]' % (idx, len(allDirs)))
				yield (os.path.join(self.extOutputDir, dirName), metadata, events, seList, objStore)
			except:
				pass


class MetadataFromTask(InfoScanner):
	def __init__(self, config, section):
		ignoreDef = map(lambda x: 'SEED_%d' % x, range(10)) + ['FILE_NAMES',
			'SB_INPUT_FILES', 'SE_INPUT_FILES', 'SE_INPUT_PATH', 'SE_INPUT_PATTERN',
			'SB_OUTPUT_FILES', 'SE_OUTPUT_FILES', 'SE_OUTPUT_PATH', 'SE_OUTPUT_PATTERN',
			'SE_MINFILESIZE', 'DOBREAK', 'MY_RUNTIME', 'MY_JOBID',
			'GC_VERSION', 'GC_DEPFILES', 'SUBST_FILES', 'SEEDS',
			'SCRATCH_LL', 'SCRATCH_UL', 'LANDINGZONE_LL', 'LANDINGZONE_UL']
		self.ignoreVars = config.getList(section, 'ignore task vars', ignoreDef)

	def getEntries(self, path, metadata, events, seList, objStore):
		newVerbosity = utils.verbosity(utils.verbosity() - 3)
		if 'GC_TASK' in objStore:
			tmp = dict(objStore['GC_TASK'].getTaskConfig())
			if 'GC_JOBNUM' in metadata:
				tmp.update(objStore['GC_TASK'].getJobConfig(metadata['GC_JOBNUM']))
			for (newKey, oldKey) in objStore['GC_TASK'].getVarMapping().items():
				tmp[newKey] = tmp.get(oldKey)
			metadata.update(utils.filterDict(tmp, kF = lambda k: k not in self.ignoreVars))
		utils.verbosity(newVerbosity + 3)
		yield (path, metadata, events, seList, objStore)


class FilesFromLS(InfoScanner):
	def __init__(self, config, section):
		self.path = config.get(section, 'source directory', '.')
		self.path = QM('://' in self.path, self.path, utils.cleanPath(self.path))

	def getEntries(self, path, metadata, events, seList, objStore):
		metadata['GC_SOURCE_DIR'] = self.path
		(log, counter) = (None, 0)
		proc = storage.se_ls(self.path)
		for fn in proc.iter():
			log = utils.ActivityLog('Reading source directory - [%d]' % counter)
			yield (os.path.join(self.path, fn.strip()), metadata, events, seList, objStore)
			counter += 1
		if proc.wait():
			utils.eprint(proc.getError())


class FilesFromJobInfo(InfoScanner):
	def getGuards(self):
		return (['SE_OUTPUT_FILE'], ['SE_OUTPUT_PATH'])

	def getEntries(self, path, metadata, events, seList, objStore):
		jobInfoPath = os.path.join(path, 'job.info')
		try:
			jobInfo = utils.DictFormat('=').parse(open(jobInfoPath))
			files = filter(lambda x: x[0].startswith('file'), jobInfo.items())
			fileInfos = map(lambda (x, y): tuple(y.strip('"').split('  ')), files)
			for (hashMD5, name_local, name_dest, pathSE) in fileInfos:
				metadata.update({'SE_OUTPUT_HASH_MD5': hashMD5, 'SE_OUTPUT_FILE': name_local,
					'SE_OUTPUT_BASE': os.path.splitext(name_local)[0], 'SE_OUTPUT_PATH': pathSE})
				yield (os.path.join(pathSE, name_dest), metadata, events, seList, objStore)
		except KeyboardInterrupt:
			sys.exit(0)
		except:
			raise RethrowError('Unable to read job results from %s!' % jobInfoPath)


class FilesFromDataProvider(InfoScanner):
	def __init__(self, config, section):
		dsPath = config.get(section, 'source dataset path')
		self.source = DataProvider.create(config, None, dsPath, 'ListProvider')

	def getEntries(self, path, metadata, events, seList, objStore):
		for block in self.source.getBlocks():
			for fi in block[DataProvider.FileList]:
				metadata.update({'SRC_DATASET': block[DataProvider.Dataset], 'SRC_BLOCK': block[DataProvider.BlockName]})
				metadata.update(dict(zip(block.get(DataProvider.Metadata, []), fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata, fi[DataProvider.NEntries], block[DataProvider.SEList], objStore)


class MatchOnFilename(InfoScanner):
	def __init__(self, config, section):
		self.match = config.getList(section, 'filename filter', ['*.root'])

	def getEntries(self, path, metadata, events, seList, objStore):
		if utils.matchFileName(path, self.match):
			yield (path, metadata, events, seList, objStore)


class AddFilePrefix(InfoScanner):
	def __init__(self, config, section):
		self.prefix = config.get(section, 'filename prefix', '')

	def getEntries(self, path, metadata, events, seList, objStore):
		yield (self.prefix + path, metadata, events, seList, objStore)


class MatchDelimeter(InfoScanner):
	def __init__(self, config, section):
		self.matchDelim = config.get(section, 'delimeter match', '').split(':')
		self.delimDS = config.get(section, 'delimeter dataset key', '')
		self.delimB = config.get(section, 'delimeter block key', '')

	def getGuards(self):
		return (QM(self.delimDS, ['DELIMETER_DS'], []), QM(self.delimB, ['DELIMETER_B'], []))

	def getEntries(self, path, metadata, events, seList, objStore):
		if len(self.matchDelim) == 2:
			if os.path.basename(path).count(self.matchDelim[0]) != self.matchDelim[1]:
				raise StopIteration
		getVar = lambda (d, s, e): str.join(d, os.path.basename(path).split(d)[s:e])
		if self.delimDS:
			metadata['DELIMETER_DS'] = getVar(splitParse(self.delimDS))
		if self.delimB:
			metadata['DELIMETER_B'] = getVar(splitParse(self.delimB))
		yield (path, metadata, events, seList, objStore)


class ParentLookup(InfoScanner):
	def __init__(self, config, section):
		self.parentKeys = config.getList(section, 'parent keys', [])
		self.looseMatch = config.getInt(section, 'parent match level', 1)
		self.source = config.get(section, 'parent source', '')
		self.merge = config.getBool(section, 'merge parents', False)
		self.lfnMap = {}

	def getGuards(self):
		return ([], QM(self.merge, [], ['PARENT_PATH']))

	def lfnTrans(self, lfn):
		if lfn and self.looseMatch:
			trunkPath = lambda x, y: (lambda s: (s[0], os.path.join(x[1], s[1])))(os.path.split(x[0]))
			return reduce(trunkPath, range(self.looseMatch), (lfn, ''))[1]
		return lfn

	def getEntries(self, path, metadata, events, seList, objStore):
		datacachePath = os.path.join(objStore.get('GC_WORKDIR', ''), 'datacache.dat')
		source = QM((self.source == '') and os.path.exists(datacachePath), datacachePath, self.source)
		if source and (source not in self.lfnMap):
			pSource = DataProvider.create(Config(), None, source, 'ListProvider')
			for (n, fl) in map(lambda b: (b[DataProvider.Dataset], b[DataProvider.FileList]), pSource.getBlocks()):
				self.lfnMap.setdefault(source, {}).update(dict(map(lambda fi: (self.lfnTrans(fi[DataProvider.URL]), n), fl)))
		pList = set()
		for key in filter(lambda k: k in metadata, self.parentKeys):
			pList.update(map(lambda pPath: self.lfnMap.get(source, {}).get(self.lfnTrans(pPath)), metadata[key]))
		metadata['PARENT_PATH'] = filter(lambda x: x, pList)
		yield (path, metadata, events, seList, objStore)


class DetermineEvents(InfoScanner):
	def __init__(self, config, section):
		self.eventsCmd = config.get(section, 'events command', '')
		self.eventsKey = config.get(section, 'events key', '')
		self.eventsDefault = config.get(section, 'events default', -1)

	def getEntries(self, path, metadata, events, seList, objStore):
		events = int(metadata.get(self.eventsKey, QM(events >= 0, events, self.eventsDefault)))
		if self.eventsCmd:
			try:
				events = int(os.popen('%s %s' % (self.eventsCmd, path)).readlines()[-1])
			except:
				pass
		yield (path, metadata, events, seList, objStore)
