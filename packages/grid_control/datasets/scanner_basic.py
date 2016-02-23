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

import os, sys
from grid_control import utils
from grid_control.config import createConfig
from grid_control.datasets import DataProvider, DatasetError
from grid_control.datasets.scanner_base import InfoScanner
from grid_control.job_db import Job, JobDB
from grid_control.job_selector import JobSelector
from grid_control.utils.parsing import parseStr
from python_compat import identity, ifilter, imap, irange, izip, lfilter, lmap, reduce, set, sorted

def splitParse(opt):
	(delim, ds, de) = utils.optSplit(opt, '::')
	return (delim, parseStr(ds, int), parseStr(de, int))

# Get output directories from external config file
class OutputDirsFromConfig(InfoScanner):
	def __init__(self, config):
		from grid_control.tasks import TaskModule
		newVerbosity = utils.verbosity(utils.verbosity() - 3)
		extConfigFN = config.getPath('source config')
		extConfig = createConfig(extConfigFN).changeView(setSections = ['global'])
		self.extWorkDir = extConfig.getWorkPath()
		self.extTask = extConfig.getPlugin(['task', 'module'], cls = TaskModule)
		selector = config.get('source job selector', '')
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
	def __init__(self, config):
		self.extWorkDir = config.get('source directory')
		self.extOutputDir = os.path.join(self.extWorkDir, 'output')

	def getEntries(self, path, metadata, events, seList, objStore):
		log = None
		allDirs = ifilter(lambda fn: fn.startswith('job_'), os.listdir(self.extOutputDir))
		for idx, dirName in enumerate(allDirs):
			try:
				metadata['GC_JOBNUM'] = int(dirName.split('_')[1])
				objStore['GC_WORKDIR'] = self.extWorkDir
				del log
				log = utils.ActivityLog('Reading job logs - [%d / %d]' % (idx, len(allDirs)))
				yield (os.path.join(self.extOutputDir, dirName), metadata, events, seList, objStore)
			except Exception:
				pass


class MetadataFromTask(InfoScanner):
	def __init__(self, config):
		ignoreDef = lmap(lambda x: 'SEED_%d' % x, irange(10)) + ['FILE_NAMES',
			'SB_INPUT_FILES', 'SE_INPUT_FILES', 'SE_INPUT_PATH', 'SE_INPUT_PATTERN',
			'SB_OUTPUT_FILES', 'SE_OUTPUT_FILES', 'SE_OUTPUT_PATH', 'SE_OUTPUT_PATTERN',
			'SE_MINFILESIZE', 'DOBREAK', 'MY_RUNTIME', 'GC_RUNTIME', 'MY_JOBID', 'GC_JOB_ID',
			'GC_VERSION', 'GC_DEPFILES', 'SUBST_FILES', 'SEEDS',
			'SCRATCH_LL', 'SCRATCH_UL', 'LANDINGZONE_LL', 'LANDINGZONE_UL']
		self.ignoreVars = config.getList('ignore task vars', ignoreDef)

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
	def __init__(self, config):
		self.path = config.get('source directory', '.')
		self.path = utils.QM('://' in self.path, self.path, utils.cleanPath(self.path))

	def getEntries(self, path, metadata, events, seList, objStore):
		metadata['GC_SOURCE_DIR'] = self.path
		counter = 0
		from grid_control.backends.storage import se_ls
		proc = se_ls(self.path)
		for fn in proc.iter():
			log = utils.ActivityLog('Reading source directory - [%d]' % counter)
			yield (os.path.join(self.path, fn.strip()), metadata, events, seList, objStore)
			counter += 1
		if proc.wait():
			utils.eprint(proc.getError())


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
		dsPath = config.get('source dataset path')
		self.source = DataProvider.createInstance('ListProvider', config, dsPath)

	def getEntries(self, path, metadata, events, seList, objStore):
		for block in self.source.getBlocks():
			for fi in block[DataProvider.FileList]:
				metadata.update({'SRC_DATASET': block[DataProvider.Dataset], 'SRC_BLOCK': block[DataProvider.BlockName]})
				metadata.update(dict(izip(block.get(DataProvider.Metadata, []), fi.get(DataProvider.Metadata, []))))
				yield (fi[DataProvider.URL], metadata, fi[DataProvider.NEntries], block[DataProvider.Locations], objStore)


class MatchOnFilename(InfoScanner):
	def __init__(self, config):
		self.match = config.getList('filename filter', ['*.root'])

	def getEntries(self, path, metadata, events, seList, objStore):
		if utils.matchFileName(path, self.match):
			yield (path, metadata, events, seList, objStore)


class AddFilePrefix(InfoScanner):
	def __init__(self, config):
		self.prefix = config.get('filename prefix', '')

	def getEntries(self, path, metadata, events, seList, objStore):
		yield (self.prefix + path, metadata, events, seList, objStore)


class MatchDelimeter(InfoScanner):
	def __init__(self, config):
		self.matchDelim = config.get('delimeter match', '').split(':')
		self.delimDS = config.get('delimeter dataset key', '')
		self.delimB = config.get('delimeter block key', '')

	def getGuards(self):
		return (utils.QM(self.delimDS, ['DELIMETER_DS'], []), utils.QM(self.delimB, ['DELIMETER_B'], []))

	def getEntries(self, path, metadata, events, seList, objStore):
		if len(self.matchDelim) == 2:
			if os.path.basename(path).count(self.matchDelim[0]) != self.matchDelim[1]:
				raise StopIteration
		def getVar(d, s, e):
			return str.join(d, os.path.basename(path).split(d)[s:e])
		if self.delimDS:
			metadata['DELIMETER_DS'] = getVar(*splitParse(self.delimDS))
		if self.delimB:
			metadata['DELIMETER_B'] = getVar(*splitParse(self.delimB))
		yield (path, metadata, events, seList, objStore)


class ParentLookup(InfoScanner):
	def __init__(self, config):
		self.parentKeys = config.getList('parent keys', [])
		self.looseMatch = config.getInt('parent match level', 1)
		self.source = config.get('parent source', '')
		self.merge = config.getBool('merge parents', False)
		self.lfnMap = {}

	def getGuards(self):
		return ([], utils.QM(self.merge, [], ['PARENT_PATH']))

	def lfnTrans(self, lfn):
		if lfn and self.looseMatch: # return looseMatch path elements in reverse order
			def trunkPath(x, y):
				return (lambda s: (s[0], os.path.join(x[1], s[1])))(os.path.split(x[0]))
			return reduce(trunkPath, irange(self.looseMatch), (lfn, ''))[1]
		return lfn

	def getEntries(self, path, metadata, events, seList, objStore):
		datacachePath = os.path.join(objStore.get('GC_WORKDIR', ''), 'datacache.dat')
		source = utils.QM((self.source == '') and os.path.exists(datacachePath), datacachePath, self.source)
		if source and (source not in self.lfnMap):
			pSource = DataProvider.createInstance('ListProvider', createConfig(), source)
			for (n, fl) in imap(lambda b: (b[DataProvider.Dataset], b[DataProvider.FileList]), pSource.getBlocks()):
				self.lfnMap.setdefault(source, {}).update(dict(imap(lambda fi: (self.lfnTrans(fi[DataProvider.URL]), n), fl)))
		pList = set()
		for key in ifilter(lambda k: k in metadata, self.parentKeys):
			pList.update(imap(lambda pPath: self.lfnMap.get(source, {}).get(self.lfnTrans(pPath)), metadata[key]))
		metadata['PARENT_PATH'] = lfilter(identity, pList)
		yield (path, metadata, events, seList, objStore)


class DetermineEvents(InfoScanner):
	def __init__(self, config):
		self._eventsCmd = config.get('events command', '')
		self._eventsKey = config.get('events key', '')
		self._eventsKeyScale = config.getInt('events key scale', 1)
		self._eventsDefault = config.getInt('events default', -1)

	def getEntries(self, path, metadata, events, seList, objStore):
		if events < 0:
			events = self._eventsDefault
		if self._eventsKey:
			events = int(metadata.get(self._eventsKey, events)) / self._eventsKeyScale
		if self._eventsCmd:
			try:
				events = int(os.popen('%s %s' % (self._eventsCmd, path)).readlines()[-1])
			except Exception:
				pass
		yield (path, metadata, events, seList, objStore)
