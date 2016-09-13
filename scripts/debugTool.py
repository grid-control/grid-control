#!/usr/bin/env python
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
from gcSupport import Job, JobSelector, Options, Plugin, getConfig, scriptOptions
from grid_control import utils
from grid_control.datasets import DataProvider, DataSplitter
from python_compat import BytesBuffer, imap, irange, lmap, lzip

parser = Options()
parser.section('back', 'Backend debugging', '%s [<backend specifier>] ...')
parser.addBool('back', '', 'backend-list-nodes',     default=False, help='List backend nodes')
parser.addBool('back', '', 'backend-list-queues',    default=False, help='List backend queues')

parser.section('part', 'Dataset Partition debugging', '%s <path to partition file> ...')
parser.addText('part', '', 'partition-list',         default=None,  help='Select dataset partition information to display')
parser.addBool('part', '', 'partition-list-invalid', default=False, help='List invalidated dataset partitions')
parser.addBool('part', '', 'partition-check',        default=None,  help='Check dataset partition in specified work directory')

parser.section('jobs', 'Jobs debugging', '%s <config file / job file> ... ')
parser.addText('jobs', '', 'job-selector',           default='',    help='Display jobs matching selector')
parser.addBool('jobs', '', 'job-reset-attempts',     default=False, help='Reset the attempt counter')
parser.addText('jobs', '', 'job-force-state',        default='',    help='Force new job state')
parser.addText('jobs', '', 'job-show-jdl',           default='',    help='Show JDL file if available')

parser.section('data', 'Dataset debugging', '%s <dataset file> <dataset file> ...')
parser.addText('data', '', 'dataset-show-diff',      default='',    help='Show difference between datasets')
parser.addText('data', '', 'dataset-show-removed',   default='',    help='Find removed dataset blocks')

parser.addText(None,  'd', 'logfile-decode',         default='',    help='Decode log files')
options = scriptOptions(parser)
(opts, args) = (options.opts, options.args)

########################################################
# BACKEND

if opts.backend_list_nodes or opts.backend_list_queues:
	config = getConfig()
	backend = str.join(' ', args) or 'local'
	wms = Plugin.getClass('WMS').createInstance(backend, config, backend)
	if opts.backend_list_nodes:
		logging.info(repr(wms.getNodes()))
	if opts.backend_list_queues:
		logging.info(repr(wms.getQueues()))

########################################################
# DATASET PARTITION

def partition_invalid(splitter):
	for jobNum in irange(splitter.get_job_len()):
		splitInfo = splitter.getSplitInfo(jobNum)
		if splitInfo.get(DataSplitter.Invalid, False):
			yield {0: jobNum}

def partition_list(splitter, keyList):
	for jobNum in irange(splitter.get_job_len()):
		splitInfo = splitter.getSplitInfo(jobNum)
		tmp = lmap(lambda k: (k, splitInfo.get(k, '')), keyList)
		yield dict([('jobNum', jobNum)] + tmp)

def partition_check(splitter):
		fail = utils.set()
		for jobNum in irange(splitter.get_job_len()):
			splitInfo = splitter.getSplitInfo(jobNum)
			try:
				(events, skip, files) = (0, 0, [])
				for line in open(os.path.join(opts.checkSplitting, 'jobs', 'job_%d.var' % jobNum)).readlines():
					if 'MAX_EVENTS' in line:
						events = int(line.split('MAX_EVENTS', 1)[1].replace('=', ''))
					if 'SKIP_EVENTS' in line:
						skip = int(line.split('SKIP_EVENTS', 1)[1].replace('=', ''))
					if 'FILE_NAMES' in line:
						files = line.split('FILE_NAMES', 1)[1].replace('=', '').replace('\"', '').replace('\\', '')
						files = lmap(lambda x: x.strip().strip(','), files.split())
				def printError(curJ, curS, msg):
					if curJ != curS:
						logging.warning('%s in job %d (j:%s != s:%s)', msg, jobNum, curJ, curS)
						fail.add(jobNum)
				printError(events, splitInfo[DataSplitter.NEntries], 'Inconsistent number of events')
				printError(skip, splitInfo[DataSplitter.Skipped], 'Inconsistent number of skipped events')
				printError(files, splitInfo[DataSplitter.FileList], 'Inconsistent list of files')
			except Exception:
				logging.warning('Job %d was never initialized!', jobNum)
		if fail:
			logging.warning('Failed: ' + str.join('\n', imap(str, fail)))

if (opts.partition_list is not None) or opts.partition_list_invalid or opts.partition_check:
	if len(args) != 1:
		utils.exitWithUsage(parser.usage('part'))
	splitter = DataSplitter.loadPartitionsForScript(args[0])

	if opts.partition_list_invalid:
		utils.printTabular([(0, 'Job')], partition_invalid(splitter))

	if opts.partition_list is not None:
		if opts.partition_list in ('', 'all'):
			keyStrings = DataSplitter.enumNames
		else:
			keyStrings = opts.partition_list.split(',')
		keyList = lmap(DataSplitter.str2enum, keyStrings)
		if None in keyList:
			logging.warning('Available keys: %r', DataSplitter.enumNames)
		utils.printTabular([('jobNum', 'Job')] + lzip(keyList, keyStrings), partition_list(splitter, keyList))

	if opts.partition_check:
		logging.info('Checking %d jobs...', splitter.get_job_len())
		partition_check(splitter)

########################################################
# JOBS

def jobs_reset_attempts(jobDB, selected):
	for jobNum in jobDB.getJobsIter(selected):
		logging.info('Resetting attempts for job %d', jobNum)
		jobObj = jobDB.getJob(jobNum)
		jobObj.attempt = 0
		jobObj.history = {}
		for key in jobObj.dict.keys():
			if key.startswith('history'):
				jobObj.dict.pop(key)
		jobDB.commit(jobNum, jobObj)

def jobs_force_state(opts, jobDB, selected):
	newState = Job.str2enum(opts.job_force_state)
	if newState is None:
		raise Exception('Invalid state: %s' % opts.job_force_state)
	for jobNum in jobDB.getJobsIter(selected):
		jobObj = jobDB.getJob(jobNum)
		oldState = jobObj.state
		if oldState == newState:
			logging.info('Job is already in state %s', Job.enum2str(newState))
			continue
		jobObj.state = newState
		jobDB.commit(jobNum, jobObj)
		logging.info('Job state changed from %s to %s', Job.enum2str(oldState), Job.enum2str(newState))

def jobs_show_jdl(jobDB, selected):
	for jobNum in jobDB.getJobsIter(selected):
		jobObj = jobDB.getJob(jobNum)
		if jobObj.get('jdl'):
			logging.info(jobObj.get('jdl'))
		else:
			logging.info('Job %d: No jdl information available!')

if opts.job_selector or opts.job_reset_attempts or opts.job_force_state or opts.job_show_jdl:
	if len(args) != 1:
		utils.exitWithUsage(parser.usage('jobs'))
	config = getConfig(args[0])
	# Initialise task module
	taskName = config.get(['task', 'module'])
	task = Plugin.createInstance(taskName, config, taskName)
	jobDB = Plugin.createInstance('TextFileJobDB', config)
	selected = JobSelector.create(opts.job_selector, task = task)
	logging.info('Matching jobs: ' + str.join(' ', imap(str, jobDB.getJobsIter(selected))))
	if opts.job_reset_attempts:
		jobs_reset_attempts(jobDB, selected)
	if opts.job_force_state:
		jobs_force_state(opts, jobDB, selected)
	if opts.job_show_jdl:
		jobs_show_jdl(jobDB, selected)

########################################################
# DATASET INFOS

if opts.dataset_show_diff:
	if len(args) != 2:
		utils.exitWithUsage('%s <dataset source 1> <dataset source 2>' % sys.argv[0])
	a = DataProvider.createInstance('ListProvider', config, args[0], None)
	b = DataProvider.createInstance('ListProvider', config, args[1], None)
	(blocksAdded, blocksMissing, blocksChanged) = DataProvider.resyncSources(a.getBlocks(show_stats = False), b.getBlocks(show_stats = False))
	utils.printTabular([(DataProvider.Dataset, 'Dataset'), (DataProvider.BlockName, 'Block')], blocksMissing)

if opts.dataset_show_removed:
	if len(args) < 2:
		utils.exitWithUsage('%s <dataset source 1> <dataset source 2> ... <dataset source N> ' % sys.argv[0])
	removed = []
	oldDP = DataProvider.createInstance('ListProvider', config, args[0], None)
	for new in args[1:]:
		newDP = DataProvider.createInstance('ListProvider', config, new, None)
		(blocksAdded, blocksMissing, blocksChanged) = DataProvider.resyncSources(oldDP.getBlocks(show_stats = False), newDP.getBlocks(show_stats = False))
		for block in blocksMissing:
			tmp = dict(block)
			tmp[-1] = new
			removed.append(tmp)
		oldDP = newDP
	utils.printTabular([(DataProvider.Dataset, 'Dataset'), (DataProvider.BlockName, 'Block'), (-1, 'Removed in file')], removed)

if opts.logfile_decode:
	import base64, gzip
	from grid_control.utils.file_objects import ZipFile
	if opts.logfile_decode.endswith('.gz'):
		fp = ZipFile(opts.logfile_decode, 'r')
	else:
		fp = open(opts.logfile_decode, 'r')

	for line in fp.readlines():
		if line.startswith('(B64) '):
			buffer = BytesBuffer(base64.b64decode(line.replace('(B64) ', '')))
			line = gzip.GzipFile(fileobj = buffer).read().decode('ascii')
		sys.stdout.write(line.rstrip() + '\n')
	fp.close()
