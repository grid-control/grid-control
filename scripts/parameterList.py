#!/usr/bin/env python
#-#  Copyright 2012-2016 Karlsruhe Institute of Technology
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

import os, sys, random, optparse
from gcSupport import getConfig, parseOptions, utils
from grid_control.datasets import DataSplitter
from grid_control.parameters import DataParameterSource, ParameterFactory, ParameterInfo, ParameterMetadata, ParameterSource

random.seed(0)

usage = '%s [OPTIONS] <parameter definition>' % sys.argv[0]
parser = optparse.OptionParser(usage=usage)
parser.add_option('-l', '--list-parameters', dest='listparams', default=False, action='store_true',
	help='')
parser.add_option('-M', '--manager',         dest='manager',    default=None,
	help='Select parameter source manager')
parser.add_option('-p', '--parameter',       dest='parameters', default=[], action='append',
	help='Specify parameters')
parser.add_option('-o', '--output',          dest='output',     default='',
	help='Show only specified parameters')
parser.add_option('-s', '--static',          dest='static',     default=False, action='store_true',
	help='Assume a static parameterset')
parser.add_option('-a', '--active',          dest='active',     default=False, action='store_true',
	help='Show activity state')
parser.add_option('-d', '--disabled',        dest='inactive',   default=False, action='store_true',
	help='Show disabled parameter sets')
parser.add_option('-t', '--untracked',       dest='untracked',  default=False, action='store_true',
	help='Display untracked variables')
parser.add_option('-c', '--collapse',        dest='collapse',   default=0, action='count',
	help='Do not collapse dataset infos in display')
parser.add_option('-I', '--intervention',    dest='intervention', default=False, action='store_true',
	help='Display intervention tasks')
parser.add_option('-f', '--force-intervention', dest='forceiv', default=False, action='store_true',
	help='Simulate dataset intervention')
parser.add_option('-D', '--dataset',         dest='dataset',    default='',
	help='Add dataset splitting (use "True" to simulate a dataset)')
parser.add_option('-i', '--reinit',          dest='init',       default=False, action='store_true',
	help='Trigger re-init')
parser.add_option('-r', '--resync',          dest='resync',     default=False, action='store_true',
	help='Trigger re-sync')
parser.add_option('-V', '--visible',         dest='visible',    default='',
	help='Set visible variables')
parser.add_option('-S', '--save',            dest='save',
	help='Saves information to specified file')
(opts, args) = parseOptions(parser)

if len(args) != 1:
	utils.exitWithUsage(usage)

def main():
	# Set config based on settings from config file or command line
	configFile = None
	if os.path.exists(args[0]):
		configFile = args[0]
	config = getConfig(configFile, section = 'global')
	config.changeView(setSections = ['jobs']).set('nseeds', '1', '?=')
	configParameters = config.changeView(setSections = ['parameters'])
	if opts.parameters:
		utils.vprint('Provided options:')
		for p in opts.parameters:
			k, v = p.split('=', 1)
			configParameters.set(k.strip(), v.strip().replace('\\n', '\n'), '=')
			utils.vprint('\t%s: %s' % (k.strip(), v.strip()))
		utils.vprint('')
	if not os.path.exists(args[0]):
		configParameters.set('parameters', str.join(' ', args).replace('\\n', '\n'))
	if opts.dataset:
		configParameters.set('default lookup', 'DATASETNICK')
#	configParameters.set('parameter adapter', 'BasicParameterAdapter', '=') # Don't track parameter changes
	if opts.verbosity > 2:
		config.changeView(setSections = None).write(sys.stdout)

	# Initialize ParameterFactory
	configTask = config.changeView(setSections = [config.get(['task', 'module'], 'DummyTask')])
	pm = config.getPlugin('parameter factory', 'SimpleParameterFactory', cls = ParameterFactory).getInstance()

	# Create dataset parameter source
	class DummySplitter:
		def getMaxJobs(self):
			return 3
		def getSplitInfo(self, pNum):
			mkEntry = lambda ds, fl, n, nick: { DataSplitter.Dataset: ds, DataSplitter.Nickname: nick,
				DataSplitter.FileList: fl, DataSplitter.NEntries: n }
			rndStr = lambda: md5(str(random.random())).hexdigest()[:10]
			tmp = [ mkEntry('ds1', ['a', 'b'], 23, 'data_1'), mkEntry('ds1', ['1'], 42, 'data_1'),
				mkEntry('ds2', ['m', 'n'], 123, 'data_2'), mkEntry('ds2', ['x', 'y', 'z'], 987, 'data_3') ]
			return tmp[pNum]

	class DataSplitProcessorTest:
		def getKeys(self):
			return map(lambda k: ParameterMetadata(k, untracked=True),
				['DATASETINFO', 'DATASETID', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])

		def process(self, pNum, splitInfo, result):
			result.update({
				'DATASETINFO': '',
				'DATASETID': splitInfo.get(DataSplitter.DatasetID, None),
				'DATASETPATH': splitInfo.get(DataSplitter.Dataset, None),
				'DATASETBLOCK': splitInfo.get(DataSplitter.BlockName, None),
				'DATASETNICK': splitInfo.get(DataSplitter.Nickname, None),
				'DATASETSPLIT': pNum,
			})

	if opts.dataset.lower() == 'true':
		utils.vprint('Registering dummy data provider data')
		dataSplitter = DummySplitter()
	elif opts.dataset:
		dataSplitter = DataSplitter.loadState(opts.dataset)

	if opts.dataset:
		DataParameterSource.datasetsAvailable['data'] = DataParameterSource(
			config.getWorkPath(), 'data', None, dataSplitter, DataSplitProcessorTest())

	psource = pm.getSource(config)

	if opts.forceiv:
		for dp in DataParameterSource.datasetSources:
			dp.intervention = (set([1]), set([0]), True)

	if opts.listparams:
		result = []
		needGCParam = False
		if psource.getMaxJobs() is not None:
			countActive = 0
			for jobNum in range(psource.getMaxJobs()):
				info = psource.getJobInfo(jobNum)
				if info[ParameterInfo.ACTIVE]:
					countActive += 1
				if opts.inactive or info[ParameterInfo.ACTIVE]:
					if not info[ParameterInfo.ACTIVE]:
						info['GC_PARAM'] = 'N/A'
					if str(info['GC_PARAM']) != str(jobNum):
						needGCParam = True
					result.append(info)
			if opts.displaymode == 'parseable':
				utils.vprint('Count,%d,%d' % (countActive, psource.getMaxJobs()))
			else:
				utils.vprint('Number of parameter points: %d' % psource.getMaxJobs())
				if countActive != psource.getMaxJobs():
					utils.vprint('Number of active parameter points: %d' % countActive)
		else:
			result.append(psource.getJobInfo(123))
		enabledOutput = opts.output.split(',')
		output = filter(lambda k: not opts.output or k in enabledOutput, psource.getJobKeys())
		stored = filter(lambda k: k.untracked == False, output)
		untracked = filter(lambda k: k.untracked == True, output)

		if opts.collapse > 0:
			result_old = result
			result = {}
			result_nicks = {}
			head = [('COLLATE_JOBS', '# of jobs')]
			if 'DATASETSPLIT' in stored:
				stored.remove('DATASETSPLIT')
				if (opts.collapse == 1):
					stored.append('DATASETNICK')
					head.append(('DATASETNICK', 'DATASETNICK'))
				elif opts.collapse == 2:
					head.append(('COLLATE_NICK', '# of nicks'))
			for pset in result_old:
				if ('DATASETSPLIT' in pset) and (opts.collapse == 1):
					pset.pop('DATASETSPLIT')
				nickname = None
				if ('DATASETNICK' in pset) and (opts.collapse == 2):
					nickname = pset.pop('DATASETNICK')
				h = md5(repr(map(lambda key: pset.get(key), stored))).hexdigest()
				result.setdefault(h, []).append(pset)
				result_nicks.setdefault(h, set()).add(nickname)

			def doCollate(h):
				tmp = result[h][0]
				tmp['COLLATE_JOBS'] = len(result[h])
				tmp['COLLATE_NICK'] = len(result_nicks[h])
				return tmp
			result = map(doCollate, result)
		else:
			head = [('GC_JOB_ID', '#')]
			if needGCParam:
				head.append(('GC_PARAM', 'GC_PARAM'))
		if opts.active:
			head.append((ParameterInfo.ACTIVE, 'ACTIVE'))
		if opts.visible:
			stored = opts.visible.split(',')
		head.extend(sorted(zip(stored, stored)))
		if opts.untracked:
			head.extend(sorted(map(lambda n: (n, '(%s)' % n), filter(lambda n: n not in ['GC_PARAM', 'GC_JOB_ID'], untracked))))
		utils.vprint('')
		utils.printTabular(head, result)

	if opts.save:
		utils.vprint('')
		ParameterSource.getClass('GCDumpParameterSource').write(opts.save, psource)
		utils.vprint('Parameter information saved to ./%s' % opts.save)

	if opts.intervention:
		utils.vprint('')
		tmp = psource.getJobIntervention()
		if tmp:
			if opts.displaymode == 'parseable':
				utils.vprint('R: %s' % str.join(',', map(str, tmp[0])))
				utils.vprint('D: %s' % str.join(',', map(str, tmp[1])))
			else:
				utils.vprint('   Redo: %r' % tmp[0])
				utils.vprint('Disable: %r' % tmp[1])
		else:
			if opts.displaymode == 'parseable':
				utils.vprint('NOINT')
			else:
				utils.vprint('No intervention')

if __name__ == '__main__':
	sys.exit(main())
