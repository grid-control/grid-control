#!/usr/bin/env python
import random
from gcSupport import *
from grid_control.datasets import DataSplitter
from grid_control.parameters import ParameterFactory, ParameterSource, ParameterInfo, DataParameterSource
random.seed(0)

usage = '%s [OPTIONS] <parameter definition>' % sys.argv[0]
parser = optparse.OptionParser(usage=usage)
parser.add_option('-l', '--list-parameters', dest='listparams', default=False, action='store_true',
	help='')
parser.add_option('-M', '--manager',         dest='manager',    default=None,
	help='Select plugin manager')
parser.add_option('-p', '--parameter',       dest='parameters', default=[], action='append',
	help='Specify parameters')
parser.add_option('-o', '--output',          dest='output',     default="",
	help='Show only specified parameters')
parser.add_option('-s', '--static',          dest='static',     default=False, action='store_true',
	help='Assume a static parameterset')
parser.add_option('-a', '--active',          dest='active',     default=False, action='store_true',
	help='Show activity state')
parser.add_option('-d', '--disabled',        dest='inactive',   default=False, action='store_true',
	help='Show disabled parameter sets')
parser.add_option('-t', '--untracked',       dest='untracked',  default=False, action='store_true',
	help='Display untracked variables')
parser.add_option('-I', '--intervention',    dest='intervention', default=False, action='store_true',
	help='Display intervention tasks')
parser.add_option('-f', '--force-intervention', dest='forceiv', default=False, action='store_true',
	help='Simulate dataset intervention')
parser.add_option('-D', '--dataset',         dest='dataset',    default="",
	help='Add dataset splitting (use "True" to simulate a dataset)')
parser.add_option('-i', '--reinit',          dest='init',       default=False, action='store_true',
	help='Trigger re-init')
parser.add_option('-r', '--resync',          dest='resync',     default=False, action='store_true',
	help='Trigger re-sync')
parser.add_option('-S', '--save',            dest='save',
	help='Saves information to specified file')
(opts, args) = parseOptions(parser)

# we need exactly one positional argument (dataset path)
if len(args) != 1:
	utils.exitWithUsage(usage)

if os.path.exists(args[0]):
	config = Config(args[0])
	if not opts.manager:
		mod = config.get('global', 'module')
		opts.manager = config.get(mod, 'parameter factory', 'SimpleParameterFactory')
else:
	if not opts.manager:
		opts.manager = 'SimpleParameterFactory'
	utils.vprint('Provided options:')
	paramSettings = {'parameters': str.join(' ', args).replace('\\n', '\n')}
	for p in opts.parameters:
		k, v = p.split('=', 1)
		paramSettings[k.strip()] = v.strip().replace('\\n', '\n')
		utils.vprint('\t%s: %s' % (k.strip(), v.strip()))
	utils.vprint('')
	config = Config(configDict={'jobs': {'nseeds': 1}, 'parameters': paramSettings})

config.set('parameters', 'parameter adapter', 'BasicParameterAdapter')
config.opts = opts
config.workDir = '.'
config.getTaskDict = lambda: utils.PersistentDict(None)

if opts.dataset:
	config.set('parameters', 'lookup', 'DATASETNICK')
pm = ParameterFactory.open(opts.manager, config, ['parameters'])

# Create dataset parameter plugin
class DummySplitter:
	def getMaxJobs(self):
		return 3
	def getSplitInfo(self, pNum):
		mkEntry = lambda ds, fl, n, nick: { DataSplitter.Dataset: ds, DataSplitter.Nickname: nick,
			DataSplitter.FileList: fl, DataSplitter.NEvents: n }
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
		config.workDir, 'data', None, dataSplitter, DataSplitProcessorTest())

plugin = pm.getSource(config)

if opts.forceiv:
	for dp in DataParameterSource.datasetSources:
		dp.intervention = (set([1]), set([0]), True)

if opts.listparams:
	result = []
	if plugin.getMaxJobs() != None:
		countActive = 0
		for jobNum in range(plugin.getMaxJobs()):
			info = plugin.getJobInfo(jobNum)
			if info[ParameterInfo.ACTIVE]:
				countActive += 1
			if opts.inactive or info[ParameterInfo.ACTIVE]:
				if not info[ParameterInfo.ACTIVE]:
					info['GC_PARAM'] = 'N/A'
				result.append(info)
		if opts.displaymode == 'parseable':
			print 'Count,%d,%d' % (countActive, plugin.getMaxJobs())
		else:
			print 'Number of parameter points:', plugin.getMaxJobs()
			print 'Number of active parameter points:', countActive
	else:
		result.append(plugin.getJobInfo(123))
	enabledOutput = opts.output.split(',')
	output = filter(lambda k: not opts.output or k in enabledOutput, plugin.getJobKeys())
	stored = filter(lambda k: k.untracked == False, output)
	untracked = filter(lambda k: k.untracked == True, output)
	head = [('MY_JOBID', '#'), ('GC_PARAM', 'GC_PARAM')]
	if opts.active:
		head.append((ParameterInfo.ACTIVE, 'ACTIVE'))
	head.extend(sorted(zip(stored, stored)))
	if opts.untracked:
		head.extend(sorted(map(lambda n: (n, '(%s)' % n), filter(lambda n: n not in ['GC_PARAM', 'MY_JOBID'], untracked))))
	print
	utils.printTabular(head, result)

if opts.save:
	print
	plugins.plugin_file.GCDumpParaPlugin.write(opts.save, plugin)
	print 'Parameter information saved to ./%s' % opts.save

if opts.intervention:
	print
	tmp = plugin.getJobIntervention()
	if tmp:
		if opts.displaymode == 'parseable':
			print "R:", str.join(',', map(str, tmp[0]))
			print "D:", str.join(',', map(str, tmp[1]))
		else:
			print "   Redo:", tmp[0]
			print "Disable:", tmp[1]
	else:
		if opts.displaymode == 'parseable':
			print "NOINT"
		else:
			print "No intervention"
