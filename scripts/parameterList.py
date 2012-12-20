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
parser.add_option('-M', '--manager',         dest='manager',    default='EasyParameterFactory',
	help='Select plugin manager')
parser.add_option('-p', '--parameter',       dest='parameters', default=[], action='append',
	help='Specify parameters')
parser.add_option('-o', '--output',          dest='output',     default="",
	help='Show only specified parameters')
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
	help='Add dataset splitting')
parser.add_option('-i', '--reinit',          dest='init',       default=False, action='store_true',
	help='Trigger re-init')
parser.add_option('-s', '--resync',          dest='resync',     default=False, action='store_true',
	help='Trigger re-sync')
parser.add_option('-S', '--save',            dest='save',
	help='Saves information to specified file')
(opts, args) = parseOptions(parser)

# we need exactly one positional argument (dataset path)
if len(args) != 1:
	utils.exitWithUsage(usage)

if os.path.exists(args[0]):
	config = Config(args[0])
else:
	utils.vprint('Provided options:')
	paramSettings = {'parameters': str.join(' ', args)}
	for p in opts.parameters:
		k, v = p.split('=', 1)
		paramSettings[k.strip()] = v.strip().replace('\\n', '\n')
		utils.vprint('\t%s: %s' % (k.strip(), v.strip()))
	utils.vprint('')
	config = Config(configDict={'jobs': {'nseeds': 1}, 'parameters': paramSettings})

config.opts = opts
config.workDir = '.'
config.getTaskDict = lambda: utils.PersistentDict(None)

pm = ParameterFactory.open(opts.manager, config, 'parameters')
utils.vprint('Registering dummy data provider data')
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

if opts.dataset:
	DataParameterSource.datasets[0] = opts.dataset
else:
	DataParameterSource.datasets[0] = {'obj': DummySplitter(), 'fun': lambda fl: fl}
if opts.forceiv:
	dp.intervention = (set([1]), set([0]), True)

plugin = pm.getSource(opts.init, opts.resync)

if opts.listparams:
	result = []
	if plugin.getMaxJobs():
		for jobNum in range(plugin.getMaxJobs()):
			info = plugin.getJobInfo(jobNum)
			if opts.inactive or info[ParameterInfo.ACTIVE]:
				result.append(info)
	else:
		result.append(plugin.getJobInfo(123))
	enabledOutput = opts.output.split(',')
	output = filter(lambda k: not opts.output or k in enabledOutput, plugin.getJobKeys())
	stored = filter(lambda k: k.untracked == False, output)
	untracked = filter(lambda k: k.untracked == True, output)
	head = [('PARAM_ID', 'PARAM_ID')]
	if opts.active:
		head.append((ParameterInfo.ACTIVE, 'ACTIVE'))
	head.extend(sorted(zip(stored, stored)))
	if opts.untracked:
		head.extend(sorted(map(lambda n: (n, '(%s)' % n), filter(lambda n: n not in ['PARAM_ID', 'MY_JOBID'], untracked))))
	print
	utils.printTabular(head, result)

if opts.save:
	print
	plugins.plugin_file.GCDumpParaPlugin.write(opts.save, plugin)
	print 'Parameter information saved to ./%s' % opts.save

if opts.intervention:
	print
	tmp = plugin.getIntervention()
	if tmp:
		print "   Redo:", tmp[0]
		print "Disable:", tmp[1]
	else:
		print "No intervention"
