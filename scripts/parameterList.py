#!/usr/bin/env python
import random
from gcSupport import *
from grid_control.datasets import DataSplitter
import grid_control.plugins
random.seed(0)

usage = '%s [OPTIONS] <parameter definition>' % sys.argv[0]
parser = optparse.OptionParser(usage=usage)
parser.add_option('-l', '--list-parameters', dest='listparams', default=False, action='store_true',
	help='')
parser.add_option('-M', '--manager',         dest='manager',    default='EasyPluginManager',
	help='Select plugin manager')
parser.add_option('-p', '--parameter',       dest='parameters', default=[], action='append',
	help='Specify parameters')
parser.add_option('-a', '--active',          dest='active',     default=False, action='store_true',
	help='Show activity state')
parser.add_option('-t', '--transient',       dest='transient',  default=False, action='store_true',
	help='Display transient variables')
parser.add_option('-I', '--intervention',    dest='intervention', default=False, action='store_true',
	help='Display intervention tasks')
parser.add_option('-f', '--force-intervention', dest='forceiv', default=False, action='store_true',
	help='Simulate dataset intervention')
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

cfgSettings = {
	'parameters': str.join(' ', args),
}

utils.vprint('Provided options:')
paramSettings = {}
for p in opts.parameters:
	k, v = p.split('=', 1)
	paramSettings[k.strip()] = v.strip().replace('\\n', '\n')
	utils.vprint('\t%s: %s' % (k.strip(), v.strip()))
utils.vprint('')
config = Config(configDict={'jobs': {'nseeds': 1}, 'dummy': cfgSettings, 'parameters': paramSettings})
config.getTaskDict = lambda: utils.PersistentDict(None)
config.opts = opts
config.workDir = '.'
pm = grid_control.plugins.PluginManager.open(opts.manager, config, 'dummy')
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
dp = grid_control.plugins.DataParaPlugin(DummySplitter(), lambda x: str.join(' ', x))
if opts.forceiv:
	dp.intervention = (set([1]), set([0]), True)
grid_control.plugins.ParameterPlugin.rawManagerMap['data'] = lambda: dp
plugin = pm.getSource(opts.init, opts.resync)

if opts.listparams:
	result = []
	if plugin.getMaxJobs():
		for jobNum in range(plugin.getMaxJobs()):
			result.append(plugin.getJobInfo(jobNum))
	else:
		result.append(plugin.getJobInfo(123))
	stored = filter(lambda k: k.transient == False, plugin.getParameterNamesSet())
	transient = filter(lambda k: k.transient == True, plugin.getParameterNamesSet())
	head = [('PARAM_ID', 'PARAM_ID')]
	if opts.active:
		head.append((grid_control.plugins.plugin_base.ParameterInfo.ACTIVE, 'ACTIVE'))
	head.extend(sorted(zip(stored, stored)))
	if opts.transient:
		head.extend(sorted(map(lambda n: (n, '(%s)' % n), filter(lambda n: n not in ['PARAM_ID', 'MY_JOBID'], transient))))
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
