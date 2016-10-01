#!/usr/bin/env python
# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

import os, sys, random, logging
from gcSupport import Options, getConfig, scriptOptions, utils
from grid_control.datasets import DataSplitter
from grid_control.parameters import ParameterAdapter, ParameterInfo, ParameterMetadata, ParameterSource
from python_compat import ifilter, imap, izip, lfilter, lmap, md5_hex, set, sorted


random.seed(0)

parser = Options(usage = '%s [OPTIONS] <parameter definition>')
parser.add_accu(None, 'c', 'collapse',           default = 0,     help = 'Do not collapse dataset infos in display')
parser.add_bool(None, 'a', 'active',             default = False, help = 'Show activity state')
parser.add_bool(None, 'd', 'disabled',           default = False, help = 'Show disabled parameter sets')
parser.add_bool(None, 'f', 'force-intervention', default = False, help = 'Simulate dataset intervention')
parser.add_bool(None, 'I', 'intervention',       default = False, help = 'Display intervention tasks')
parser.add_bool(None, 'l', 'list-parameters',    default = False, help = 'Display parameter list')
parser.add_bool(None, 'L', 'show-sources',       default = False, help = 'Show parameter sources')
parser.add_bool(None, 't', 'untracked',          default = False, help = 'Display untracked variables')
parser.add_bool(None, 'T', 'persistent',         default = False, help = 'Work with persistent paramters')
parser.add_list(None, 'p', 'parameter',          default = [],    help = 'Specify parameters')
parser.add_text(None, 'D', 'dataset',            default = '',    help = 'Add dataset splitting (use "True" to simulate a dataset)')
parser.add_text(None, 'j', 'job',                default = None,  help = 'Select job to display (used for unbounded parameter spaces)')
parser.add_text(None, 'F', 'factory',            default = None,  help = 'Select parameter source factory')
parser.add_text(None, 'o', 'output',             default = '',    help = 'Show only specified parameters')
parser.add_text(None, 'S', 'save',               default = '',    help = 'Saves information to specified file')
parser.add_text(None, 'V', 'visible',            default = '',    help = 'Set visible variables')
options = scriptOptions(parser)

if len(options.args) != 1:
	utils.exit_with_usage(parser.usage())

log = logging.getLogger()

# Create dataset parameter source
class DummySplitter:
	def get_partition_len(self):
		return 3

	def get_partition(self, pNum):
		mkEntry = lambda ds, fl, n, nick: { DataSplitter.Dataset: ds, DataSplitter.Nickname: nick,
			DataSplitter.FileList: fl, DataSplitter.NEntries: n }
		tmp = [ mkEntry('ds1', ['a', 'b'], 23, 'data_1'), mkEntry('ds1', ['1'], 42, 'data_1'),
			mkEntry('ds2', ['m', 'n'], 123, 'data_2'), mkEntry('ds2', ['x', 'y', 'z'], 987, 'data_3') ]
		return tmp[pNum]

class DataSplitProcessorTest:
	def get_partition_metadata(self):
		return lmap(lambda k: ParameterMetadata(k, untracked=True),
			['DATASETINFO', 'DATASETPATH', 'DATASETBLOCK', 'DATASETNICK'])

	def process(self, pNum, partition, result):
		result.update({
			'DATASETINFO': '',
			'DATASETPATH': partition.get(DataSplitter.Dataset, None),
			'DATASETBLOCK': partition.get(DataSplitter.BlockName, None),
			'DATASETNICK': partition.get(DataSplitter.Nickname, None),
			'DATASETSPLIT': pNum,
		})

repository = {}

def force_intervention():
	for dp in repository.values():
		dp.intervention = (set([1]), set([0]), True)

def process_intervention(opts, psource):
	log.info('')
	tmp = psource.getJobIntervention()
	if tmp:
		if opts.displaymode == 'parseable':
			log.info('R: %s', str.join(',', imap(str, tmp[0])))
			log.info('D: %s', str.join(',', imap(str, tmp[1])))
		else:
			log.info('   Redo: %s', repr(tmp[0]))
			log.info('Disable: %s', repr(tmp[1]))
	else:
		if opts.displaymode == 'parseable':
			log.info('NOINT')
		else:
			log.info('No intervention')

def save_parameters(psource, fn):
	log.info('')
	ParameterSource.get_class('GCDumpParameterSource').write(fn, psource)
	log.info('Parameter information saved to ./%s', fn)

def setup_config(opts, args):
	# Set config based on settings from config file or command line
	config_file = None
	if os.path.exists(args[0]):
		config_file = args[0]
	config = getConfig(config_file, section = 'global')
	if os.path.exists(config.get_work_path('datamap.tar')):
		opts.dataset = config.get_work_path('datamap.tar')
	config.change_view(setSections = ['jobs']).set('nseeds', '1', '?=')
	configParameters = config.change_view(setSections = ['parameters'])
	if opts.parameter:
		log.info('Provided options:')
		for p in opts.parameter:
			k, v = p.split('=', 1)
			configParameters.set(k.strip(), v.strip().replace('\\n', '\n'), '=')
			log.info('\t%s: %s', k.strip(), v.strip())
		log.info('')

	if config_file is None:
		configParameters.set('parameters', str.join(' ', args).replace('\\n', '\n'))
		if opts.dataset:
			configParameters.set('default lookup', 'DATASETNICK')
		if opts.verbose > 2:
			config.change_view(setSections = None).write(sys.stdout)
	return config

def setup_dataset(config, dataset):
	if dataset.lower() == 'true':
		log.info('Registering dummy data provider data')
		dataSplitter = DummySplitter()
	else:
		dataSplitter = DataSplitter.load_partitions_for_script(dataset)

	config = config.change_view(setSections = None)
	partProcessor = config.get_composited_plugin('partition processor',
		'TFCPartitionProcessor LocationPartitionProcessor MetaPartitionProcessor BasicPartitionProcessor',
		'MultiPartitionProcessor', cls = 'PartitionProcessor', on_change = None, pargs = ('dataset',))
	ParameterSource.create_instance('DataParameterSource', config.get_work_path(), 'data',
		None, dataSplitter, partProcessor, repository)

# Initialize ParameterFactory and ParameterSource
def get_psrc(opts, args):
	config = setup_config(opts, args)
	if opts.factory:
		config.set('parameter factory', opts.factory)
	pm = config.get_plugin('internal parameter factory', 'BasicParameterFactory', cls = 'ParameterFactory')
	if opts.dataset:
		setup_dataset(config, opts.dataset)
	adapter = 'BasicParameterAdapter'
	if opts.persistent:
		adapter = 'TrackedParameterAdapter'
	return ParameterAdapter.create_instance(adapter, config, pm.get_psrc(repository))

def get_parameters(opts, psource):
	result = []
	needGCParam = False
	if psource.get_job_len() is not None:
		countActive = 0
		for info in psource.iter_jobs():
			if info[ParameterInfo.ACTIVE]:
				countActive += 1
			if opts.disabled or info[ParameterInfo.ACTIVE]:
				if not info[ParameterInfo.ACTIVE]:
					info['GC_PARAM'] = 'N/A'
				if str(info['GC_PARAM']) != str(info['GC_JOB_ID']):
					needGCParam = True
				result.append(info)
		if opts.parseable:
			log.info('Count,%d,%d', countActive, psource.get_job_len())
		else:
			log.info('Number of parameter points: %d', psource.get_job_len())
			if countActive != psource.get_job_len():
				log.info('Number of active parameter points: %d', countActive)
	else:
		job = 123
		if opts.job is not None:
			job = int(opts.job)
		log.info('Unbounded parameter space found - showing parameters for job %d', job)
		result.append(psource.get_job_content(job))
	return (result, needGCParam)

def list_parameters(opts, psource):
	(result, needGCParam) = get_parameters(opts, psource)
	enabledOutput = opts.output.split(',')
	output = lfilter(lambda k: not opts.output or k in enabledOutput, psource.get_job_metadata())
	stored = lmap(lambda k: k.value, ifilter(lambda k: k.untracked == False, output))
	untracked = lmap(lambda k: k.value, ifilter(lambda k: k.untracked == True, output))

	if opts.collapse > 0:
		result_old = result
		result = {}
		result_nicks = {}
		head = [('COLLATE_JOBS', '# of jobs')]
		if 'DATASETSPLIT' in stored:
			stored.remove('DATASETSPLIT')
			if opts.collapse == 1:
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
			h = md5_hex(repr(lmap(lambda key: pset.get(str(key)), stored)))
			result.setdefault(h, []).append(pset)
			result_nicks.setdefault(h, set()).add(nickname)

		def doCollate(h):
			tmp = result[h][0]
			tmp['COLLATE_JOBS'] = len(result[h])
			tmp['COLLATE_NICK'] = len(result_nicks[h])
			return tmp
		result = lmap(doCollate, result)
	else:
		head = [('GC_JOB_ID', '#')]
		if needGCParam:
			head.append(('GC_PARAM', 'GC_PARAM'))
	if opts.active:
		head.append((ParameterInfo.ACTIVE, 'ACTIVE'))
	if opts.visible:
		stored = opts.visible.split(',')
	head.extend(sorted(izip(stored, stored)))
	if opts.untracked:
		head.extend(sorted(imap(lambda n: (n, '(%s)' % n), ifilter(lambda n: n not in ['GC_PARAM', 'GC_JOB_ID'], untracked))))
	utils.display_table(head, result)

def main(opts, args):
	psource = get_psrc(opts, args)

	if opts.show_sources:
		sys.stdout.write(str.join('\n', psource.show()) + '\n\n')
	if opts.list_parameters:
		list_parameters(opts, psource)
	if opts.force_intervention:
		force_intervention()
	if opts.intervention:
		process_intervention(opts, psource)
	if opts.save:
		save_parameters(psource, opts.save)

if __name__ == '__main__':
	sys.exit(main(options.opts, options.args))
