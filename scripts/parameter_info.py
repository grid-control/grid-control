#!/usr/bin/env python
# | Copyright 2017 Karlsruhe Institute of Technology
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
from gc_scripts import ConsoleTable, ScriptOptions, gc_create_config
from grid_control.datasets import DataSplitter, PartitionReader
from grid_control.parameters import ParameterAdapter, ParameterInfo, ParameterSource
from python_compat import StringBuffer, ifilter, imap, izip, lfilter, lmap, md5_hex, set, sorted


random.seed(0)


def collapse_psp_list(psp_list, tracked_list, opts):
	psp_dict = {}
	psp_dict_nicks = {}
	header_list = [('COLLATE_JOBS', '# of jobs')]
	if 'DATASETSPLIT' in tracked_list:
		tracked_list.remove('DATASETSPLIT')
		if opts.collapse == 1:
			tracked_list.append('DATASETNICK')
			header_list.append(('DATASETNICK', 'DATASETNICK'))
		elif opts.collapse == 2:
			header_list.append(('COLLATE_NICK', '# of nicks'))
	for pset in psp_list:
		if ('DATASETSPLIT' in pset) and (opts.collapse == 1):
			pset.pop('DATASETSPLIT')
		nickname = None
		if ('DATASETNICK' in pset) and (opts.collapse == 2):
			nickname = pset.pop('DATASETNICK')
		hash_str = md5_hex(repr(lmap(lambda key: pset.get(str(key)), tracked_list)))
		psp_dict.setdefault(hash_str, []).append(pset)
		psp_dict_nicks.setdefault(hash_str, set()).add(nickname)

	def _do_collate(hash_str):
		psp = psp_dict[hash_str][0]
		psp['COLLATE_JOBS'] = len(psp_dict[hash_str])
		psp['COLLATE_NICK'] = len(psp_dict_nicks[hash_str])
		return psp
	psp_list = sorted(imap(_do_collate, psp_dict), key=lambda x: tuple(imap(str, x.values())))
	return (header_list, psp_list)


def get_parameters(opts, psrc):
	psp_list = []
	need_gc_param = False
	if psrc.get_job_len() is not None:
		count_active = 0
		for info in psrc.iter_jobs():
			if info[ParameterInfo.ACTIVE]:
				count_active += 1
			if opts.disabled or info[ParameterInfo.ACTIVE]:
				if not info[ParameterInfo.ACTIVE]:
					info['GC_PARAM'] = 'N/A'
				if str(info['GC_PARAM']) != str(info['GC_JOB_ID']):
					need_gc_param = True
				psp_list.append(info)
		if opts.parseable:
			logging.info('Count,%d,%d', count_active, psrc.get_job_len())
		else:
			logging.info('Number of parameter points: %d', psrc.get_job_len())
			if count_active != psrc.get_job_len():
				logging.info('Number of active parameter points: %d', count_active)
	else:
		jobnum = 123
		if opts.job is not None:
			jobnum = int(opts.job)
		logging.info('Unbounded parameter space found - showing parameters for job %d', jobnum)
		psp_list.append(psrc.get_job_content(jobnum))
	return (psp_list, need_gc_param)


def get_psrc(opts, args):
	# Initialize ParameterFactory and ParameterSource
	repository = {}
	config = setup_config(opts, args)
	if opts.factory:
		config.set('parameter factory', opts.factory)
	pfactory = config.get_plugin('internal parameter factory', 'BasicParameterFactory',
		cls='ParameterFactory')
	if opts.dataset:
		setup_dataset(config, opts.dataset, repository)
	psrc = pfactory.get_psrc(repository)
	adapter = 'BasicParameterAdapter'
	if opts.persistent:
		adapter = 'TrackedParameterAdapter'
		config.set_state(True, 'resync', detail='parameters')
	return ParameterAdapter.create_instance(adapter, config, psrc)


def list_parameters(psrc, opts):
	(psp_list, need_gc_param) = get_parameters(opts, psrc)
	enabled_vn_list = opts.output.split(',')
	meta_list = lfilter(lambda k: (k in enabled_vn_list) or not opts.output, psrc.get_job_metadata())
	tracked_vn_list = lmap(lambda k: k.value, ifilter(lambda k: not k.untracked, meta_list))
	untracked_vn_list = lmap(lambda k: k.value, ifilter(lambda k: k.untracked, meta_list))

	if opts.collapse > 0:
		(header_list, psp_list) = collapse_psp_list(psp_list, tracked_vn_list, opts)
	else:
		header_list = [('GC_JOB_ID', '#')]
		if need_gc_param:
			header_list.append(('GC_PARAM', 'GC_PARAM'))
	if opts.active:
		header_list.append((ParameterInfo.ACTIVE, 'ACTIVE'))
	if opts.visible:
		tracked_vn_list = opts.visible.split(',')
	header_list.extend(sorted(izip(tracked_vn_list, tracked_vn_list)))
	if opts.untracked:
		header_list.extend(sorted(imap(lambda n: (n, '(%s)' % n),
			ifilter(lambda n: n not in ['GC_PARAM', 'GC_JOB_ID'], untracked_vn_list))))
	ConsoleTable.create(header_list, psp_list)


def save_parameters(psrc, fn):
	logging.info('')
	ParameterSource.get_class('GCDumpParameterSource').write(fn, psrc.get_job_len(),
		psrc.get_job_metadata(), psrc.iter_jobs())
	logging.info('Parameter information saved to ./%s', fn)


def setup_config(opts, args):
	# Set config based on settings from config file or command line
	config_fn = None
	if os.path.exists(args[0]):
		config_fn = args[0]
	config = gc_create_config(config_file=config_fn).change_view(set_sections=['global'])
	if os.path.exists(config.get_work_path('datamap.tar')):
		opts.dataset = config.get_work_path('datamap.tar')
	config.change_view(set_sections=['jobs']).set('nseeds', '1', '?=')
	param_config = config.change_view(set_sections=['parameters'])
	if opts.parameter:
		logging.info('Provided options:')
		for param in opts.parameter:
			key, value = param.split('=', 1)
			param_config.set(key.strip(), value.strip().replace('\\n', '\n'), '=')
			logging.info('\t%s: %s', key.strip(), value.strip())
		logging.info('')

	if config_fn is None:
		param_config.set('parameters', str.join(' ', args).replace('\\n', '\n'))
		if opts.dataset:
			param_config.set('default lookup', 'DATASETNICK')
		if opts.verbose > 2:
			buffer = StringBuffer()
			config.change_view(set_sections=None).write(buffer)
			logging.getLogger('script').info(buffer.getvalue().rstrip())
	return config


def setup_dataset(config, dataset, repository):
	if dataset.lower() == 'true':
		logging.info('Registering dummy data provider data')

		def _create_partition(ds_name, nick, n_events, fn_list):
			return {DataSplitter.Dataset: ds_name, DataSplitter.Nickname: nick,
				DataSplitter.FileList: fn_list, DataSplitter.NEntries: n_events}
		reader = PartitionReader.create_instance('TrivialPartitionReader', [
			_create_partition('ds1', 'data_1', 23, ['a', 'b']),
			_create_partition('ds1', 'data_1', 42, ['1']),
			_create_partition('ds2', 'data_2', 123, ['m', 'n']),
			_create_partition('ds2', 'data_3', 987, ['x', 'y', 'z'])
		])
	else:
		reader = DataSplitter.load_partitions(dataset)
	config = config.change_view(set_sections=None, default_on_change=None)
	ParameterSource.create_instance('BaseDataParameterSource', config, 'dataset', repository, reader)


def _main():
	options = _parse_cmd_line()
	psrc = get_psrc(options.opts, options.args)

	if options.opts.show_sources:
		logging.getLogger('script').info(str.join('\n', psrc.show()) + '\n')
	if options.opts.list_parameters:
		list_parameters(psrc, options.opts)
	if options.opts.save:
		save_parameters(psrc, options.opts.save)


def _parse_cmd_line():
	parser = ScriptOptions(usage='%s [OPTIONS] <parameter definition>')
	parser.add_accu(None, 'c', 'collapse', default=0,
		help='Do not collapse dataset infos in display')
	parser.add_bool(None, 'a', 'active', default=False,
		help='Show activity state')
	parser.add_bool(None, 'd', 'disabled', default=False,
		help='Show disabled parameter sets')
	parser.add_bool(None, 'l', 'list-parameters', default=False,
		help='Display parameter list')
	parser.add_bool(None, 'L', 'show-sources', default=False,
		help='Show parameter sources')
	parser.add_bool(None, 't', 'untracked', default=False,
		help='Display untracked variables')
	parser.add_bool(None, 'T', 'persistent', default=False,
		help='Work with persistent parameters')
	parser.add_list(None, 'p', 'parameter', default=[],
		help='Specify parameters')
	parser.add_text(None, 'D', 'dataset', default='',
		help='Add dataset splitting (use "True" to simulate a dataset)')
	parser.add_text(None, 'j', 'job', default=None,
		help='Select job to display (used for unbounded parameter spaces)')
	parser.add_text(None, 'F', 'factory', default=None,
		help='Select parameter source factory')
	parser.add_text(None, 'o', 'output', default='',
		help='Show only specified parameters')
	parser.add_text(None, 'S', 'save', default='',
		help='Saves information to specified file')
	parser.add_text(None, 'V', 'visible', default='',
		help='Set visible variables')
	options = parser.script_parse()
	if len(options.args) != 1:
		parser.exit_with_usage()
	return options


if __name__ == '__main__':
	sys.exit(_main())
