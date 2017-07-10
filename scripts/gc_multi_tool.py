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

import sys, gzip, base64, logging
from gc_scripts import ConsoleTable, Job, JobSelector, Plugin, ScriptOptions, gc_create_config, get_script_object_cmdline  # pylint:disable=line-too-long
from grid_control.backends import WMS
from grid_control.datasets import DataProvider, DataSplitter
from grid_control.utils.file_tools import GZipTextFile, SafeFile, with_file
from python_compat import BytesBuffer, imap, lmap, lzip, set, sorted


def backend_list(finder_name):
	finder = Plugin.get_class('BackendDiscovery').create_instance(finder_name, gc_create_config())
	item_dict_list = []
	item_key_set = set()
	for item_dict in finder.discover():
		nice_item_dict = {}
		for (key, value) in item_dict.items():
			if isinstance(key, int):
				key = WMS.enum2str(key)
			nice_item_dict[key] = value
			item_key_set.add(key)
		item_dict_list.append(nice_item_dict)
	item_key_set.remove('name')
	item_key_list = sorted(item_key_set)
	ConsoleTable.create([('name', 'Name')] + lzip(item_key_list, item_key_list), item_dict_list)


def dataset_iter_blocks(block_list):
	for block in block_list:
		block[DataProvider.NFiles] = len(block.get(DataProvider.FileList, []))
		yield block


def dataset_show_diff(options):
	if len(options.args) != 2:
		options.parser.exit_with_usage(options.parser.usage('data'))

	provider_a = DataProvider.load_from_file(options.args[0])
	provider_b = DataProvider.load_from_file(options.args[1])
	block_resync_tuple = DataProvider.resync_blocks(provider_a.get_block_list_cached(show_stats=False),
		provider_b.get_block_list_cached(show_stats=False))
	(block_list_added, block_list_missing, block_list_matching) = block_resync_tuple

	def _dataset_iter_matching_blocks():
		for (block_old, block_new, _, _) in block_list_matching:
			def _format_change(old, new):
				if old != new:
					return '%s -> %s' % (old, new)
				return old
			block_old[DataProvider.NFiles] = _format_change(len(block_old.get(DataProvider.FileList, [])),
				len(block_new.get(DataProvider.FileList, [])))
			block_old[DataProvider.NEntries] = _format_change(block_old[DataProvider.NEntries],
				block_new[DataProvider.NEntries])
			yield block_old

	header_list = [(DataProvider.Dataset, 'Dataset'), (DataProvider.BlockName, 'Block'),
		(DataProvider.NFiles, '#Files'), (DataProvider.NEntries, '#Entries')]
	if block_list_added:
		ConsoleTable.create(header_list, dataset_iter_blocks(block_list_added), title='Added blocks')
	if block_list_missing:
		ConsoleTable.create(header_list, dataset_iter_blocks(block_list_missing), title='Removed blocks')
	if block_list_matching:
		ConsoleTable.create(header_list, _dataset_iter_matching_blocks(), title='Matching blocks')


def dataset_show_removed(options):
	if len(options.args) < 2:
		options.parser.exit_with_usage(options.parser.usage('data'))

	block_list_missing = []
	provider_old = DataProvider.load_from_file(options.args[0])
	for dataset_fn in options.args[1:]:
		provider_new = DataProvider.load_from_file(dataset_fn)
		block_resync_tuple = DataProvider.resync_blocks(
			provider_old.get_block_list_cached(show_stats=False),
			provider_new.get_block_list_cached(show_stats=False))
		for block in block_resync_tuple[1]:  # iterate missing block list
			tmp = dict(block)
			tmp[DataProvider.RemovedIn] = dataset_fn
			block_list_missing.append(tmp)
		provider_old = provider_new
	if block_list_missing:
		ConsoleTable.create([(DataProvider.Dataset, 'Dataset'), (DataProvider.BlockName, 'Block'),
			(DataProvider.NFiles, '#Files'), (DataProvider.NEntries, '#Entries'),
			(DataProvider.RemovedIn, 'Removed in file')],
			dataset_iter_blocks(block_list_missing), title='Removed blocks')


def get_job_db(options):
	if len(options.args) != 1:
		options.parser.exit_with_usage(options.parser.usage('jobs'))
	config = gc_create_config(config_file=options.args[0])
	# Initialise task module
	task_cls_name = config.get(['task', 'module'])
	task = Plugin.create_instance(task_cls_name, config, task_cls_name)
	job_db = Plugin.create_instance('TextFileJobDB', config)
	selected = JobSelector.create(options.opts.job_selector, task=task)
	logging.info('Matching jobs: ' + str.join(' ', imap(str, job_db.iter_jobs(selected))))


def get_partition_reader(options):
	if len(options.args) != 1:
		options.parser.exit_with_usage(options.parser.usage('part'))
	return DataSplitter.load_partitions(options.args[0])


def jobs_force_state(opts, job_db):
	new_job_state = Job.str2enum(opts.job_force_state)
	new_job_state_str = Job.enum2str(new_job_state)
	for jobnum in job_db.iter_jobs():
		job_obj = job_db.get_job_persistent(jobnum)
		old_job_state = job_obj.state
		if old_job_state == new_job_state:
			logging.info('Job is already in state %s', new_job_state_str)
			continue
		job_obj.state = new_job_state
		job_db.commit(jobnum, job_obj)
		logging.info('Job state changed from %s to %s', Job.enum2str(old_job_state), new_job_state_str)


def jobs_reset_attempts(job_db):
	for jobnum in job_db.iter_jobs():
		logging.info('Resetting attempts for job %d', jobnum)
		job_obj = job_db.get_job_persistent(jobnum)
		job_obj.attempt = 0
		job_obj.history = {}
		for key in job_obj.dict.keys():
			if key.startswith('history'):
				job_obj.dict.pop(key)
		job_db.commit(jobnum, job_obj)


def jobs_show_jdl(job_db):
	for jobnum in job_db.iter_jobs():
		job_obj = job_db.get_job_transient(jobnum)
		if job_obj.get('jdl'):
			logging.info(job_obj.get('jdl'))
		else:
			logging.info('Job %d: No jdl information available!')


def logfile_decode(fn):
	def _decode_stream(fp):
		for line in fp.readlines():
			if line.startswith('(B64) '):
				buffer_obj = BytesBuffer(base64.b64decode(line.replace('(B64) ', '')))
				line = gzip.GzipFile(fileobj=buffer_obj).read().decode('ascii')
			logging.getLogger('script').info(line.rstrip())
	if fn.endswith('.gz'):
		with_file(GZipTextFile(fn, 'r'), _decode_stream)
	else:
		with_file(SafeFile(fn), _decode_stream)


def partition_display(opts, partition_iter):
	def _iter_partitions():
		for partition_num, partition in enumerate(partition_iter):
			partition['partition_num'] = partition_num
			yield partition

	header_list = lmap(lambda key: (key, DataSplitter.enum2str(key)), opts.partition_key_list)
	ConsoleTable.create([('partition_num', 'Partition')] + header_list, _iter_partitions())


def partition_iter_invalid(reader):
	for partition in reader.iter_partitions():
		if partition.get(DataSplitter.Invalid, False):
			yield partition


def _main():
	# Add some enums for consistent access to info dicts
	DataProvider.NFiles = -1
	DataProvider.RemovedIn = -2

	options = _parse_cmd_line()
	opts = options.opts

	if opts.backend_list:
		backend_list(opts.backend_list)

	if opts.partition_list:
		partition_display(options.opts, get_partition_reader(options).iter_partitions())
	if opts.partition_list_invalid:
		partition_display(options.opts, partition_iter_invalid(get_partition_reader(options)))

	if opts.job_reset_attempts or opts.job_force_state or opts.job_show_jdl:
		job_db = get_script_object_cmdline(options.args)
		if opts.job_reset_attempts:
			jobs_reset_attempts(job_db)
		if opts.job_force_state:
			jobs_force_state(options.opts, job_db)
		if opts.job_show_jdl:
			jobs_show_jdl(job_db)

	if opts.dataset_show_diff:
		dataset_show_diff(options)
	if opts.dataset_show_removed:
		dataset_show_removed(options)

	if opts.logfile_decode:
		logfile_decode(opts.logfile_decode)


def _parse_cmd_line():
	parser = ScriptOptions()
	parser.section('back', 'Backend debugging', '%s ...')
	parser.add_text('back', '', 'backend-list', default='',
		help='Specify backend discovery plugin')

	parser.section('part', 'Dataset Partition debugging', '%s <path to partition file> ...')
	parser.add_bool('part', '', 'partition-list', default=False,
		help='List all dataset partitions')
	parser.add_bool('part', '', 'partition-list-invalid', default=False,
		help='List invalidated dataset partitions')
	parser.add_text('part', '', 'partition-key-list', default='',
		help='Select dataset partition information to display')

	parser.section('jobs', 'Jobs debugging', '%s <config file / job file> ... ')
	parser.add_text('jobs', '', 'job-selector', default='',
		help='Display jobs matching selector')
	parser.add_bool('jobs', '', 'job-reset-attempts', default=False,
		help='Reset the attempt counter')
	parser.add_text('jobs', '', 'job-force-state', default='',
		help='Force new job state')
	parser.add_text('jobs', '', 'job-show-jdl', default='',
		help='Show JDL file if available')

	parser.section('data', 'Dataset debugging', '%s <dataset file> <dataset file> ...')
	parser.add_bool('data', '', 'dataset-show-diff', default=False,
		help='Show difference between datasets')
	parser.add_bool('data', '', 'dataset-show-removed', default=False,
		help='Find removed dataset blocks')

	parser.add_text(None, 'd', 'logfile-decode', default='',
		help='Decode log files')
	options = parser.script_parse()

	# Parse partition key list
	if options.opts.partition_key_list in ('', 'all'):
		partition_key_list = DataSplitter.enum_value_list
	else:
		partition_key_list = lmap(DataSplitter.str2enum, options.opts.partition_key_list.split(','))
	if None in partition_key_list:
		logging.warning('Available keys: %r', DataSplitter.enum_name_list)
	options.opts.partition_key_list = partition_key_list

	return options


if __name__ == '__main__':
	sys.exit(_main())
