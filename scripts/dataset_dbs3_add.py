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

import os, sys, time, fcntl, pickle, logging
from gc_scripts import ScriptOptions, gc_create_config
from grid_control.config import create_config
from grid_control.datasets import DataProvider, DatasetError
from grid_control.gc_exceptions import UserError
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import SafeFile, with_file
from grid_control.utils.webservice import GridJSONRestClient
from grid_control_cms.access_cms import get_cms_cert
from grid_control_cms.dbs3_input_validation import validate_dbs3_json
from grid_control_cms.sitedb import SiteDB
from hpfwk import NestedException, clear_current_exception
from python_compat import imap, izip, json, lmap, md5_hex, partial, resolve_fun, set


class AlreadyQueued(NestedException):
	pass


class MigrationFailed(NestedException):
	pass


def create_dbs3_json_blocks(opts, dataset_blocks):
	dbs3_proto_block_iter = create_dbs3_proto_blocks(opts, dataset_blocks)
	for (block, block_dump, block_size, dataset_type) in dbs3_proto_block_iter:
		dataset = block[DataProvider.Dataset]
		try:
			primary_dataset, processed_dataset, data_tier = dataset[1:].split('/')
		except Exception:
			raise DatasetError('Dataset name %s is not a valid DBS name!' % dataset)

		# add primary dataset information
		block_dump['primds'] = {'primary_ds_type': dataset_type, 'primary_ds_name': primary_dataset}

		# add dataset information
		block_dump['dataset'] = {
			'dataset': dataset, 'processed_ds_name': processed_dataset, 'data_tier_name': data_tier,
			'physics_group_name': None, 'dataset_access_type': 'VALID',
			'xtcrosssection': None,  # TODO: Add to metadata from FrameWorkJobReport, if possible!
		}

		# add block information
		site_db = SiteDB()
		try:
			origin_site_name = site_db.se_to_cms_name(block[DataProvider.Locations][0])[0]
		except IndexError:
			clear_current_exception()
			origin_site_name = 'UNKNOWN'

		block_dump['block'] = {'block_name': DataProvider.get_block_id(block), 'block_size': block_size,
			'file_count': len(block[DataProvider.FileList]), 'origin_site_name': origin_site_name}
		if opts.do_close_blocks:
			block_dump['block']['open_for_writing'] = 0
		else:
			block_dump['block']['open_for_writing'] = 1

		# add acquisition_era, CRAB is important because of checks within DBS 3
		block_dump['acquisition_era'] = {'acquisition_era_name': 'CRAB', 'start_date': 0}
		# add processing_era
		block_dump['processing_era'] = {'processing_version': 1, 'description': 'grid-control'}

		yield validate_dbs3_json('blockBulk', block_dump)


def create_dbs3_json_files(opts, block_info, block_dump):
	block_size = 0
	dataset_type = set()
	for file_info in block_info[DataProvider.FileList]:
		metadata_info = dict(izip(block_info[DataProvider.Metadata], file_info[DataProvider.Metadata]))
		if metadata_info['CMSSW_DATATYPE']:  # this is not always correctly filled
			dataset_type.add(metadata_info['CMSSW_DATATYPE'])
		file_size = metadata_info['SE_OUTPUT_SIZE']
		lfn = file_info[DataProvider.URL]

		# add file information
		block_dump['files'].append({
			'logical_file_name': lfn, 'file_size': file_size,
			'check_sum': metadata_info['SE_OUTPUT_HASH_CRC32'],
			'md5': metadata_info['SE_OUTPUT_HASH_MD5'],
			'adler32': 'NOTSET',
			'file_lumi_list': lmap(lambda run_lumi:
				{'run_num': run_lumi[0], 'lumi_section_num': run_lumi[1]}, metadata_info['CMSSW_LUMIS']),
			'event_count': metadata_info['CMSSW_EVENTS_WRITE'],
			'file_type': 'EDM',
			'auto_cross_section': 0.0,
		})

		# add file parentage information
		if not opts.no_parents:
			block_dump['file_parent_list'].extend(imap(lambda parent_lfn:
				{'logical_file_name': lfn, 'parent_logical_file_name': parent_lfn},
				metadata_info['CMSSW_PARENT_LFN']))

		# fill file / dataset configurations
		dataset_conf_dict = {
			'release_version': metadata_info['CMSSW_VERSION'],
			'pset_hash': metadata_info['CMSSW_CONFIG_HASH'],
			'app_name': 'cmsRun',
			'output_module_label': 'crab2_mod_label',
			'global_tag': metadata_info.get('CMSSW_GLOBALTAG', opts.globaltag)
		}
		if opts.unique_cfg:
			dataset_conf_dict['pset_hash'] = md5_hex(dataset_conf_dict['pset_hash'] +
				block_info[DataProvider.Dataset])
		if dataset_conf_dict not in block_dump['dataset_conf_list']:
			block_dump['dataset_conf_list'].append(dataset_conf_dict)

		# file configurations also specifies lfn
		file_conf_dict = dict(dataset_conf_dict)
		file_conf_dict['lfn'] = lfn
		block_dump['file_conf_list'].append(file_conf_dict)

		# update block size for block summary information
		block_size += file_size
	return (block_size, dataset_type)


def create_dbs3_proto_blocks(opts, dataset_blocks):
	for dataset in dataset_blocks:
		missing_info_blocks = []
		dataset_types = set()
		for block in dataset_blocks[dataset]:
			block_dump = {'dataset_conf_list': [], 'files': [], 'file_conf_list': [], 'file_parent_list': []}
			(block_size, block_dataset_types) = create_dbs3_json_files(opts, block, block_dump)
			if len(block_dataset_types) > 1:
				raise Exception('Data and MC files are mixed in block %s' % DataProvider.get_block_id(block))
			elif len(block_dataset_types) == 1:
				yield (block, block_dump, block_size, block_dataset_types.pop())
			else:
				missing_info_blocks.append((block, block_dump, block_size))
			# collect dataset types in this dataset for blocks with missing type information
			dataset_types.update(block_dataset_types)

		if missing_info_blocks:
			if len(dataset_types) > 1:
				raise Exception('Data and MC files are mixed in dataset %s! ' +
					'Unable to determine dataset type for blocks without type info')
			elif len(dataset_types) == 0:
				if not opts.datatype:
					raise Exception('Please supply dataset type via --datatype!')
				dataset_type = opts.datatype
			else:
				dataset_type = dataset_types.pop()
			for (block, block_dump, block_size) in missing_info_blocks:
				yield (block, block_dump, block_size, dataset_type)


def discover_blocks(options):
	# Get work directory, create dbs dump directory
	if os.path.isdir(options.args[0]):
		work_dn = os.path.abspath(os.path.normpath(options.args[0]))
	else:
		work_dn = gc_create_config(config_file=options.args[0]).get_work_path()
	if not options.opts.tempdir:
		options.opts.tempdir = os.path.join(work_dn, 'dbs')
	if not os.path.exists(options.opts.tempdir):
		os.mkdir(options.opts.tempdir)

	# get provider with dataset information
	config = gc_create_config(config_dict={'dataset': options.config_dict}, load_old_config=False)
	if options.opts.input_file:
		provider = DataProvider.create_instance('ListProvider',
			config, 'dataset', options.opts.input_file)
	else:
		provider = DataProvider.create_instance('DBSInfoProvider',
			config, 'dataset', options.args[0])

	blocks = provider.get_block_list_cached(show_stats=False)
	DataProvider.save_to_file(os.path.join(options.opts.tempdir, 'dbs.dat'), blocks)
	if options.opts.discovery:
		sys.exit(os.EX_OK)
	return blocks


def do_migration(queue):
	while True:
		try:
			task = queue.popleft()
		except IndexError:
			clear_current_exception()
			# quit worker, means all tasks are done
			break
		try:
			task.run()
		except Exception:
			raise
		else:
			if not (task.is_done() or task.is_failed()):
				# re-queue task for further processing
				queue.append(task)
			else:
				# last execution of run, to print final migration done/failed message
				task.run()


def dump_dbs3_json(dn, block_dump_iter):
	for block_dump in block_dump_iter:
		block_dump_fn = block_dump['block']['block_name'].strip('/').replace('/', '_') + '.json'
		with_file(SafeFile(os.path.join(dn, block_dump_fn), 'w'), partial(json.dump, block_dump))


def filter_blocks(opts, blocks):
	return blocks


def process_dbs3_json_blocks(opts, block_dump_iter):
	log = logging.getLogger('dbs3-migration')
	log.setLevel(logging.DEBUG)

	# dry run without import - just store block dumps in temp dir
	if opts.do_import:
		return dump_dbs3_json(opts.tempdir, block_dump_iter)
	# set-up dbs clients
	dbs3_target_client = DBS3LiteClient(url=opts.target_instance)
	dbs3_source_client = DBS3LiteClient(url=opts.source_instance)
	dbs3_migration_queue = DBS3MigrationQueue()
	dbs3_migration_file = os.path.join(opts.tempdir, 'dbs3_migration.pkl')

	# migrate parents and register datasets with dbs3
	for block_dump in block_dump_iter:
		if not opts.continue_migration:
			# initiate the dbs3 to dbs3 migration of parent blocks
			log.debug('Checking parentage for block: %s', block_dump['block']['block_name'])
			unique_parent_lfns = set(imap(lambda parent: parent['parent_logical_file_name'],
				block_dump['file_parent_list']))
			unique_blocks = set()
			for parent_lfn in unique_parent_lfns:
				for block in dbs3_source_client.get_dbs_block_list(logical_file_name=parent_lfn):
					unique_blocks.add(block['block_name'])
			for parent_block in unique_blocks:
				if dbs3_target_client.get_dbs_block_list(block_name=parent_block):
					log.debug('Block %s is already at destination', parent_block)
					continue
				migration_task = MigrationTask(block_name=parent_block,
					migration_url=opts.dbsSource, dbs_client=dbs3_target_client)
				try:
					dbs3_migration_queue.add_migration_task(migration_task)
				except AlreadyQueued:
					log.exception('Already queued')
					clear_current_exception()
			dbs3_migration_queue.save_to_disk(dbs3_migration_file)
		else:
			try:
				dbs3_migration_queue = DBS3MigrationQueue.read_from_disk(dbs3_migration_file)
			except IOError:
				log.exception('Probably, there is no DBS 3 migration for this dataset ongoing')
				raise

		# wait for all parent blocks migrated to dbs3
		do_migration(dbs3_migration_queue)
		# insert block into dbs3
		dbs3_target_client.insert_dbs_block_dump(block_dump)


def sort_dataset_blocks(blocks):
	result = {}
	for block in blocks:
		result.setdefault(block[DataProvider.Dataset], []).append(block)
	return result


class DBS3LiteClient(object):
	def __init__(self, url):
		self._reader_url = '%s/%s' % (url, 'DBSReader')
		self._writer_url = '%s/%s' % (url, 'DBSWriter')
		self._migrate_url = '%s/%s' % (url, 'DBSMigrate')
		self._gjrc = GridJSONRestClient(get_cms_cert(create_config()),
			cert_error_msg='VOMS proxy needed to query DBS3!', cert_error_cls=UserError)

	def get_dbs_block_list(self, **kwargs):
		return self._gjrc.get(url=self._reader_url, api='blocks', params=kwargs)

	def get_dbs_file_list(self, **kwargs):
		return self._gjrc.get(url=self._reader_url, api='files', params=kwargs)

	def insert_dbs_block_dump(self, data):
		return self._gjrc.post(url=self._writer_url, api='bulkblocks', data=data)

	def migration_request_status(self, **kwargs):
		return self._gjrc.get(url=self._migrate_url, api='status', params=kwargs)

	def migration_request_submit(self, data):
		return self._gjrc.post(url=self._migrate_url, api='submit', data=data)


class FileMutex(object):
	def __init__(self, lockfile):
		self._lockfile = lockfile
		activity = Activity('Trying to aquire lock file %s ...' % lockfile)
		while os.path.exists(self._lockfile):
			time.sleep(0.2)
		activity.finish()
		self._fd = open(self._lockfile, 'w')
		fcntl.flock(self._fd, fcntl.LOCK_EX)

	def __del__(self):
		self.release()

	def release(self):
		if self._fd:
			fcntl.flock(self._fd, fcntl.LOCK_UN)
			self._fd.close()
			self._fd = None
		try:
			if os.path.exists(self._lockfile):
				os.unlink(self._lockfile)
		except Exception:
			clear_current_exception()


class MigrationDoneState(object):
	def __init__(self, migration_task):
		self.migration_task = migration_task

	def run(self):
		self.migration_task.logger.info("%s is done!" % self.migration_task)


class MigrationFailedState(object):
	def __init__(self, migration_task):
		self.migration_task = migration_task

	def run(self):
		self.migration_task.logger.error("%s failed! Please contact DBS admin!" % self.migration_task)
		raise MigrationFailed("%s failed! Please contact DBS admin!" % self.migration_task)


class MigrationRequestedState(object):
	def __init__(self, migration_task):
		self.migration_task = migration_task

	def run(self):
		# submit task to DBS 3 migration
		try:
			self.migration_task.migration_request = self.migration_task.dbs_client.migration_request_submit(
				self.migration_task.payload())
		except AttributeError:
			clear_current_exception()  # simulation
			self.migration_task.logger.info("%s has been queued for migration!" % self.migration_task)
		else:
			self.migration_task.logger.info("%s has been queued for migration!" % self.migration_task)

		self.migration_task.state = MigrationSubmittedState(self.migration_task)


class MigrationSubmittedState(object):
	def __init__(self, migration_task):
		self.migration_task = migration_task
		self.max_poll_interval = 10
		self.last_poll_time = time.time()

	def run(self):
		if abs(self.last_poll_time - time.time()) > self.max_poll_interval:
			# check migration status
			try:
				migration_details = self.migration_task.migration_request['migration_details']
				migration_request_id = migration_details['migration_request_id']
				request_status = self.migration_task.dbs_client.migration_request_status(
					migration_rqst_id=migration_request_id)
				self.migration_task.logger.debug("%s has migration_status=%s" % (
					self.migration_task, request_status[0]['migration_status']))
				self.last_poll_time = time.time()
			except AttributeError:
				clear_current_exception()  # simulation
				logging.warning("Simulation")
				request_status = [{'migration_status': 2}]
				self.migration_task.logger.debug("%s has migration_status=%s" % (
					self.migration_task, request_status[0]['migration_status']))
				self.last_poll_time = time.time()

			if request_status[0]['migration_status'] == 2:
				# migration okay
				self.migration_task.state.__class__ = MigrationDoneState
			elif request_status[0]['migration_status'] == 9:
				# migration failed
				self.migration_task.state.__class__ = MigrationFailedState


class MigrationTask(object):
	def __init__(self, block_name, migration_url, dbs_client):
		self.block_name = block_name
		self.migration_url = migration_url
		self.dbs_client = dbs_client
		self.logger = logging.getLogger('dbs3-migration')
		self.state = MigrationRequestedState(self)

	def __eq__(self, other):
		return (self.block_name == other.block_name) and (self.migration_url == other.migration_url)

	def __getstate__(self):
		"""
		Logger object cannot be pickled
		"""
		state = dict(self.__dict__)
		del state['logger']
		return state

	def __hash__(self):
		return hash(self.block_name + self.migration_url)

	def __ne__(self, other):
		return (self.block_name != other.block_name) or (self.migration_url != other.migration_url)

	def __repr__(self):
		return '%s(block_name="%s", migration_url="%s")' % (
			self.__class__.__name__, self.block_name, self.migration_url)

	def __setstate__(self, state):
		"""
		Logger object cannot be pickled, restore logger attribute
		"""
		state['logger'] = logging.getLogger('dbs3-migration')
		self.__dict__.update(state)
		return True

	def __str__(self):
		return repr(self)

	def is_done(self):
		return self.state.__class__ == MigrationDoneState

	def is_failed(self):
		return self.state.__class__ == MigrationFailedState

	def payload(self):
		return {
			'migration_url': self.migration_url,
			'migration_input': self.block_name
		}

	def run(self):
		self.state.run()


class DBS3MigrationQueue(resolve_fun('collections:deque', '<builtin>:list')):
	_unique_queued_tasks = set()

	def __init__(self, tasks=None, maxlen=None):
		super(DBS3MigrationQueue, self).__init__(iterable=tasks or [], maxlen=maxlen)

	def add_migration_task(self, migration_task):
		if migration_task not in self._unique_queued_tasks:
			self._unique_queued_tasks.add(migration_task)
			self.append(migration_task)
		else:
			raise AlreadyQueued('%s is already queued!' % migration_task)

	def read_from_disk(filename):
		return with_file(SafeFile(filename), pickle.load)
	read_from_disk = staticmethod(read_from_disk)

	def save_to_disk(self, filename):
		with_file(SafeFile(filename, 'w'), partial(pickle.dump, self))


def _main():
	# Handle command line options
	options = _parse_cmd_line()
	config_dict = options.config_dict
	config_dict['include parent infos'] = True
	config_dict['dataset hash keys'] = config_dict['dataset hash keys'].replace(',', ' ')
	if options.opts.jobhash:
		config_dict['dataset hash keys'] += ' CMSSW_CONFIG_JOBHASH'
	if options.opts.discovery:
		config_dict['dataset name pattern'] = '@DS_KEY@'
	if len(options.args) != 1:
		options.parser.exit_with_usage(options.parser.usage(),
			'Neither work directory nor config file specified!')
	# Lock file in case several instances of this program are running
	mutex = FileMutex(os.path.join(options.opts.tempdir, 'datasetDBSAdd.lock'))
	try:
		# 1) Get dataset information
		blocks = discover_blocks(options)
		# 2) Filter datasets
		blocks = filter_blocks(options.opts, blocks)
		# 3) Process datasets (migrate parents and register
		dbs3_json_block_iter = create_dbs3_json_blocks(options.opts, sort_dataset_blocks(blocks))
		process_dbs3_json_blocks(options.opts, dbs3_json_block_iter)
	finally:
		mutex.release()


def _parse_cmd_line():
	parser = ScriptOptions(usage='%s [OPTIONS] <config file / work directory>')
	parser.section('disc', 'Discovery options - ignored in case dbs input file is specified')
	# options that are used as config settings for InfoScanners
	parser.add_text('disc', 'n', 'dataset-name-pattern', default='',
		help='Specify dbs path name - Example: DataSet_@NICK@_@VAR@')
	parser.add_text('disc', 'H', 'dataset-hash-keys', default='',
		help='Included additional variables in dataset hash calculation')
	parser.add_text('disc', 'J', 'source-job-selector', default='',
		help='Specify dataset(s) to process')
	parser.add_bool('disc', 'p', 'no-parents', default=False,
		help='Do not add parent infromation to DBS')
	parser.add_bool('disc', 'm', 'merge-parents', default=False,
		help='Merge output files from different parent blocks ' +
			'into a single block [Default: Keep boundaries]')
	parser.add_text('disc', 'P', 'parent-source', default='',
		help='Override parent information source - to bootstrap a reprocessing on local files')
	# options directly used by this script
	parser.add_text('disc', 'T', 'datatype', default=None,
		help='Supply dataset type in case cmssw report did not specify it - valid values: "mc" or "data"')
	parser.add_bool('disc', 'j', 'jobhash', default=False,
		help='Use hash of all config files in job for dataset key calculation')
	parser.add_bool('disc', 'u', 'unique-cfg', default=False,
		help='Circumvent edmConfigHash collisions so each dataset ' +
			'is stored with unique config information')
	parser.add_text('disc', 'G', 'globaltag', default='crab2_tag',
		help='Specify global tag')

	parser.section('proc', 'Processing mode')
	parser.add_bool('proc', 'd', 'discovery', default=False,
		help='Enable discovery mode - just collect file information and exit')
	parser.add_text('proc', ' ', 'tempdir', default='',
		help='Override temp directory')
	parser.add_bool('proc', 'i', 'no-import', default=True, dest='do_import',
		help='Disable import of new datasets into target DBS instance - ' +
			'only temporary json files are created')
	parser.add_bool('proc', 'I', 'incremental', default=False,
		help='Skip import of existing files - Warning: this destroys coherent block structure!')
	parser.add_bool('proc', 'o', 'open-blocks', default=True, dest='do_close_blocks',
		help='Keep blocks open for addition of further files [Default: Close blocks]')

	parser.section('dbsi', 'DBS instance handling')
	parser.add_text('dbsi', 't', 'target-instance', default='https://cmsweb.cern.ch/dbs/prod/phys03',
		help='Specify target dbs instance url')
	parser.add_text('dbsi', 's', 'source-instance', default='https://cmsweb.cern.ch/dbs/prod/global',
		help='Specify source dbs instance url(s), where parent datasets are taken from')

	parser.add_text(None, 'F', 'input-file', default=None,
		help='Specify dbs input file to use instead of scanning job output')
	parser.add_bool(None, 'c', 'continue-migration', default=False,
		help='Continue an already started migration')

	return parser.script_parse()


def _run_test():
	from grid_control.logging_setup import logging_defaults
	logging_defaults()
	logger = logging.getLogger('dbs3-migration')

	block_names = ['test1', 'test1', 'test2', 'test3', 'test4']
	migration_queue = DBS3MigrationQueue()

	for block in block_names:
		try:
			migration_queue.add_migration_task(MigrationTask(block_name=block,
				migration_url='http://a.b.c', dbs_client=None))
		except AlreadyQueued:
			logger.exception('Already queued')
			clear_current_exception()

	migration_queue.save_to_disk('test.pkl')
	del migration_queue
	new_migration_queue = DBS3MigrationQueue.read_from_disk('test.pkl')
	do_migration(new_migration_queue)


if __name__ == '__main__':
	sys.exit(_main())
