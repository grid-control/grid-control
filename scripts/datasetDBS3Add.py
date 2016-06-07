#!/usr/bin/env python
# | Copyright 2014-2016 Karlsruhe Institute of Technology
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
from gcSupport import FileMutex, Options, getConfig, scriptOptions, utils
from grid_control.datasets import DataProvider, DatasetError
from grid_control_cms.dbs3_input_validation import validate_dbs3_json
from grid_control_cms.dbs3_lite_client import DBS3LiteClient
from grid_control_cms.dbs3_migration_queue import AlreadyQueued, DBS3MigrationQueue, MigrationTask, do_migration
from grid_control_cms.sitedb import SiteDB
from python_compat import imap, izip, json, lmap, md5_hex, set

def create_dbs3_json_files(opts, block_info, block_dump):
	block_size = 0
	dataset_type = set()
	for file_info in block_info[DataProvider.FileList]:
		metadata_info = dict(izip(block_info[DataProvider.Metadata], file_info[DataProvider.Metadata]))
		if metadata_info['CMSSW_DATATYPE']: # this is not always correctly filled
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
		block_dump['file_parent_list'].extend(imap(lambda parent_lfn:
			{'logical_file_name': lfn, 'parent_logical_file_name': parent_lfn}, metadata_info['CMSSW_PARENT_LFN']))

		# fill file / dataset configurations
		dataset_conf_dict = {'release_version': metadata_info['CMSSW_VERSION'],
			'pset_hash': metadata_info['CMSSW_CONFIG_HASH'],
			'app_name': 'cmsRun',
			'output_module_label': 'crab2_mod_label',
			'global_tag': metadata_info['CMSSW_GLOBALTAG']#,#default=opts.globaltag)
		}
		if opts.unique_cfg:
			dataset_conf_dict['pset_hash'] = md5_hex(dataset_conf_dict['pset_hash'] + block_info[DataProvider.Dataset])
		if dataset_conf_dict not in block_dump['dataset_conf_list']:
			block_dump['dataset_conf_list'].append(dataset_conf_dict)

		# file configurations also specifies lfn
		file_conf_dict = dict(dataset_conf_dict)
		file_conf_dict['lfn'] = lfn
		block_dump['file_conf_list'].append(file_conf_dict)

		# update block size for block summary information
		block_size += file_size
	return (block_size, dataset_type)


def sort_dataset_blocks(blocks):
	result = {}
	for block in blocks:
		result.setdefault(block[DataProvider.Dataset], []).append(block)
	return result


def create_dbs3_proto_blocks(opts, dataset_blocks):
	for dataset in dataset_blocks:
		missing_info_blocks = []
		dataset_types = set()
		for block in dataset_blocks[dataset]:
			block_dump = {'dataset_conf_list': [], 'files': [], 'file_conf_list': [], 'file_parent_list': []}
			(block_size, block_dataset_types) = create_dbs3_json_files(opts, block, block_dump)
			if len(block_dataset_types) > 1:
				raise Exception('Data and MC files are mixed in block %s#%s' % (dataset, block[DataProvider.BlockName]))
			elif len(block_dataset_types) == 1:
				yield (block, block_dump, block_size, block_dataset_types.pop())
			else:
				missing_info_blocks.append((block, block_dump, block_size))
			dataset_types.update(block_dataset_types) # collect dataset types in this dataset for blocks with missing type information

		if missing_info_blocks:
			if len(dataset_types) > 1:
				raise Exception('Data and MC files are mixed in dataset %s! Unable to determine dataset type for blocks without type info')
			elif len(dataset_types) == 0:
				if not opts.datatype:
					raise Exception('Please supply dataset type via --datatype!')
				dataset_type = opts.datatype
			else:
				dataset_type = dataset_types.pop()
			for (block, block_dump, block_size) in missing_info_blocks:
				yield (block, block_dump, block_size, dataset_type)


def create_dbs3_json_blocks(opts, dataset_blocks):
	for (block, block_dump, block_size, dataset_type) in create_dbs3_proto_blocks(opts, dataset_blocks):
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
			'xtcrosssection': None, # TODO: Add to meta data from FrameWorkJobReport, if possible!
		}

		# add block information
		site_db = SiteDB()
		try:
			origin_site_name = site_db.se_to_cms_name(block[DataProvider.Locations][0])[0]
		except IndexError:
			origin_site_name = 'UNKNOWN'

		block_dump['block'] = {'block_name': '%s#%s' % (dataset, block[DataProvider.BlockName]),
			'file_count': len(block[DataProvider.FileList]),
			'block_size': block_size, 'origin_site_name': origin_site_name}
		if opts.do_close_blocks:
			block_dump['block']['open_for_writing'] = 0
		else:
			block_dump['block']['open_for_writing'] = 1

		# add acquisition_era, CRAB is important because of checks within DBS 3
		block_dump['acquisition_era'] = {'acquisition_era_name': 'CRAB', 'start_date': 0}
		# add processing_era
		block_dump['processing_era'] = {'processing_version': 1, 'description': 'grid-control'}

		yield validate_dbs3_json('blockBulk', block_dump)


def setup_parser():
	parser = Options(usage = '%s [OPTIONS] <config file / work directory>')
	parser.section('disc', 'Discovery options - ignored in case dbs input file is specified')
	# options that are used as config settings for InfoScanners
	parser.addText('disc', 'n', 'dataset-name-pattern', default = '',
		help = 'Specify dbs path name - Example: DataSet_@NICK@_@VAR@')
	parser.addText('disc', 'H', 'dataset-hash-keys',    default = '',
		help = 'Included additional variables in dataset hash calculation')
	parser.addText('disc', 'J', 'source-job-selector',  default = '',
		help = 'Specify dataset(s) to process')
	parser.addBool('disc', 'm', 'merge-parents',        default = False,
		help = 'Merge output files from different parent blocks into a single block [Default: Keep boundaries]')
	parser.addText('disc', 'P', 'parent-source',        default = '',
		help = 'Override parent information source - to bootstrap a reprocessing on local files')
	# options directly used by this script
	parser.addText('disc', 'T', 'datatype',             default = None,
		help = 'Supply dataset type in case cmssw report did not specify it - valid values: "mc" or "data"')
	parser.addBool('disc', 'j', 'jobhash',              default = False,
		help = 'Use hash of all config files in job for dataset key calculation')
	parser.addBool('disc', 'u', 'unique-cfg',           default = False,
		help = 'Circumvent edmConfigHash collisions so each dataset is stored with unique config information')
	parser.addText('disc', 'G', 'globaltag',          default = 'crab2_tag',
		help = 'Specify global tag')

	parser.section('proc', 'Processing mode')
	parser.addBool('proc', 'd', 'discovery',            default = False,
		help = 'Enable discovery mode - just collect file information and exit')
	parser.addText('proc', ' ', 'tempdir',              default = '',
		help = 'Override temp directory')
	parser.addBool('proc', 'i', 'no-import',            default = True, dest = 'do_import',
		help = 'Disable import of new datasets into target DBS instance - only temporary json files are created')
	parser.addBool('proc', 'I', 'incremental',          default = False,
		help = 'Skip import of existing files - Warning: this destroys coherent block structure!')
	parser.addBool('proc', 'o', 'open-blocks',          default = True, dest = 'do_close_blocks',
		help = 'Keep blocks open for addition of further files [Default: Close blocks]')
#	parser.addBool('proc', 'b', 'batch',                default = False,
#		help = 'Enable non-interactive batch mode [Default: Interactive mode]')

	parser.section('dbsi', 'DBS instance handling')
	parser.addText('dbsi', 't', 'target-instance',      default = 'https://cmsweb.cern.ch/dbs/prod/phys03',
		help = 'Specify target dbs instance url')
	parser.addText('dbsi', 's', 'source-instance',      default = 'https://cmsweb.cern.ch/dbs/prod/global',
		help = 'Specify source dbs instance url(s), where parent datasets are taken from')

	parser.addText(None, 'F', 'input-file',         default = None,
		help = 'Specify dbs input file to use instead of scanning job output')
	parser.addBool(None, 'c', 'continue-migration', default = False,
		help = 'Continue an already started migration')
#	parser.addText(None, 'D', 'display-dataset',    default = None,
#		help = 'Display information associated with dataset key(s) (accepts "all")')
#	parser.addText(None, 'C', 'display-config',     default = None,
#		help = 'Display information associated with config hash(es) (accepts "all")')
#	parser.addText(None, 'k', 'dataset-key-select', default = '',
#		help = 'Specify dataset keys to process')

	return scriptOptions(parser)


def discover_blocks(options):
	# Get work directory, create dbs dump directory
	if os.path.isdir(options.args[0]):
		workDir = os.path.abspath(os.path.normpath(options.args[0]))
	else:
		workDir = getConfig(configFile = options.args[0]).getWorkPath()
	if not options.opts.tempdir:
		options.opts.tempdir = os.path.join(workDir, 'dbs')
	if not os.path.exists(options.opts.tempdir):
		os.mkdir(options.opts.tempdir)

	# get provider with dataset information
	if options.opts.input_file:
		provider = DataProvider.createInstance('ListProvider', getConfig(), options.opts.input_file, None)
	else:
		config = getConfig(configDict = {'dataset': options.config_dict})
		provider = DataProvider.createInstance('DBSInfoProvider', config, options.args[0], None)

	blocks = provider.getBlocks()
	DataProvider.saveToFile(os.path.join(options.opts.tempdir, 'dbs.dat'), blocks)
	if options.opts.discovery:
		sys.exit(os.EX_OK)
	return blocks


def filter_blocks(opts, blocks):
	return blocks
#	if opts.incremental:
		# Query target DBS for all found datasets and perform dataset resync with "supposed" state
#		dNames = set(ximap(lambda b: b[DataProvider.Dataset], blocks))
#		dNames = xfilter(lambda ds: hasDataset(opts.dbsTarget, ds), dNames) - todo
#		config = getConfig(configDict = {None: {'dbs instance': opts.dbsTarget}})
#		oldBlocks = xreduce(xoperator.add, ximap(lambda ds: DBSApiv2(config, None, ds, None).getBlocks(), dNames), [])
#		(blocksAdded, blocksMissing, blocksChanged) = DataProvider.resyncSources(oldBlocks, blocks)
#		if len(blocksMissing) or len(blocksChanged):
#			if not utils.getUserBool(' * WARNING: Block structure has changed! Continue?', False):
#				sys.exit(os.EX_OK)
		# Search for blocks which were partially added and generate "pseudo"-blocks with left over files
#		setOldBlocks = set(ximap(lambda x: x[DataProvider.BlockName], oldBlocks))
#		setAddedBlocks = set(ximap(lambda x: x[DataProvider.BlockName], blocksAdded))
#		blockCollision = set.intersection(setOldBlocks, setAddedBlocks)
#		if blockCollision and opts.closeBlock: # Block are closed and contents have changed
#			for block in blocksAdded:
#				if block[DataProvider.BlockName] in blockCollision:
#					block[DataProvider.BlockName] = strGuid(xmd5(str(time.time())).hexdigest())
#		blocks = blocksAdded


def dump_dbs3_json(dn, block_dump_iter):
	for blockDump in block_dump_iter:
		fp = open(os.path.join(dn, blockDump['block']['block_name'] + '.json'), 'w')
		json.dump(blockDump, fp)
		fp.close()


def process_dbs3_json_blocks(opts, block_dump_iter):
	logger = logging.getLogger('dbs3-migration')
	logger.setLevel(logging.DEBUG)

	# dry run without import - just store block dumps in temp dir
	if opts.do_import:
		return dump_dbs3_json(opts.tempdir, block_dump_iter)
	# set-up dbs clients
	dbs3_target_client = DBS3LiteClient(url = opts.dbsTarget)
	dbs3_source_client = DBS3LiteClient(url = opts.dbsSource)
	dbs3_migration_queue = DBS3MigrationQueue()
	dbs3_migration_file = os.path.join(opts.tempdir, 'dbs3_migration.pkl')

	# migrate parents and register datasets with dbs3
	for blockDump in block_dump_iter:
		if not opts.continue_migration:
			# initiate the dbs3 to dbs3 migration of parent blocks
			logger.debug('Checking parentage for block: %s', blockDump['block']['block_name'])
			unique_parent_lfns = set(imap(lambda parent: parent['parent_logical_file_name'], blockDump['file_parent_list']))
			unique_blocks = set()
			for parent_lfn in unique_parent_lfns:
				for block in dbs3_source_client.listBlocks(logical_file_name = parent_lfn):
					unique_blocks.add(block['block_name'])
			for parent_block in unique_blocks:
				if dbs3_target_client.listBlocks(block_name = parent_block): # block already at destination
					logger.debug('Block %s is already at destination', parent_block)
					continue
				migration_task = MigrationTask(block_name = parent_block, migration_url = opts.dbsSource, dbs_client = dbs3_target_client)
				try:
					dbs3_migration_queue.add_migration_task(migration_task)
				except AlreadyQueued as aq:
					logger.debug(aq.message)
			dbs3_migration_queue.save_to_disk(dbs3_migration_file)
		else:
			try:
				dbs3_migration_queue = DBS3MigrationQueue.read_from_disk(dbs3_migration_file)
			except IOError:
				logger.exception('Probably, there is no DBS 3 migration for this dataset ongoing!')
				raise

		# wait for all parent blocks migrated to dbs3
		do_migration(dbs3_migration_queue)
		# insert block into dbs3
		dbs3_target_client.insertBulkBlock(blockDump)


def main():
	# Handle command line options
	options = setup_parser()
	options.config_dict['include parent infos'] = True
	options.config_dict['dataset hash keys'] = options.config_dict['dataset hash keys'].replace(',', ' ')
	if options.opts.jobhash:
		options.config_dict['dataset hash keys'] = options.config_dict['dataset hash keys'] + ' CMSSW_CONFIG_JOBHASH'
	if options.opts.discovery:
		options.config_dict['dataset name pattern'] = '@DS_KEY@'
	if len(options.args) != 1:
		utils.exitWithUsage(options.parser.usage(), 'Neither work directory nor config file specified!')
	# Lock file in case several instances of this program are running
	mutex = FileMutex(os.path.join(options.opts.tempdir, 'datasetDBSAdd.lock'))
	try:
		# 1) Get dataset information
		blocks = discover_blocks(options)
		# 2) Filter datasets
		blocks = filter_blocks(options.opts, blocks)
		# 3) Process datasets (migrate parents and register
		process_dbs3_json_blocks(options.opts, create_dbs3_json_blocks(options.opts, sort_dataset_blocks(blocks)))
	finally:
		mutex.release()


if __name__ == '__main__':
	sys.exit(main())
