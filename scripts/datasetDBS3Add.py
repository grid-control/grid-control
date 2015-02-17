#!/usr/bin/env python
#-#  Copyright 2014 Karlsruhe Institute of Technology
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

import os, optparse
from gcSupport import *
from grid_control.utils import QM
from grid_control.exceptions import UserError
from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.provider_basic import ListProvider
from grid_control.datasets.provider_scan import GCProvider
from grid_control_cms.dbs3_migration_queue import DBS3MigrationQueue, MigrationTask, do_migration, AlreadyQueued
from grid_control_cms.webservice_api import readJSON, sendJSON

class DBSInfoProvider(GCProvider):
	def __init__(self, config, datasetExpr, datasetNick, datasetID = 0):
		tmp = QM(os.path.isdir(datasetExpr), ['OutputDirsFromWork'], ['OutputDirsFromConfig', 'MetadataFromModule'])
		config.set('scanner', str.join(' ', tmp + ['ObjectsFromCMSSW', 'FilesFromJobInfo',
			'MetadataFromCMSSW', 'ParentLookup', 'SEListFromPath', 'LFNFromPath', 'DetermineEvents',
			'FilterEDMFiles']))
		config.set('include config infos', 'True')
		config.set('parent keys', 'CMSSW_PARENT_LFN CMSSW_PARENT_PFN')
		config.set('events key', 'CMSSW_EVENTS_WRITE')
		GCProvider.__init__(self, config, datasetExpr, datasetNick, datasetID)
		self.discovery = config.getBool('discovery', False)

	def generateDatasetName(self, key, data):
		if self.discovery:
			return GCProvider.generateDatasetName(self, key, data)
		if 'CMSSW_DATATIER' not in data:
			raise RuntimeError('Incompatible data tiers in dataset: %s' % data)
		getPathComponents = lambda path: QM(path, tuple(path.strip('/').split('/')), ())
		userPath = getPathComponents(self.nameDS)

		(primary, processed, tier) = (None, None, None)
		# In case of a child dataset, use the parent infos to construct new path
		for parent in data.get('PARENT_PATH', []):
			if len(userPath) == 3:
				(primary, processed, tier) = userPath
			else:
				try:
					(primary, processed, tier) = getPathComponents(parent)
				except:
					pass
		if (primary == None) and (len(userPath) > 0):
			primary = userPath[0]
			userPath = userPath[1:]

		if len(userPath) == 2:
			(processed, tier) = userPath
		elif len(userPath) == 1:
			(processed, tier) = (userPath[0], data['CMSSW_DATATIER'])
		elif len(userPath) == 0:
			(processed, tier) = ('Dataset_%s' % key, data['CMSSW_DATATIER'])

		rawDS = '/%s/%s/%s' % (primary, processed, tier)
		if None in (primary, processed, tier):
			raise RuntimeError('Invalid dataset name supplied: %r\nresulting in %s' % (self.nameDS, rawDS))
		return utils.replaceDict(rawDS, data)

	def generateBlockName(self, key, data):
		return utils.strGuid(key)



class DBS3LiteClient(object):
    def __init__(self, url):
        self._reader_url = '%s/%s' % (url, 'DBSReader')
        self._writer_url = '%s/%s' % (url, 'DBSWriter')
        self._migrate_url = '%s/%s' % (url, 'DBSMigrate')

        self._proxy_path = os.environ.get('X509_USER_PROXY', '')
        if not os.path.exists(self._proxy_path):
            raise UserError('VOMS proxy needed to query DBS3! Environment variable X509_USER_PROXY is "%s"'
                            % self._proxy_path)

        #check if pycurl dbs client is available, will improve performance
        try:
            from dbs.apis.dbsClient import DbsApi as dbs_api
        except ImportError:
            pass
        else:
            self._dbs_reader_api = dbs_api(url=self._reader_url, key=self._proxy_path, cert=self._proxy_path)
            self._dbs_writer_api = dbs_api(url=self._writer_url, key=self._proxy_path, cert=self._proxy_path)
            self._dbs_migrate_api = dbs_api(url=self._migrate_url, key=self._proxy_path, cert=self._proxy_path)

    def insertBulkBlock(self, data):
        if hasattr(self, '_dbs_api'):
            return self._dbs_writer_api.insertBulkBlock(data)
        return sendJSON('%s/%s' % (self._migrate_url, 'bulkblocks'), data=data, cert=self._proxy_path)

    def listBlocks(self, **kwargs):
        if hasattr(self, '_dbs_api'):
            return self._dbs_reader_api.listBlocks(**kwargs)
        return readJSON('%s/%s' % (self._reader_url, 'blocks'), params=kwargs, cert=self._proxy_path)

    def listFiles(self, **kwargs):
        if hasattr(self, '_dbs_api'):
            return self._dbs_reader_api.listFiles(**kwargs)
        return readJSON('%s/%s' % (self._reader_url, 'files'), params=kwargs, cert=self._proxy_path)

    def listFileParents(self, **kwargs):
        if hasattr(self, '_dbs_api'):
            return self._dbs_reader_api.listFileParents(**kwargs)
        return readJSON('%s/%s' % (self._reader_url, 'fileparents'), params=kwargs, cert=self._proxy_path)

    def migrateSubmit(self, data):
        if hasattr(self, '_dbs_api'):
            return self._dbs_migrate_api.migrateSubmit(data)
        return sendJSON('%s/%s' % (self._migrate_url, 'submit'), data=data, cert=self._proxy_path)

    def migrateStatus(self, **kwargs):
        if hasattr(self, '_dbs_api'):
            return self._dbs_migrate_api.migrateStatus(**kwargs)
        return readJSON('%s/%s' % (self._migrate_url, 'status'), params=kwargs, cert=self._proxy_path)


def generateDBS3BlockDumps(opts, blocks):
    for blockInfo in blocks:
        blockDump = dict(dataset_conf_list=[], files=[], file_conf_list=[], file_parent_list=[])
        locations = blockInfo[DataProvider.Locations]
        dataset = blockInfo[DataProvider.Dataset]
        primaryDataset, processedDataset, dataTier = dataset[1:].split('/')
        blockName = blockInfo[DataProvider.BlockName]
        fileList = blockInfo[DataProvider.FileList]
        blockSize = 0
        datasetConfigurations = blockDump[u'dataset_conf_list']

        for fileInfo in fileList:
            metadataInfo = dict(zip(blockInfo[DataProvider.Metadata], fileInfo[DataProvider.Metadata]))
            parent_lfns = metadataInfo['CMSSW_PARENT_LFN']
            datasetType = metadataInfo['CMSSW_DATATYPE'].lower()
            fileSize = metadataInfo['SE_OUTPUT_SIZE']
            lfn = fileInfo[DataProvider.URL]
            fileLumiList = [{u'lumi_section_num': lumi_num,
                             u'run_num': run_num} for run_num, lumi_num in metadataInfo['CMSSW_LUMIS']]

            ###add file information
            fileDict = {u'check_sum': metadataInfo['SE_OUTPUT_HASH_CRC32'],
                        u'file_lumi_list': fileLumiList,
                        u'adler32': 'NOTSET',
                        u'event_count': metadataInfo['CMSSW_EVENTS_WRITE'],
                        u'file_type': 'EDM',
                        u'logical_file_name': lfn,
                        u'file_size': fileSize,
                        u'md5': metadataInfo['SE_OUTPUT_HASH_MD5'],
                        u'auto_cross_section': 0.0
                        }
            blockDump[u'files'].append(fileDict)

            ###add file configurations
            fileConfDict = {u'release_version': metadataInfo['CMSSW_VERSION'],
                            u'pset_hash': metadataInfo['CMSSW_CONFIG_HASH'],
                            u'lfn': lfn,
                            u'app_name': 'cmsRun',
                            u'output_module_label': 'crab2_mod_label',
                            u'global_tag': opts.globaltag,#metadataInfo['GLOBALTAG'],
                            }

            blockDump[u'file_conf_list'].append(fileConfDict)

            ###add file parentage information
            file_parentage = [{'logical_file_name': lfn,
                               'parent_logical_file_name': parent_lfn} for parent_lfn in parent_lfns]
            blockDump[u'file_parent_list'].extend(file_parentage)

            ###fill dataset configurations, same as file configurations with removed lfn
            datasetConfDict = dict(fileConfDict)
            del datasetConfDict[u'lfn']

            if datasetConfDict not in datasetConfigurations:
                datasetConfigurations.append(datasetConfDict)

            ###update block size for block summary information
            blockSize += fileSize

        ###add primary dataset information
        blockDump[u'primds'] = {u'primary_ds_type': datasetType,
                                u'primary_ds_name': primaryDataset}

        ###add dataset information
        blockDump[u'dataset'] = {u'physics_group_name': None,
                                 u'dataset_access_type': 'VALID',
                                 u'data_tier_name': dataTier,
                                 u'processed_ds_name': processedDataset,
                                 u'xtcrosssection': None,###Add to meta data from FrameWorkJobReport, if possible!
                                 u'dataset': dataset
                                 }

        ###add block information
        blockDump[u'block'] = {u'open_for_writing': 0,
                               u'block_name': '%s#%s' % (dataset, blockName),
                               u'file_count': len(fileList),
                               u'block_size': blockSize,
                               u'origin_site_name': locations[0] if len(locations) else 'UNKNOWN'}

        ###add acquisition_era, CRAB is important because of checks within DBS 3
        blockDump[u'acquisition_era'] = {u'acquisition_era_name': 'CRAB',
                                         u'start_date': 0}

        ###add processing_era
        blockDump[u'processing_era'] = {u'processing_version': 1,
                                        u'description': 'grid-control'}

        yield blockDump


if __name__ == '__main__':
	usage = '%s [OPTIONS] <config file / work directory>' % sys.argv[0]
	parser = optparse.OptionParser(usage=usage)
	parser.add_option('-G', '--globaltag',       dest='globaltag',          default=None,
		help='Specify global tag')
	parser.add_option('-F', '--input',           dest='inputFile',          default=None,
		help='Specify dbs input file to use instead of scanning job output')
	parser.add_option('-k', '--key-select',      dest='dataset key select', default='',
		help='Specify dataset keys to process')

	parser.add_option('-L', '--no-lumi',         dest='importLumi',    default=True,   action='store_false',
		help='Do not include lumi section information [Default: Include Lumi information]')
	parser.add_option('-p', '--no-parents',      dest='importParents', default=True,   action='store_false',
		help='Disable import of parent datasets into target DBS instance - ' +
			'Warning: this will disconnect the dataset from it\'s parents [Default: Import parents]')

	ogDiscover = optparse.OptionGroup(parser, 'Discovery options - ignored in case dbs input file is specified', '')
	ogDiscover.add_option('-n', '--name',        dest='dataset name pattern', default='',
		help='Specify dbs path name - Example: DataSet_@NICK@_@VAR@')
	ogDiscover.add_option('-T', '--datatype',    dest='datatype',      default=None,
		help='Supply dataset type in case cmssw report did not specify it - valid values: "mc" or "data"')
	ogDiscover.add_option('-m', '--merge',       dest='merge parents', default=False,  action='store_true',
		help='Merge output files from different parent blocks into a single block [Default: Keep boundaries]')
	ogDiscover.add_option('-j', '--jobhash',     dest='useJobHash',    default=False,  action='store_true',
		help='Use hash of all config files in job for dataset key calculation')
	ogDiscover.add_option('-u', '--unique-cfg',  dest='uniqueCfg',     default=False,  action='store_true',
		help='Cirumvent edmConfigHash collisions so each dataset is stored with unique config information')
	ogDiscover.add_option('-P', '--parent',      dest='parent source', default='',
		help='Override parent information source - to bootstrap a reprocessing on local files')
	ogDiscover.add_option('-H', '--hash-keys',   dest='dataset hash keys', default='',
		help='Included additional variables in dataset hash calculation')
	parser.add_option_group(ogDiscover)

	ogDiscover2 = optparse.OptionGroup(parser, 'Discovery options II - only available when config file is used', '')
	ogDiscover2.add_option('-J', '--job-selector',    dest='selected',      default=None,
		help='Specify dataset(s) to process')
	parser.add_option_group(ogDiscover2)

	ogMode = optparse.OptionGroup(parser, 'Processing mode', '')
	ogMode.add_option('-b', '--batch',           dest='batch',         default=False, action='store_true',
		help='Enable non-interactive batch mode [Default: Interactive mode]')
	ogMode.add_option('-d', '--discovery',       dest='discovery',     default=False, action='store_true',
		help='Enable discovery mode - just collect file information and exit')
	ogMode.add_option('-i', '--no-import',       dest='doImport',      default=True,  action='store_false',
		help='Disable import of new datasets into target DBS instance - only temporary xml files are created, ' +
			'which can be added later via datasetDBSTool.py [Default: Import datasets]')
	parser.add_option_group(ogMode)

	ogInc = optparse.OptionGroup(parser, 'Incremental adding of files to DBS', '')
	ogInc.add_option('-I', '--incremental',     dest='incremental',   default=False,  action='store_true',
		help='Skip import of existing files - Warning: this destroys coherent block structure!')
#	ogInc.add_option('-o', '--open-blocks',     dest='closeBlock',    default=True,   action='store_false',
#		help='Keep blocks open for addition of further files [Default: Close blocks]')
	parser.add_option_group(ogInc)

#	ogInst = optparse.OptionGroup(parser, 'DBS instance handling', '')
#	ogInst.add_option('-t', '--target-instance', dest='dbsTarget',
#		default='https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet',
#		help='Specify target dbs instance url')
#	ogInst.add_option('-s', '--source-instance', dest='dbsSource',
#		default='http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet',
#		help='Specify source dbs instance url(s), where parent datasets are taken from')
#	parser.add_option_group(ogInst)

	ogDbg = optparse.OptionGroup(parser, 'Display options', '')
	ogDbg.add_option('-D', '--display-dataset', dest='display_data',  default=None,
		help='Display information associated with dataset key(s) (accepts "all")')
	ogDbg.add_option('-C', '--display-config',  dest='display_cfg',   default=None,
		help='Display information associated with config hash(es) (accepts "all")')
	ogDbg.add_option('-v', '--verbose',         dest='verbosity',     default=0, action='count',
		help='Increase verbosity')
	parser.add_option_group(ogDbg)

	(opts, args) = parser.parse_args()
	utils.verbosity(opts.verbosity)
	setattr(opts, 'include parent infos', opts.importParents)
	setattr(opts, 'dataset hash keys', getattr(opts, 'dataset hash keys').replace(',', ' '))
	if opts.useJobHash:
		setattr(opts, 'dataset hash keys', getattr(opts, 'dataset hash keys') + ' CMSSW_CONFIG_JOBHASH')

	# 0) Get work directory, create dbs dump directory
	if len(args) != 1:
		utils.exitWithUsage(usage, 'Neither work directory nor config file specified!')
	if os.path.isdir(args[0]):
		opts.workDir = os.path.abspath(os.path.normpath(args[0]))
	else:
		opts.workDir = getConfig(configFile = args[0]).getWorkPath()
	opts.tmpDir = '/tmp'#os.path.join(opts.workDir, 'dbs')
	if not os.path.exists(opts.tmpDir):
		os.mkdir(opts.tmpDir)
	# Lock file in case several instances of this program are running
	mutex = FileMutex(os.path.join(opts.tmpDir, 'datasetDBSAdd.lock'))

	# 1) Get dataset information
	if opts.inputFile:
		provider = datasets.ListProvider(getConfig(), None, opts.inputFile, None)
	else:
		config = getConfig(configDict = {'dataset': dict(parser.values.__dict__)})
		if opts.discovery:
			config.set('dataset name pattern', '@DS_KEY@')
		provider = DBSInfoProvider(config, args[0], None)

	provider.saveState(os.path.join(opts.tmpDir, 'dbs.dat'))
	if opts.discovery:
		sys.exit(os.EX_OK)
	blocks = provider.getBlocks()

	# 2) Filter datasets
	if opts.incremental:
		# Query target DBS for all found datasets and perform dataset resync with "supposed" state
		dNames = set(map(lambda b: b[DataProvider.Dataset], blocks))
		dNames = filter(lambda ds: hasDataset(opts.dbsTarget, ds), dNames)
		config = getConfig(configDict = {None: {'dbs instance': opts.dbsTarget}})
		oldBlocks = reduce(operator.add, map(lambda ds: DBSApiv2(config, None, ds, None).getBlocks(), dNames), [])
		(blocksAdded, blocksMissing, blocksChanged) = DataProvider.resyncSources(oldBlocks, blocks)
		if len(blocksMissing) or len(blocksChanged):
			if not utils.getUserBool(' * WARNING: Block structure has changed! Continue?', False):
				sys.exit(os.EX_OK)
		# Search for blocks which were partially added and generate "pseudo"-blocks with left over files
		setOldBlocks = set(map(lambda x: x[DataProvider.BlockName], oldBlocks))
		setAddedBlocks = set(map(lambda x: x[DataProvider.BlockName], blocksAdded))
		blockCollision = set.intersection(setOldBlocks, setAddedBlocks)
		if blockCollision and opts.closeBlock: # Block are closed and contents have changed
			for block in blocksAdded:
				if block[DataProvider.BlockName] in blockCollision:
					block[DataProvider.BlockName] = utils.strGuid(md5(str(time.time())).hexdigest())
		blocks = blocksAdded

	# 3) Display dataset properties
	if opts.display_data or opts.display_cfg:
		raise APIError('Not yet reimplemented')

	from python_compat import NullHandler, set

	#set-up logging
	logging.basicConfig(format='%(levelname)s: %(message)s')
	logger = logging.getLogger('dbs3-migration')
	logger.addHandler(NullHandler())
	logger.setLevel(logging.DEBUG)

	#set-up dbs clients
	dbs3_phys03_client = DBS3LiteClient(url='https://cmsweb.cern.ch/dbs/prod/phys03')
	dbs3_global_client = DBS3LiteClient(url='https://cmsweb.cern.ch/dbs/prod/global')
	proxy_path = os.environ.get('X509_USER_PROXY')
	#dbs3_client = dbs_api(url='https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader', key=proxy_path, cert=proxy_path,
	#					  verifypeer=False)
	dbs3_migration_queue = DBS3MigrationQueue()

	for blockDump in generateDBS3BlockDumps(opts, blocks):
		###initiate the dbs3 to dbs3 migration of parent blocks
		logger.debug('Checking parentage for block: %s' % blockDump['block']['block_name'])
		unique_parent_lfns = set((parent[u'parent_logical_file_name'] for parent in blockDump[u'file_parent_list']))
		unique_blocks = set((block['block_name'] for parent_lfn in unique_parent_lfns
			for block in dbs3_global_client.listBlocks(logical_file_name=parent_lfn)))
		for block_to_migrate in unique_blocks:
			if dbs3_phys03_client.listBlocks(block_name=block_to_migrate):
				#block already at destination
				continue

			migration_task = MigrationTask(block_name=block_to_migrate,
				migration_url='http://a.b.c', dbs_client=None)#dbs3_client)
			try:
				dbs3_migration_queue.add_migration_task(migration_task)
			except AlreadyQueued as aq:
				logger.debug(aq.message)

		#wait for all parent blocks migrated to dbs3
		do_migration(dbs3_migration_queue)

		#insert block into dbs3
		#dbs3_client.insertBulkBlock(blockDump)
