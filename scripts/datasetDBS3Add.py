#!/usr/bin/env python
#-#  Copyright 2014-2015 Karlsruhe Institute of Technology
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

import os, sys, logging, optparse
from gcSupport import FileMutex, getConfig, utils
from grid_control.datasets.provider_base import DataProvider
from grid_control_cms.dbs3_input_validation import DBS3InputValidation
from grid_control_cms.dbs3_lite_client import DBS3LiteClient
from grid_control_cms.dbs3_migration_queue import AlreadyQueued
from grid_control_cms.dbs3_migration_queue import DBS3MigrationQueue
from grid_control_cms.dbs3_migration_queue import MigrationTask
from grid_control_cms.dbs3_migration_queue import do_migration
from grid_control_cms.provider_sitedb import SiteDB
from python_compat import NullHandler, set

def generateDBS3BlockDumps(opts, blocks):
    for block_info in blocks:
        block_dump = dict(dataset_conf_list=[], files=[], file_conf_list=[], file_parent_list=[])
        locations = block_info[DataProvider.Locations]
        dataset = block_info[DataProvider.Dataset]
        primary_dataset, processed_dataset, data_tier = dataset[1:].split('/')

        block_name = block_info[DataProvider.BlockName]
        file_list = block_info[DataProvider.FileList]
        block_size = 0
        dataset_configurations = block_dump[u'dataset_conf_list']

        for file_info in file_list:
            metadata_info = dict(zip(block_info[DataProvider.Metadata], file_info[DataProvider.Metadata]))
            parent_lfns = metadata_info['CMSSW_PARENT_LFN']
            dataset_type = metadata_info['CMSSW_DATATYPE']
            file_size = metadata_info['SE_OUTPUT_SIZE']
            lfn = file_info[DataProvider.URL]
            file_lumi_list = [{u'lumi_section_num': lumi_num, u'run_num': run_num} for run_num, lumi_num in
                              metadata_info['CMSSW_LUMIS']]

            ###add file information
            file_dict = {u'check_sum': metadata_info['SE_OUTPUT_HASH_CRC32'],
                         u'file_lumi_list': file_lumi_list,
                         u'adler32': 'NOTSET',
                         u'event_count': metadata_info['CMSSW_EVENTS_WRITE'],
                         u'file_type': 'EDM',
                         u'logical_file_name': lfn,
                         u'file_size': file_size,
                         u'md5': metadata_info['SE_OUTPUT_HASH_MD5'],
                         u'auto_cross_section': 0.0
                         }
            block_dump[u'files'].append(file_dict)

            ###add file configurations
            file_conf_dict = {u'release_version': metadata_info['CMSSW_VERSION'],
                              u'pset_hash': metadata_info['CMSSW_CONFIG_HASH'],
                              u'lfn': lfn,
                              u'app_name': 'cmsRun',
                              u'output_module_label': 'crab2_mod_label',
                              u'global_tag': metadata_info['CMSSW_GLOBALTAG']#,#default=opts.globaltag)
                            }

            block_dump[u'file_conf_list'].append(file_conf_dict)

            ###add file parentage information
            file_parentage = [{'logical_file_name': lfn,
                               'parent_logical_file_name': parent_lfn} for parent_lfn in parent_lfns]
            block_dump[u'file_parent_list'].extend(file_parentage)

            ###fill dataset configurations, same as file configurations with removed lfn
            dataset_conf_dict = dict(file_conf_dict)
            del dataset_conf_dict[u'lfn']

            if dataset_conf_dict not in dataset_configurations:
                dataset_configurations.append(dataset_conf_dict)

            ###update block size for block summary information
            block_size += file_size

        ###add primary dataset information
        block_dump[u'primds'] = {u'primary_ds_type': dataset_type,
                                 u'primary_ds_name': primary_dataset}

        ###add dataset information
        block_dump[u'dataset'] = {u'physics_group_name': None,
                                 u'dataset_access_type': 'VALID',
                                 u'data_tier_name': data_tier,
                                 u'processed_ds_name': processed_dataset,
                                 u'xtcrosssection': None,###Add to meta data from FrameWorkJobReport, if possible!
                                 u'dataset': dataset
                                 }

        ###add block information
        site_db = SiteDB()
        try:
            origin_site_name = site_db.se_to_cms_name(locations[0])[0]
        except IndexError:
            origin_site_name = 'UNKNOWN'

        block_dump[u'block'] = {u'open_for_writing': 0,
                               u'block_name': '%s#%s' % (dataset, block_name),
                               u'file_count': len(file_list),
                               u'block_size': block_size,
                               u'origin_site_name': origin_site_name}

        ###add acquisition_era, CRAB is important because of checks within DBS 3
        block_dump[u'acquisition_era'] = {u'acquisition_era_name': 'CRAB',
                                         u'start_date': 0}

        ###add processing_era
        block_dump[u'processing_era'] = {u'processing_version': 1,
                                        u'description': 'grid-control'}

        yield DBS3InputValidation.validate_json_input('blockBulk', block_dump)


def main():
    usage = '%s [OPTIONS] <config file / work directory>' % sys.argv[0]
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-G', '--globaltag', dest='globaltag', default='crab2_tag', help='Specify global tag')
    parser.add_option('-F', '--input', dest='inputFile', default=None,
                      help='Specify dbs input file to use instead of scanning job output')
#    parser.add_option('-k', '--key-select',      dest='dataset key select', default='',
#        help='Specify dataset keys to process')
    parser.add_option('-c', '--continue-migration', dest='continue_migration', default=False, action='store_true',
                      help='Continue an already started migration')

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
        help='Circumvent edmConfigHash collisions so each dataset is stored with unique config information')
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
    ogMode.add_option('',   '--tempdir',         dest='tmpDir',        default='',
        help='Override temp directory')
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

    ogInst = optparse.OptionGroup(parser, 'DBS instance handling', '')
    ogInst.add_option('-t', '--target-instance', dest='dbsTarget',
                      default='https://cmsweb.cern.ch/dbs/prod/phys03',
                      help='Specify target dbs instance url')
    ogInst.add_option('-s', '--source-instance', dest='dbsSource',
                      default='https://cmsweb.cern.ch/dbs/prod/global',
                      help='Specify source dbs instance url(s), where parent datasets are taken from')
    parser.add_option_group(ogInst)

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
    setattr(opts, 'include parent infos', True)
    setattr(opts, 'importLumi', True)
    setattr(opts, 'dataset hash keys', getattr(opts, 'dataset hash keys').replace(',', ' '))
    if opts.useJobHash:
        setattr(opts, 'dataset hash keys', getattr(opts, 'dataset hash keys') + ' CMSSW_CONFIG_JOBHASH')

    # 0) Get work directory, create dbs dump directory
    if len(args) != 1:
        utils.exitWithUsage(usage, 'Neither work directory nor config file specified!')
    if os.path.isdir(args[0]):
        opts.workDir = os.path.abspath(os.path.normpath(args[0]))
    else:
        opts.workDir = getConfig(configFile=args[0]).getWorkPath()
    if not opts.tmpDir:
        opts.tmpDir = os.path.join(opts.workDir, 'dbs')
    if not os.path.exists(opts.tmpDir):
        os.mkdir(opts.tmpDir)
    # Lock file in case several instances of this program are running
    mutex = FileMutex(os.path.join(opts.tmpDir, 'datasetDBSAdd.lock'))

    # 1) Get dataset information
    if opts.inputFile:
        provider = DataProvider.getInstance('ListProvider', getConfig(), opts.inputFile, None)
    else:
        config = getConfig(configDict = {'dataset': dict(parser.values.__dict__)})
        if opts.discovery:
            config.set('dataset name pattern', '@DS_KEY@')
        provider = DataProvider.getInstance('DBSInfoProvider', config, args[0], None)

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

    #set-up logging
    logging.basicConfig(format='%(levelname)s: %(message)s')
    logger = logging.getLogger('dbs3-migration')
    logger.addHandler(NullHandler())
    logger.setLevel(logging.DEBUG)

    #set-up dbs clients
    dbs3_target_client = DBS3LiteClient(url=opts.dbsTarget)
    dbs3_source_client = DBS3LiteClient(url=opts.dbsSource)

    dbs3_migration_queue = DBS3MigrationQueue()

    for blockDump in generateDBS3BlockDumps(opts, blocks):
        if not opts.continue_migration:
            ###initiate the dbs3 to dbs3 migration of parent blocks
            logger.debug('Checking parentage for block: %s' % blockDump['block']['block_name'])
            unique_parent_lfns = set((parent[u'parent_logical_file_name'] for parent in blockDump[u'file_parent_list']))
            unique_blocks = set((block['block_name'] for parent_lfn in unique_parent_lfns
                                 for block in dbs3_source_client.listBlocks(logical_file_name=parent_lfn)))
            for block_to_migrate in unique_blocks:
                if dbs3_target_client.listBlocks(block_name=block_to_migrate):
                    #block already at destination
                    logger.debug('Block %s is already at destination' % block_to_migrate)
                    continue
                migration_task = MigrationTask(block_name=block_to_migrate,
                                               migration_url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                                               dbs_client=dbs3_target_client)
                try:
                    dbs3_migration_queue.add_migration_task(migration_task)
                except AlreadyQueued as aq:
                    logger.debug(aq.message)

            dbs3_migration_queue.save_to_disk(os.path.join(opts.tmpDir, 'dbs3_migration.pkl'))
        else:
            try:
                dbs3_migration_queue = DBS3MigrationQueue.read_from_disk(os.path.join(opts.tmpDir,
                                                                                      'dbs3_migration.pkl'))
            except IOError as io_err:
                msg = "Probably, there is no DBS 3 migration for this dataset ongoing, Dude!"
                logger.exception('%s\n%s' % (io_err.message, msg))
                raise

        #wait for all parent blocks migrated to dbs3
        do_migration(dbs3_migration_queue)

        #insert block into dbs3
        dbs3_target_client.insertBulkBlock(blockDump)

if __name__ == '__main__':
    sys.exit(main())
