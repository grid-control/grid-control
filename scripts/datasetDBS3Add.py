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

from gcSupport import *
from grid_control.exceptions import UserError
from grid_control.datasets.provider_base import DataProvider
from grid_control.datasets.provider_basic import ListProvider
from grid_control_cms.dbs3_migration_queue import DBS3MigrationQueue, MigrationTask, do_migration, AlreadyQueued
from grid_control_cms.webservice_api import readJSON, sendJSON

import os

config = getConfig(configDict = {'nickname check collision': 'False'}, section = 'dataset')
provider = ListProvider(config, 'datacache.dat', None, None)


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


def generateDBS3BlockDumps(provider):
    for blockInfo in provider.getBlocks():
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
                            u'global_tag': metadataInfo['GLOBALTAG'],
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
    #                      verifypeer=False)
    dbs3_migration_queue = DBS3MigrationQueue()

    for blockDump in generateDBS3BlockDumps(provider):
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
