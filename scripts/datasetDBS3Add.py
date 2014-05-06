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
from grid_control.datasets import DataProvider, ListProvider
import pprint

config = getConfig(cfgDict = {'nickname check collision': 'False'}, section = 'dataset')
provider = ListProvider(config, 'datacache.dat', None, None)

def generateDBS3BlockDumps(provider):
    for blockInfo in provider.getBlocks():
        blockDump = dict(files=[], file_conf_list=[], file_parent_list=[])
        locations = blockInfo[DataProvider.Locations]
        dataset = blockInfo[DataProvider.Dataset]
        primaryDataset, processedDataset, dataTier = dataset[1:].split('/')
        blockName = blockInfo[DataProvider.BlockName]
        fileList = blockInfo[DataProvider.FileList]
        blockSize = 0
        datasetConfigurations = []

        for fileInfo in fileList:
            metadataInfo = dict(zip(blockInfo[DataProvider.Metadata], fileInfo[DataProvider.Metadata]))
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
                          'parent_logical_file_name': parent_lfn} for parent_lfn in metadataInfo['CMSSW_PARENT_LFN']]
            blockDump[u'file_parent_list'].extend(file_parentage)

            ###fill dataset configurations
            datasetConfDict = dict(fileConfDict)
            del datasetConfDict[u'lfn']
            ###check for duplicates
            if datasetConfDict not in datasetConfigurations:
                datasetConfigurations.extend(datasetConfDict)

            ###update block size for block information
            blockSize += fileSize

        ###add primary dataset information
        blockDump[u'primds'] = {u'primary_ds_type': datasetType,
                                u'primary_ds_name': primaryDataset}

        ###add dataset information
        blockDump[u'dataset'] = {u'physics_group_name': None,
                                 u'dataset_access_type': 'PRODUCTION',
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

        ###add dataset configurations
        blockDump[u'dataset_conf_list'] = list(datasetConfigurations)

        yield blockDump

for blockDump in generateDBS3BlockDumps(provider):
    pprint.pprint(blockDump)
