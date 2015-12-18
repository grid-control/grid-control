#!/usr/bin/env python
#-#  Copyright 2015 Karlsruhe Institute of Technology
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

from grid_control_cms.Lexicon import DBSUser
from grid_control_cms.Lexicon import InputValidationError
from grid_control_cms.Lexicon import acqname
from grid_control_cms.Lexicon import block
from grid_control_cms.Lexicon import dataset
from grid_control_cms.Lexicon import globalTag
from grid_control_cms.Lexicon import lfn
from grid_control_cms.Lexicon import primdataset
from grid_control_cms.Lexicon import procdataset
from grid_control_cms.Lexicon import searchstr
from grid_control_cms.Lexicon import userprocdataset
from grid_control_cms.Lexicon import validateUrl

class DBS3InputValidation(object):
    _accepted_input_keys = {
        'dataTier': ['data_tier_name'],
        'blockBulk': ['file_conf_list', 'dataset_conf_list', 'block_parent_list', 'physics_group_name',
                      'processing_era', 'dataset', 'block', 'acquisition_era', 'primds', 'ds_parent_list', 'files',
                      'file_parent_list'],
        'file_conf_list': ['release_version', 'pset_hash', 'pset_name', 'lfn', 'app_name', 'output_module_label',
                           'global_tag'],
        'file_output_config_list': ['release_version', 'pset_hash', 'pset_name', 'lfn', 'app_name',
                                    'output_module_label', 'global_tag'],
        'file_parent_list': ['file_parent_lfn', 'parent_logical_file_name', 'logical_file_name'],
        'dataset_conf_list': ['release_version', 'pset_hash', 'pset_name', 'app_name', 'output_module_label',
                              'global_tag'],
        'output_configs': ['release_version', 'pset_hash', 'pset_name', 'app_name', 'output_module_label',
                           'global_tag'],
        'physics_group_name': [],
        'processing_era': ['processing_version', 'description', 'create_by', 'creation_date'],
        'dataset': ['dataset', 'physics_group_name', 'processed_ds_name', 'dataset_access_type', 'data_tier_name',
                    'output_configs', 'primary_ds_name', 'primary_ds_type', 'acquisition_era_name',
                    'processing_version', 'xtcrosssection', 'create_by', 'creation_date', 'last_modification_date',
                    'last_modified_by', 'detail', 'prep_id', 'dataset_id'],
        'block': ['block_name', 'open_for_writing', 'origin_site_name', 'dataset', 'creation_date', 'creation_date',
                  'create_by', 'last_modification_date', 'last_modified_by', 'file_count', 'block_size'],
        'acquisition_era': ['acquisition_era_name', 'description', 'start_date', 'end_date'],
        'primds': ['primary_ds_type', 'primary_ds_name', 'creation_date', 'create_by'],
        'files': ['check_sum', 'file_lumi_list', 'event_count', 'file_type', 'logical_file_name', 'file_size',
                  'file_output_config_list', 'file_parent_list', 'last_modified_by', 'last_modification_date',
                  'create_by', 'creation_date', 'auto_cross_section', 'adler32', 'dataset', 'block_name', 'md5',
                  'run_num', 'validFileOnly', 'detail', 'run_num', 'release_version', 'pset_hash', 'app_name',
                  'output_module_label', 'origin_site_name', 'lumi_list'],
        'file_lumi_list': ['lumi_section_num', 'run_num'],
        'migration_rqst': ['migration_url', 'migration_input', 'migration_rqst_id']
    }

    _validation_function = {
        'block_name': 'block_name_validation',
        'dataset': 'dataset_validation',
        'logical_file_name': 'logical_file_name_validation',
        'file_parent_lfn': 'logical_file_name_validation',
        'primary_ds_name': 'primary_dataset_validation',
        'processed_ds_name': 'processed_dataset_validation',
        'processing_version': 'processing_version_validation',
        'acquisition_era_name': 'acquisition_era_name_validation',
        'global_tag': 'global_tag_validation',
        'migration_url': 'url_validation',
        'create_by': 'user_validation',
        'last_modified_by': 'user_validation'
    }

    @staticmethod
    def acquisition_era_name_validation(item):
        try:
            acqname(item)
        except AssertionError:
            raise InputValidationError('acquisition_era_name %s does not match input validation for DBS 3 publication'
                                       % item)
        else:
            return item

    @staticmethod
    def block_name_validation(item):
        try:
            block(item)
        except AssertionError:
            raise InputValidationError('block_name %s does not match block name input validation for DBS 3 publication'
                                       % item)
        else:
            return item

    @staticmethod
    def dataset_validation(item):
        try:
            dataset(item)
        except AssertionError:
            raise InputValidationError('dataset %s does not match dataset input validation for DBS 3 publication'
                                       % item)
        else:
            return item

    @staticmethod
    def default_validation(key, item):
        try:
            searchstr(item)
        except AssertionError:
            raise InputValidationError('%s does not match string input validation for key %s in DBS 3 publication'
                                       % (item, key))
        else:
            return item

    @staticmethod
    def global_tag_validation(item):
        try:
            globalTag(item)
        except AssertionError:
            raise InputValidationError('global_tag %s does not match global tag validation in DBS 3 publication' % item)
        else:
            return item

    @staticmethod
    def logical_file_name_validation(item):
        def check_lfn(file_name):
            try:
                lfn(file_name)
            except AssertionError:
                raise InputValidationError('lfn %s does not match lfn input validation for DBS 3 publication'
                                           % file_name)
            else:
                return file_name
        return [check_lfn(file_name) for file_name in item] if isinstance(item, list) else check_lfn(item)

    @staticmethod
    def primary_dataset_validation(item):
        try:
            primdataset(item)
        except AssertionError:
            raise InputValidationError('primary_dataset %s does not match input validation for DBS 3 publication'
                                       % item)
        else:
            return item

    @staticmethod
    def processed_dataset_validation(item):
        try:
            procdataset(item)
        except AssertionError:
            pass
        else:
            return item
        try:
            userprocdataset(item)
        except AssertionError:
            raise InputValidationError('processed_dataset %s does not match input validation for DBS 3 publication'
                                       % item)
        else:
            return item

    @staticmethod
    def url_validation(item):
        try:
            validateUrl(item)
        except AssertionError:
            raise InputValidationError('url %s does not match input validation for DBS 3 publication' % item)
        else:
            return item

    @staticmethod
    def user_validation(item):
        try:
            DBSUser(item)
        except AssertionError:
            raise InputValidationError('user %s does not match input validation for DBS 3 publication' % item)
        else:
            return item

    @staticmethod
    def validate_json_input(input_key, input_data):
        if isinstance(input_data, dict):
            for key in input_data.iterkeys():
                if key not in DBS3InputValidation._accepted_input_keys[input_key]:
                    raise InputValidationError('%s is not a valid key for %s' % (key, input_key))
                input_data[key] = DBS3InputValidation.validate_json_input(key, input_data[key])
        elif isinstance(input_data, list):
            input_data = [DBS3InputValidation.validate_json_input(input_key, entry) for entry in input_data]
        elif isinstance(input_data, basestring):
            try:
                getattr(DBS3InputValidation, DBS3InputValidation._validation_function[input_key])(input_data)
            except KeyError:
                DBS3InputValidation.default_validation(input_key, input_data)

        return input_data