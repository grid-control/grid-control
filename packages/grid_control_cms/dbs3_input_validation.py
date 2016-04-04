#!/usr/bin/env python
# | Copyright 2015-2016 Karlsruhe Institute of Technology
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

import grid_control_cms.Lexicon
from python_compat import lmap, unicode

def dbs3_check(checker, msg):
	def dbs3_check_int(item):
		try:
			checker(item)
			return item
		except AssertionError:
			raise grid_control_cms.Lexicon.InputValidationError(msg % item)
	return dbs3_check_int

def processed_dataset_validation(item):
	try:
		grid_control_cms.Lexicon.procdataset(item)
		return item
	except AssertionError:
		pass
	try:
		grid_control_cms.Lexicon.userprocdataset(item)
		return item
	except AssertionError:
		raise

def logical_file_name_validation(item):
	check_lfn = dbs3_check(grid_control_cms.Lexicon.lfn,
		'lfn %r does not match input validation for DBS 3 publication')
	if isinstance(item, list):
		return lmap(check_lfn, item)
	return check_lfn(item)

def validate_dbs3_json(input_key, input_data):
	# define input key groups
	ikg_create = ['create_by', 'creation_date']
	ikg_conf = ['app_name', 'output_module_label', 'pset_hash', 'release_version'] # 'pset_name', 'global_tag' only missing in 'files'?
	ikg_modified = ['last_modification_date', 'last_modified_by']
	ikg_pds = ['primary_ds_name', 'primary_ds_type']

	accepted_input_keys = {
		'acquisition_era': ['acquisition_era_name', 'description', 'end_date', 'start_date'],
		'block': ['block_name', 'block_size', 'dataset', 'file_count',
			'open_for_writing', 'origin_site_name'] + ikg_create + ikg_modified,
		'blockBulk': ['acquisition_era', 'block', 'block_parent_list', 'dataset',
			'dataset_conf_list', 'ds_parent_list', 'file_conf_list', 'file_parent_list',
			'files', 'physics_group_name', 'primds', 'processing_era'],
		'dataTier': ['data_tier_name'],
		'dataset': ['acquisition_era_name', 'data_tier_name', 'dataset',
			'dataset_access_type', 'dataset_id', 'detail',
			'output_configs', 'physics_group_name', 'prep_id',
			'processed_ds_name','processing_version', 'xtcrosssection'] + ikg_create + ikg_modified + ikg_pds,
		'dataset_conf_list': ['global_tag', 'pset_name'] + ikg_conf,
		'file_conf_list': ['global_tag', 'lfn', 'pset_name'] + ikg_conf,
		'file_lumi_list': ['lumi_section_num', 'run_num'],
		'file_output_config_list': ['global_tag', 'lfn', 'pset_name'] + ikg_conf,
		'file_parent_list': ['file_parent_lfn', 'logical_file_name', 'parent_logical_file_name'],
		'files': ['adler32', 'auto_cross_section', 'block_name', 'check_sum', 'dataset',
			'detail', 'event_count', 'file_lumi_list', 'file_output_config_list', 'file_parent_list',
			'file_size', 'file_type', 'logical_file_name', 'lumi_list', 'md5', 'origin_site_name',
			'run_num', 'validFileOnly'] + ikg_create + ikg_modified + ikg_conf,
		'migration_rqst': ['migration_input', 'migration_rqst_id', 'migration_url'],
		'output_configs': ['global_tag', 'pset_name'] + ikg_conf,
		'physics_group_name': [],
		'primds': ikg_create + ikg_pds,
		'processing_era': ['description', 'processing_version'] + ikg_create,
	}

	key_validators = {
		'acquisition_era_name': dbs3_check(grid_control_cms.Lexicon.acqname,
			'acquisition_era_name %r does not match input validation for DBS 3 publication'),
		'block_name': dbs3_check(grid_control_cms.Lexicon.block,
			'block_name %r does not match block name input validation for DBS 3 publication'),
		'create_by': dbs3_check(grid_control_cms.Lexicon.DBSUser,
			'create_by user %r does not match input validation for DBS 3 publication'),
		'dataset': dbs3_check(grid_control_cms.Lexicon.dataset,
			'dataset %r does not match dataset input validation for DBS 3 publication'),
		'file_parent_lfn': logical_file_name_validation,
		'global_tag': dbs3_check(grid_control_cms.Lexicon.global_tag_validation,
			'global_tag %r does not match global tag validation in DBS 3 publication'),
		'last_modified_by': dbs3_check(grid_control_cms.Lexicon.DBSUser,
			'create_by user %r does not match input validation for DBS 3 publication'),
		'logical_file_name': logical_file_name_validation,
		'migration_url': dbs3_check(grid_control_cms.Lexicon.validateUrl,
			'url %r does not match input validation for DBS 3 publication'),
		'primary_ds_name': dbs3_check(grid_control_cms.Lexicon.primdataset,
			'primary_dataset %r does not match input validation for DBS 3 publication'),
		'processed_ds_name': dbs3_check(grid_control_cms.Lexicon.processed_ds_name,
			'processed_dataset %r does not match input validation for DBS 3 publication'),
		'processing_version': dbs3_check(grid_control_cms.Lexicon.procversion,
			'processing_version %r does not match input validation for DBS 3 publication'),
	}
	default_validator = dbs3_check(grid_control_cms.Lexicon.searchstr,
		'%r does not match string input validation for key ' + input_key + ' in DBS 3 publication')

	if isinstance(input_data, dict):
		if input_key not in accepted_input_keys:
			raise grid_control_cms.Lexicon.InputValidationError('Unexpected input_key %r' % input_key)
		for key in input_data.keys():
			if key not in accepted_input_keys[input_key]:
				raise grid_control_cms.Lexicon.InputValidationError('%r is not a valid key for %r' % (key, input_key))
			input_data[key] = validate_dbs3_json(key, input_data[key])
		return input_data
	elif isinstance(input_data, list):
		return lmap(lambda entry: validate_dbs3_json(input_key, entry), input_data)
	elif isinstance(input_data, (str, unicode)):
		return key_validators.get(input_key, default_validator)(input_data)
	raise grid_control_cms.Lexicon.InputValidationError('Unexpected datatype %r' % input_data)
