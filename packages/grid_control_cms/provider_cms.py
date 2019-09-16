# | Copyright 2012-2017 Karlsruhe Institute of Technology
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

from grid_control.config import TriggerResync
from grid_control.datasets import DataProvider, DataSplitter, DatasetError
from grid_control.datasets.splitter_basic import HybridSplitter
from grid_control.utils import split_opt
from grid_control.utils.activity import Activity, ProgressActivity
from grid_control.utils.data_structures import make_enum
from grid_control.utils.thread_tools import start_thread
from grid_control.utils.webservice import JSONRestClient
from grid_control_cms.lumi_tools import parse_lumi_filter, str_lumi
from grid_control_cms.sitedb import SiteDB
from hpfwk import AbstractError
from python_compat import itemgetter, lfilter, sorted


CMSLocationFormat = make_enum(['hostname', 'siteDB', 'both'])  # pylint:disable=invalid-name


class CMSBaseProvider(DataProvider):
	# required format: <dataset path>[@<instance>][#<block>]
	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		dataset_config = config.change_view(default_on_change=TriggerResync(['datasets', 'parameters']))
		self._lumi_filter = dataset_config.get_lookup(['lumi filter', '%s lumi filter' % datasource_name],
			default={}, parser=parse_lumi_filter, strfun=str_lumi)
		if not self._lumi_filter.empty():
			config.set('%s processor' % datasource_name, 'LumiDataProcessor', '+=')
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)
		# LumiDataProcessor instantiated in DataProcessor.__ini__ will set lumi metadata as well
		self._lumi_query = dataset_config.get_bool(
			['lumi metadata', '%s lumi metadata' % datasource_name], default=not self._lumi_filter.empty())
		config.set('phedex sites matcher mode', 'ShellStyleMatcher', '?=')
		# PhEDex blacklist: 'T1_*_Disk nodes allow user jobs - other T1's dont!
		self._phedex_filter = dataset_config.get_filter('phedex sites', '-* T1_*_Disk T2_* T3_*',
			default_matcher='BlackWhiteMatcher', default_filter='StrictListFilter')
		self._only_complete = dataset_config.get_bool('only complete sites', True)
		self._only_valid = dataset_config.get_bool('only valid', True)
		self._allow_phedex = dataset_config.get_bool('allow phedex', True)
		self._location_format = dataset_config.get_enum('location format',
			CMSLocationFormat, CMSLocationFormat.hostname)
		self._pjrc = JSONRestClient(url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas')
		self._sitedb = SiteDB()

		dataset_expr_parts = split_opt(dataset_expr, '@#')
		(self._dataset_path, self._dataset_instance, self._dataset_block_selector) = dataset_expr_parts
		instance_default = dataset_config.get('dbs instance', '')
		self._dataset_instance = self._dataset_instance or instance_default
		if not self._dataset_instance:
			self._dataset_instance = 'prod/global'
		elif '/' not in self._dataset_instance:
			self._dataset_instance = 'prod/%s' % self._dataset_instance
		self._dataset_block_selector = self._dataset_block_selector or 'all'

	def check_splitter(self, splitter):
		# Check if splitter is valid
		if (DataSplitter.Skipped in splitter.get_needed_enums()) and not self._lumi_filter.empty():
			self._log.debug('Selected splitter %s is not compatible with active lumi filter!',
				splitter.__name__)
			self._log.warning('Active lumi section filter forced selection of HybridSplitter')
			return HybridSplitter
		return splitter

	def get_dataset_name_list(self):
		if self._cache_dataset is None:
			self._cache_dataset = [self._dataset_path]
			if '*' in self._dataset_path:
				activity = Activity('Getting dataset list for %s' % self._dataset_path)
				self._cache_dataset = list(self._get_cms_dataset_list(self._dataset_path))
				if not self._cache_dataset:
					raise DatasetError('No datasets selected by DBS wildcard %s !' % self._dataset_path)
				activity.finish()
		return self._cache_dataset

	def get_query_interval(self):
		# Define how often the dataprovider can be queried automatically
		return 2 * 60 * 60  # 2 hour delay minimum

	def _fill_cms_fi_list(self, block, block_path):
		activity_fi = Activity('Getting file information')
		lumi_used = False
		lumi_info_dict = {}
		if self._lumi_query:  # central lumi query
			lumi_info_dict = self._get_cms_lumi_dict(block_path)
		fi_list = []
		for (fi, lumi_info_list) in self._iter_cms_files(block_path, self._only_valid, self._lumi_query):
			self._raise_on_abort()
			if lumi_info_dict and not lumi_info_list:
				lumi_info_list = lumi_info_dict.get(fi[DataProvider.URL], [])
			if lumi_info_list:
				(run_list_result, lumi_list_result) = ([], [])
				for (run, lumi_list) in sorted(lumi_info_list):
					run_list_result.extend([run] * len(lumi_list))
					lumi_list_result.extend(lumi_list)
				assert len(run_list_result) == len(lumi_list_result)
				fi[DataProvider.Metadata] = [run_list_result, lumi_list_result]
				lumi_used = True
			fi_list.append(fi)
		if lumi_used:
			block.setdefault(DataProvider.Metadata, []).extend(['Runs', 'Lumi'])
		block[DataProvider.FileList] = fi_list
		activity_fi.finish()

	def _filter_cms_blockinfo_list(self, dataset_path, do_query_sites):
		iter_dataset_block_name_selist = self._iter_cms_blocks(dataset_path, do_query_sites)
		n_blocks = 0
		selected_blocks = False
		for (dataset_block_name, selist) in iter_dataset_block_name_selist:
			n_blocks += 1
			block_name = str.split(dataset_block_name, '#')[1]
			if (self._dataset_block_selector != 'all') and (block_name != self._dataset_block_selector):
				continue
			selected_blocks = True
			yield (dataset_block_name, selist)
		if (n_blocks > 0) and not selected_blocks:
			raise DatasetError('Dataset %r contains %d blocks, but none were selected by %r' % (
				dataset_path, n_blocks, self._dataset_block_selector))

	def _get_cms_dataset_list(self, dataset_path):
		raise AbstractError

	def _get_cms_lumi_dict(self, block_path):
		return None

	def _get_gc_block_list(self, use_phedex):
		dataset_name_list = self.get_dataset_name_list()
		progress_ds = ProgressActivity('Getting dataset', len(dataset_name_list))
		for dataset_idx, dataset_path in enumerate(dataset_name_list):
			progress_ds.update_progress(dataset_idx, msg='Getting dataset %s' % dataset_path)
			counter = 0
			blockinfo_list = list(self._filter_cms_blockinfo_list(dataset_path, not use_phedex))
			progress_block = ProgressActivity('Getting block information', len(blockinfo_list))
			for (block_path, replica_infos) in blockinfo_list:
				result = {}
				result[DataProvider.Dataset] = block_path.split('#')[0]
				result[DataProvider.BlockName] = block_path.split('#')[1]
				progress_block.update_progress(counter,
					msg='Getting block information for ' + result[DataProvider.BlockName])

				if use_phedex and self._allow_phedex:  # Start parallel phedex query
					replicas_dict = {}
					phedex_thread = start_thread('Query phedex site info for %s' % block_path,
						self._get_phedex_replica_list, block_path, replicas_dict)
					self._fill_cms_fi_list(result, block_path)
					phedex_thread.join()
					replica_infos = replicas_dict.get(block_path)
				else:
					self._fill_cms_fi_list(result, block_path)
				result[DataProvider.Locations] = self._process_replica_list(block_path, replica_infos)

				if len(result[DataProvider.FileList]):
					counter += 1
					yield result
			progress_block.finish()

			if counter == 0:
				raise DatasetError('Dataset %s does not contain any valid blocks!' % dataset_path)
		progress_ds.finish()

	def _get_phedex_replica_list(self, block_path, replicas_dict):
		activity_fi = Activity('Getting file replica information from PhEDex')
		# Get dataset se list from PhEDex (perhaps concurrent with get_dbs_file_list)
		replicas_dict[block_path] = []
		for phedex_block in self._pjrc.get(params={'block': block_path})['phedex']['block']:
			for replica in phedex_block['replica']:
				replica_info = (replica['node'], replica.get('se'), replica['complete'] == 'y')
				replicas_dict[block_path].append(replica_info)
		activity_fi.finish()

	def _iter_cms_blocks(self, dataset_path, do_query_sites):
		raise AbstractError

	def _iter_cms_files(self, block_path, query_only_valid, query_lumi):
		raise AbstractError

	def _iter_formatted_locations(self, replica_infos):
		for replica_info in replica_infos:
			(_, _, completed) = replica_info
			if completed:
				for entry in self._iter_replica_locations(replica_info):
					yield entry
			else:
				for entry in self._iter_replica_locations(replica_info):
					yield '(%s)' % entry

	def _iter_replica_locations(self, replica_info):
		(name_node, name_hostname, _) = replica_info
		if self._location_format == CMSLocationFormat.siteDB:
			yield name_node
		else:
			if name_hostname is not None:
				name_hostnames = [name_hostname]
			else:
				name_hostnames = self._sitedb.cms_name_to_se(name_node)
			for name_hostname in name_hostnames:
				if self._location_format == CMSLocationFormat.hostname:
					yield name_hostname
				else:
					yield '%s/%s' % (name_node, name_hostname)

	def _process_replica_list(self, block_path, replica_infos):
		def _empty_with_warning(error_msg, *args):
			self._log.warning('Dataset block %r ' + error_msg, block_path, *args)
			return []

		def _expanded_replica_locations(replica_infos):
			for replica_info in replica_infos:
				for entry in self._iter_replica_locations(replica_info):
					yield entry

		if not replica_infos:
			return _empty_with_warning('has no replica information!')
		replica_infos_selected = self._phedex_filter.filter_list(replica_infos, key=itemgetter(0))
		if not replica_infos_selected:
			return _empty_with_warning('is not available at the selected locations!\n' +
				'Available locations: %s', str.join(', ', self._iter_formatted_locations(replica_infos)))
		if not self._only_complete:
			return list(_expanded_replica_locations(replica_infos_selected))
		replica_infos_complete = lfilter(lambda nn_nh_c: nn_nh_c[2], replica_infos_selected)
		if not replica_infos_complete:
			return _empty_with_warning('is not completely available at the selected locations!\n' +
				'Available locations: %s', str.join(', ', self._iter_formatted_locations(replica_infos)))
		return list(_expanded_replica_locations(replica_infos_complete))


class DBS2Provider(CMSBaseProvider):
	alias_list = ['dbs2']

	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None):  # pylint:disable=super-init-not-called
		raise DatasetError('CMS deprecated all DBS2 Services in April 2014! ' +
			'Please use DBS3Provider instead.')
