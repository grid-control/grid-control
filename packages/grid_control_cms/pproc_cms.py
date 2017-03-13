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

from grid_control.datasets import DataSplitter, PartitionProcessor
from grid_control.parameters import ParameterMetadata
from python_compat import imap, lmap


class LFNPartitionProcessor(PartitionProcessor):
	alias_list = ['lfnprefix']

	def __init__(self, config, datasource_name):
		PartitionProcessor.__init__(self, config, datasource_name)
		lfn_modifier = config.get(self._get_pproc_opt('lfn modifier'), '')
		lfn_modifier_shortcuts = config.get_dict(self._get_pproc_opt('lfn modifier dict'), {
			'<xrootd>': 'root://cms-xrd-global.cern.ch/',
			'<xrootd:eu>': 'root://xrootd-cms.infn.it/',
			'<xrootd:us>': 'root://cmsxrootd.fnal.gov/',
		})[0]
		self._prefix = None
		if lfn_modifier == '/':
			self._prefix = '/store/'
		elif lfn_modifier.lower() in lfn_modifier_shortcuts:
			self._prefix = lfn_modifier_shortcuts[lfn_modifier.lower()] + '/store/'
		elif lfn_modifier:
			self._prefix = lfn_modifier + '/store/'

	def enabled(self):
		return self._prefix is not None

	def get_partition_metadata(self):
		return lmap(lambda k: ParameterMetadata(k, untracked=True), ['DATASET_SRM_FILES'])

	def process(self, pnum, partition, result):
		def _modify_filelist_for_srm(filelist):
			return lmap(lambda f: 'file://' + f.split('/')[-1], filelist)

		def _prefix_lfn(lfn):
			return self._prefix + lfn.split('/store/', 1)[-1]

		if self._prefix:
			partition[DataSplitter.FileList] = lmap(_prefix_lfn, partition[DataSplitter.FileList])
			if 'srm' in self._prefix:
				result.update({'DATASET_SRM_FILES': str.join(' ', partition[DataSplitter.FileList])})
				partition[DataSplitter.FileList] = _modify_filelist_for_srm(partition[DataSplitter.FileList])


class CMSSWPartitionProcessor(PartitionProcessor.get_class('BasicPartitionProcessor')):  # pylint:disable=no-init
	alias_list = ['cmsswpart']

	def _format_fn_list(self, fn_list):
		return str.join(', ', imap(lambda x: '"%s"' % x, fn_list))
