# | Copyright 2010-2017 Karlsruhe Institute of Technology
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

import os
from grid_control.config import ConfigError
from grid_control.datasets import DataProvider
from grid_control.parameters import ParameterSource
from grid_control.utils.parsing import str_dict_cfg
from grid_control.utils.table import ConsoleTable
from grid_control_cms.lumi_tools import format_lumi, parse_lumi_filter, str_lumi
from grid_control_cms.task_cmssw import CMSSW
from python_compat import ichain, imap, lfilter, lmap, set, sorted


def str_lumi_nice(lumifilter):
	lumi_filter_str = format_lumi(lumifilter)
	if len(lumi_filter_str) < 5:
		return str.join(', ', lumi_filter_str)
	return '%s ... %s (%d entries)' % (lumi_filter_str[0], lumi_filter_str[-1], len(lumi_filter_str))


class CMSSWAdvanced(CMSSW):  # pylint:disable=too-many-ancestors
	alias_list = ['CMSSW_Advanced']
	config_section_list = CMSSW.config_section_list + ['CMSSW_Advanced']

	def __init__(self, config, name):
		self._name = name  # needed for change_view calls before the constructor
		head = [('DATASETNICK', 'Nickname')]

		# Mapping between nickname and config files:
		self._nm_cfg = config.get_lookup('nickname config', {}, default_matcher='RegExMatcher',
			parser=lambda x: lmap(str.strip, x.split(',')), strfun=lambda x: str.join(',', x))
		if not self._nm_cfg.empty():
			all_config_fn_list = sorted(set(ichain(self._nm_cfg.get_values())))
			config.set('config file', str.join('\n', all_config_fn_list))
			head.append((1, 'Config file'))
		elif config.get('config file', ''):
			raise ConfigError("Please use 'nickname config' instead of 'config file'")

		# Mapping between nickname and constants - configure and display
		# work is handled by the 'normal' parameter factory
		nm_const_vn_list = config.get_list('nickname constants', [], on_change=None)
		param_config = config.change_view(view_class='TaggedConfigView',
			set_classes=None, set_names=None, add_sections=['parameters'])
		param_config.set('constants', str.join(' ', nm_const_vn_list), '+=')
		for const_vn in nm_const_vn_list:
			param_config.set(const_vn + ' matcher', 'RegexMatcher')
			param_config.set(const_vn + ' lookup', 'DATASETNICK')
			head.append((const_vn, const_vn))

		# Mapping between nickname and lumi filter - configure and display
		# work is handled by the 'normal' lumi filter
		config.set('lumi filter matcher', 'RegexMatcher')
		if 'nickname lumi filter' in config.get_option_list():
			config.set('lumi filter',
				str_dict_cfg(config.get_dict('nickname lumi filter', {}, on_change=None)))
		self._nm_lumi = config.get_lookup('lumi filter', {},
			parser=parse_lumi_filter, strfun=str_lumi, on_change=None)
		if not self._nm_lumi.empty():
			head.append((2, 'Lumi filter'))

		CMSSW.__init__(self, config, name)
		self._display_setup(config.get_work_path('datacache.dat'), head)

	def get_job_dict(self, jobnum):
		data = CMSSW.get_job_dict(self, jobnum)
		config_fn_list = self._nm_cfg.lookup(data.get('DATASETNICK'), [], is_selector=False)
		data['CMSSW_CONFIG'] = str.join(' ', imap(os.path.basename, config_fn_list))
		return data

	def _display_setup(self, dataset_fn, head):
		if os.path.exists(dataset_fn):
			nick_name_set = set()
			for block in DataProvider.load_from_file(dataset_fn).get_block_list_cached(show_stats=False):
				nick_name_set.add(block[DataProvider.Nickname])
			self._log.info('Mapping between nickname and other settings:')
			report = []

			def _get_dataset_lookup_psrc(psrc):
				is_lookup_cls = isinstance(psrc, ParameterSource.get_class('LookupBaseParameterSource'))
				return is_lookup_cls and ('DATASETNICK' in psrc.get_parameter_deps())

			ps_lookup = lfilter(_get_dataset_lookup_psrc, self._source.get_used_psrc_list())
			for nick in sorted(nick_name_set):
				tmp = {'DATASETNICK': nick}
				for src in ps_lookup:
					src.fill_parameter_content(None, tmp)
				tmp[1] = str.join(', ', imap(os.path.basename,
					self._nm_cfg.lookup(nick, '', is_selector=False)))
				tmp[2] = str_lumi_nice(self._nm_lumi.lookup(nick, '', is_selector=False))
				report.append(tmp)
			ConsoleTable.create(head, report, 'cl')
