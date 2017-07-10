# | Copyright 2007-2017 Karlsruhe Institute of Technology
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

import os, time, random
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_grid import GridCancelJobs, GridCheckJobs, GridWMS
from grid_control.utils import get_path_share, ping_host, resolve_install_path
from grid_control.utils.activity import Activity
from grid_control.utils.parsing import parse_bool, parse_str
from grid_control.utils.persistency import load_dict, save_dict
from grid_control.utils.process_base import LocalProcess
from hpfwk import clear_current_exception
from python_compat import md5_hex, sort_inplace


class DiscoverGliteEndpointsLazy(object):  # TODO: Move to broker infrastructure
	def __init__(self, config):
		self._state_fn = config.get_work_path('glitewms.info')
		(self._wms_list_ok, self._wms_list_all, self._ping_dict, self.pos) = self._load_state()
		self._wms_timeout_dict = {}
		self._full = config.get_bool('wms discover full', True, on_change=None)
		self._lcg_infosites_exec = resolve_install_path('lcg-infosites')
		self._job_list_match_exec = resolve_install_path('glite-wms-job-list-match')

	def get_endpoint(self):
		activity = Activity('Discovering available WMS services')
		wms_best_list = []
		for wms in self._list_endpoint_good():
			activity_wms = Activity('pinging WMS %s' % wms)
			if wms is None:
				continue
			ping, pingtime = self._ping_dict.get(wms, (None, 0))
			if time.time() - pingtime > 30 * 60:  # check every ~30min
				ping = ping_host(wms.split('://')[1].split('/')[0].split(':')[0])
				self._ping_dict[wms] = (ping, time.time() + 10 * 60 * random.random())  # 10 min variation
			if ping is not None:
				wms_best_list.append((wms, ping))
			activity_wms.finish()
		activity.finish()
		if not wms_best_list:
			return None
		sort_inplace(wms_best_list, key=lambda name_ping: name_ping[1])
		result = _choice_exp(wms_best_list)
		if result is not None:
			activity = Activity('selecting WMS %s' % result)
			wms, ping = result  # reduce timeout by 5min for chosen wms => re-ping every 6 submits
			self._ping_dict[wms] = (ping, self._ping_dict[wms][1] + 5 * 60)
			result = wms
			activity.finish()
		self._update_state()
		return result

	def get_site_list(self):
		return self._match_sites(self.get_endpoint())

	def _list_endpoint_all(self):
		result = []
		proc = LocalProcess(self._lcg_infosites_exec, 'wms')
		for line in proc.stdout.iter(timeout=10):
			result.append(line.strip())
		proc.status_raise(timeout=0)
		random.shuffle(result)
		return result

	def _list_endpoint_good(self):
		if (self.pos is None) or (len(self._wms_list_all) == 0):  # initial discovery
			self.pos = 0
			self._wms_list_all = self._list_endpoint_all()
		if self._full:
			if not self._wms_list_ok:
				for wms in self._wms_list_all:
					match = self._match_sites(wms)
					if match:
						self._wms_list_ok.append(wms)
				self._update_state()
			return self._wms_list_ok
		if self.pos == len(self._wms_list_all):  # self.pos = None => perform rediscovery in next step
			self.pos = 0
		else:
			wms = self._wms_list_all[self.pos]
			if wms in self._wms_list_ok:
				self._wms_list_ok.remove(wms)
			if len(self._match_sites(wms)):
				self._wms_list_ok.append(wms)
			self.pos += 1
			if self.pos == len(self._wms_list_all):  # mark finished
				self._wms_list_ok.append(None)
		return self._wms_list_ok

	def _load_state(self):
		try:
			assert os.path.exists(self._state_fn)
			tmp = load_dict(self._state_fn, ' = ')
			ping_dict = {}
			for wms in tmp:
				is_ok, ping, ping_time = tuple(tmp[wms].split(',', 2))
				if parse_bool(is_ok):
					ping_dict[wms] = (parse_str(ping, float), parse_str(ping_time, float, 0))
			return (list(ping_dict), list(tmp), ping_dict, 0)
		except Exception:
			clear_current_exception()
			return ([], [], {}, None)

	def _match_sites(self, endpoint):
		activity = Activity('Discovering available WMS services - testing %s' % endpoint)
		check_arg_list = ['-a']
		if endpoint:
			check_arg_list.extend(['-e', endpoint])
		check_arg_list.append(get_path_share('null.jdl'))

		proc = LocalProcess(self._job_list_match_exec, *check_arg_list)
		result = []
		for line in proc.stdout.iter(timeout=3):
			if line.startswith(' - '):
				result.append(line[3:].strip())
		activity.finish()
		if proc.status(timeout=0) is None:
			self._wms_timeout_dict[endpoint] = self._wms_timeout_dict.get(endpoint, 0) + 1
			if self._wms_timeout_dict.get(endpoint, 0) > 10:  # remove endpoints after 10 failures
				self._wms_list_all.remove(endpoint)
			return []
		return result

	def _update_state(self):
		tmp = {}
		for wms in self._wms_list_all:
			pingentry = self._ping_dict.get(wms, (None, 0))
			tmp[wms] = '%r,%s,%s' % (wms in self._wms_list_ok, pingentry[0], pingentry[1])
		save_dict(tmp, self._state_fn, ' = ')


class GliteWMS(GridWMS):
	alias_list = ['gwms']
	config_section_list = GridWMS.config_section_list + ['glite-wms', 'glitewms']

	def __init__(self, config, name, check_executor=None):
		GridWMS.__init__(self, config, name,
			submit_exec=resolve_install_path('glite-wms-job-submit'),
			output_exec=resolve_install_path('glite-wms-job-output'),
			check_executor=check_executor or GridCheckJobs(config, 'glite-wms-job-status'),
			cancel_executor=GridCancelJobs(config, 'glite-wms-job-cancel'))

		self._delegate_exec = resolve_install_path('glite-wms-job-delegate-proxy')
		self._submit_args_dict.update({'-r': self._ce, '--config': self._config_fn})
		self._use_delegate = config.get_bool('try delegate', True, on_change=None)
		self._force_delegate = config.get_bool('force delegate', False, on_change=None)
		self._discovery_plugin = None
		if config.get_bool('discover wms', True, on_change=None):
			self._discovery_plugin = DiscoverGliteEndpointsLazy(config)
		self._discover_sites = config.get_bool('discover sites', False, on_change=None)

	def submit_jobs(self, jobnum_list, task):
		if not self._begin_bulk_submission():  # Trying to delegate proxy failed
			if self._force_delegate:  # User switched on forcing delegation => exception
				raise BackendError('Unable to delegate proxy!')
			self._log.error('Unable to delegate proxy! Continue with automatic delegation...')
			self._submit_args_dict.update({'-a': ' '})
			self._use_delegate = False
		for result in GridWMS.submit_jobs(self, jobnum_list, task):
			yield result

	def _begin_bulk_submission(self):
		self._submit_args_dict.update({'-d': None})
		if self._discovery_plugin:
			self._submit_args_dict.update({'-e': self._discovery_plugin.get_endpoint()})
		if self._use_delegate is False:
			self._submit_args_dict.update({'-a': ' '})
			return True
		delegate_id = 'GCD' + md5_hex(str(time.time()))[:10]
		activity = Activity('creating delegate proxy for job submission')
		delegate_arg_list = []
		if self._config_fn:
			delegate_arg_list.extend(['--config', self._config_fn])
		proc = LocalProcess(self._delegate_exec, '-d', delegate_id,
			'--noint', '--logfile', '/dev/stderr', *delegate_arg_list)
		output = proc.get_output(timeout=10, raise_errors=False)
		if ('glite-wms-job-delegate-proxy Success' in output) and (delegate_id in output):
			self._submit_args_dict.update({'-d': delegate_id})
		activity.finish()

		if proc.status(timeout=0, terminate=True) != 0:
			self._log.log_process(proc)
		return self._submit_args_dict.get('-d') is not None

	def _get_site_list(self):
		if self._discover_sites and self._discovery_plugin:
			return self._discovery_plugin.get_site_list()


def _choice_exp(sample, prob=0.5):
	for item in sample:
		if random.random() < prob:
			return item
	return sample[-1]
