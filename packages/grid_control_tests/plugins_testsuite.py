# | Copyright 2016-2017 Karlsruhe Institute of Technology
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
from grid_control.backends.wms import BasicWMS, WMS
from grid_control.config import ConfigError
from grid_control.datasets import DataProvider, DataSplitter, PartitionProcessor
from grid_control.gui import GUI
from grid_control.job_db import Job
from grid_control.monitoring import Monitoring
from grid_control.parameters import ParameterInfo, ParameterMetadata
from grid_control.tasks import TaskModule
from grid_control.utils import abort, ensure_dir_exists
from grid_control.utils.activity import Activity, ProgressActivity
from grid_control.utils.thread_tools import start_daemon
from grid_control_gui.ge_basic import BufferGUIElement
from python_compat import irange, lmap, md5_hex


class TestsuiteWMS(BasicWMS):
	def __init__(self, config, name):  # pylint:disable=super-init-not-called
		WMS.__init__(self, config, name)  # pylint:disable=non-parent-init-called
		self._tmp_dir = config.get_work_path('dummy')
		ensure_dir_exists(self._tmp_dir)
		self._token = config.get_composited_plugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls='AccessToken', bind_kwargs={'inherit': True, 'tags': [self]})
		if config.get_bool('watchdog', False, on_change=None):
			start_daemon('tmp', self._tmp)

	def can_submit(self, needed_time, can_currently_submit):
		return True

	def cancel_jobs(self, gc_id_list):
		self._log.warning('Discarded abort of %d jobs', len(gc_id_list))

	def check_jobs(self, gc_id_list):
		progress = ProgressActivity('Checking jobs', len(gc_id_list))
		for idx, gc_id in enumerate(gc_id_list):
			progress.update_progress(idx)
			time.sleep(0.02)
			yield (gc_id, Job.DONE, {})
		progress.finish()

	def deploy_task(self, task, monitor, transfer_se, transfer_sb):
		return

	def get_access_token(self, gc_id):
		return self._token

	def retrieve_jobs(self, gc_id_jobnum_list):
		progress = ProgressActivity('Retrieving jobs', len(gc_id_jobnum_list))
		for (_, jobnum) in gc_id_jobnum_list:
			progress.update_progress(jobnum)
			time.sleep(0.02)
			yield (jobnum, -1, {}, self._tmp_dir)
		progress.finish()

	def submit_jobs(self, jobnum_list, task):
		progress = ProgressActivity('Submitting jobs', len(jobnum_list))
		for idx, jobnum in enumerate(jobnum_list):
			progress.update_progress(idx)
			activity = Activity('Processing job')
			time.sleep(0.2)
			self._write_job_config(os.path.join(self._tmp_dir, str(jobnum)), jobnum, task, {})
			activity.finish()
			yield (jobnum, 'WMSID.%s.%s' % (self._name, 'job-%04d' % jobnum), {})
		progress.finish()

	def _tmp(self):
		while not abort():
			time.sleep(0.1)
			self._log.info('thread %s %s', time.time(), abort())


class TestsuiteGUIElement(BufferGUIElement):
	alias_list = ['debug']

	def __init__(self, config, name, workflow, redraw_event):
		try:
			self._height = int(name)
		except Exception:
			if name == 'random':
				self._height = None
			else:
				self._height = 1
		BufferGUIElement.__init__(self, config, name, workflow, redraw_event)

	def get_height(self):
		if self._height is None:
			return random.randint(1, 5)
		return self._height

	def _update_buffer(self):
		self._buffer.truncate(0)
		width_info_str = str((self._layout_pos, self._layout_width)).center(self._layout_width - 18)
		for line in irange(self._layout_height):
			output = '+ %3d / %-3d %s %3d +\n' % (line + 1, self._layout_height, width_info_str, line + 1)
			self._buffer.write(output)


class TestsuiteProvider(DataProvider):
	def __init__(self, config, datasource_name, dataset_expr, dataset_nick=None, dataset_proc=None):
		DataProvider.__init__(self, config, datasource_name, dataset_expr, dataset_nick, dataset_proc)
		dataset_config = {}
		for key_value_str in dataset_expr.split():
			(key, value) = key_value_str.split('=')
			dataset_config[key] = int(value)
		self._block_len = dataset_config.pop('blocks', 100)
		self._file_len = dataset_config.pop('files', 99)
		self._dir_len = dataset_config.pop('directories', 4)
		self._entries = dataset_config.pop('entries', 12500)
		self._location_max = dataset_config.pop('locations_max', 4)
		self._location_min = dataset_config.pop('locations_min', 1)
		self._seed_block = dataset_config.pop('seed_block', 1512)
		random.seed(dataset_config.pop('seed', 0))

		def create_random_list(fmt, num):
			return lmap(lambda idx: fmt % md5_hex(repr([random.random(), idx])), irange(num))
		self._dataset_list = create_random_list('/dataset/DS_%s', dataset_config.pop('datasets_avail', 9))
		self._dir_list = create_random_list('DIR_%s', dataset_config.pop('directories_avail', 9))
		self._location_list = create_random_list('SE_%s', dataset_config.pop('locations_avail', 6))
		if dataset_config:
			raise ConfigError('Unknown dataset options %r' % dataset_config)

	def _iter_blocks_raw(self):
		for block_num in irange(self._block_len):
			random.seed(block_num + self._seed_block)
			location_len = random.randint(self._location_min, self._location_max)
			location_list = random.sample(self._location_list, location_len)
			if random.random() < 0.05:
				location_list = None
			dir_list = random.sample(self._dir_list, random.randint(1, self._dir_len))
			dir_list_str = str.join('/', dir_list)

			def _create_fi(_):
				return {
					DataProvider.URL: '/%s/FILE_%s' % (dir_list_str, md5_hex(repr(random.random()))),
					DataProvider.NEntries: random.randint(1, self._entries)
				}
			yield {
				DataProvider.Dataset: random.choice(self._dataset_list),
				DataProvider.BlockName: md5_hex(repr(random.random())),
				DataProvider.Locations: location_list,
				DataProvider.FileList: lmap(_create_fi, irange(random.randint(1, self._file_len))),
			}


class TestsuiteGUIFail(GUI):
	def start_interface(self):
		raise Exception('GUI startup failed')


class TestsuiteAbortMonitor(Monitoring):
	def __init__(self, config, name):
		Monitoring.__init__(self, config, name)
		abort(True)


class TestsuiteHandler(Monitoring):
	def on_job_update(self, wms, job_obj, jobnum, data):
		delay = 1
		mon_a1 = Activity('monitor-1A')
		time.sleep(delay)
		mon_a1.update('monitor-1B')
		time.sleep(delay)
		for idx in irange(3):
			mon_a2 = Activity('monitor-%s-2A' % idx)
			time.sleep(delay)
			mon_a2.update('monitor-%s-2B' % idx)
			time.sleep(delay)
			mon_a2.finish()
		mon_a1.update('monitor-1C')
		time.sleep(delay)
		mon_a1.finish()


class TestsuitePartitionProcessor(PartitionProcessor):
	def get_partition_metadata(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked=True), ['FN', 'EVT', 'SKIP'])
		result.append(ParameterMetadata('SID', untracked=False))
		return result

	def process(self, pnum, partition, result):
		result.update({
			'FN': str.join(' ', partition[DataSplitter.FileList]),
			'EVT': partition[DataSplitter.NEntries],
			'SKIP': partition.get(DataSplitter.Skipped, 0),
			'SID': pnum,
		})
		if partition.get(DataSplitter.Invalid, False):
			result[ParameterInfo.ACTIVE] = False


class TestsuiteAbortTask(TaskModule):
	def __init__(self, config, name):
		TaskModule.__init__(self, config, name)
		abort(True)


class TestsuiteTask(TaskModule):
	def __init__(self, config, name):
		TaskModule.__init__(self, config, name)
		while not abort():
			time.sleep(0.5)


class TestsuiteAbortBackend(WMS):
	def __init__(self, config, name):
		WMS.__init__(self, config, name)
		abort(True)
