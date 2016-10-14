# | Copyright 2016 Karlsruhe Institute of Technology
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

import os, random
from grid_control.backends.wms import BasicWMS, WMS
from grid_control.config import ConfigError
from grid_control.datasets import DataProvider
from grid_control.job_db import Job
from grid_control.utils import ensure_dir_exists
from python_compat import irange, lmap, md5_hex


class TestsuiteWMS(BasicWMS):
	def __init__(self, config, name):
		WMS.__init__(self, config, name)
		self._tmp_dir = config.get_work_path('dummy')
		ensure_dir_exists(self._tmp_dir)
		self._token = config.get_composited_plugin(['proxy', 'access token'], 'TrivialAccessToken',
			'MultiAccessToken', cls='AccessToken', inherit=True, tags=[self])

	def can_submit(self, needed_time, can_currently_submit):
		return True

	def get_access_token(self, gc_id):
		return self._token

	def deploy_task(self, task, monitor, transfer_se, transfer_sb):
		return

	def submit_jobs(self, jobnumList, task):
		for jobnum in jobnumList:
			self._write_job_config(os.path.join(self._tmp_dir, str(jobnum)), jobnum, task, {})
			yield (jobnum, 'WMSID.%s.%s' % (self._name, 'job-%04d' % jobnum), {})

	def check_jobs(self, gc_id_list):
		for gc_id in gc_id_list:
			yield (gc_id, Job.DONE, {})

	def retrieve_jobs(self, gc_id_jobnum_List):
		for (_, jobnum) in gc_id_jobnum_List:
			yield (jobnum, 0, {}, self._tmp_dir)

	def cancel_jobs(self, gc_id_list):
		self._log.warning('Discarded abort of %d jobs', len(gc_id_list))


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

			def create_fi():
				return {
					DataProvider.URL: '/%s/FILE_%s' % (dir_list_str, md5_hex(repr(random.random()))),
					DataProvider.NEntries: random.randint(1, self._entries)
				}
			fi_list = lmap(lambda x: create_fi(), irange(random.randint(1, self._file_len)))
			yield {
				DataProvider.Dataset: random.choice(self._dataset_list),
				DataProvider.BlockName: md5_hex(repr(random.random())),
				DataProvider.Locations: location_list,
				DataProvider.FileList: fi_list,
			}
