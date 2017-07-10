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

import math, time
from grid_control.config import ConfigError
from grid_control.job_db import Job
from grid_control.report import LocationReport, TableReport
from grid_control.utils.parsing import parse_str, str_time_short
from python_compat import imap, itemgetter, lmap, lzip, sorted


class LocationHistoryReport(LocationReport):
	alias_list = ['history']

	def _fill_report_dict_list(self, reports, job_obj):
		history = list(job_obj.history.items())
		history.reverse()
		for attempt, site_queue_str in history:
			if site_queue_str != 'N/A':
				reports.append({1: attempt, 2: ' -> ' + site_queue_str})


class BackendReport(TableReport):
	alias_list = ['backend']

	def __init__(self, config, name, job_db, task=None):
		TableReport.__init__(self, config, name, job_db, task)
		self._level_map = {'wms': 2, 'endpoint': 3, 'site': 4, 'queue': 5}
		self._use_history = config.get_bool('report history', False, on_change=None)
		hierarchy_list = config.get_list('report hierarchy', ['wms'], on_change=None)
		self._idx_list = lmap(lambda x: self._level_map[x.lower()], hierarchy_list)
		self._idx_list.reverse()
		if not self._idx_list:
			raise ConfigError('Backend report was not configured!')
		self._state_map = [(None, 'WAITING'), (Job.RUNNING, 'RUNNING'),
			(Job.FAILED, 'FAILED'), (Job.SUCCESS, 'SUCCESS')]

	def show_report(self, job_db, jobnum_list):
		state_map = dict(self._state_map)

		def _transform(data, label, level):
			if None in data:
				total = data.pop(None)
				if len(data) > 1:
					for result in self._get_entry(state_map, total, ['Total']):
						yield result
					yield '='
			for idx, entry in enumerate(sorted(data)):
				if level == 1:
					for result in self._get_entry(state_map, data[entry], [entry] + label):
						yield result
				else:
					for result in _transform(data[entry], [entry] + label, level - 1):
						yield result
				if idx != len(data) - 1:
					yield '-'
		stats = self._get_hierachical_stats_dict(job_db, jobnum_list)
		displace_states_list = lmap(itemgetter(1), self._state_map)
		header = [('', 'Category')] + lzip(displace_states_list, displace_states_list)
		self._show_table(header, _transform(stats, [], len(self._idx_list)),
			align_str='l' + 'c' * len(state_map), fmt_dict={'': lambda x: str.join(' ', x)})

	def _get_entry(self, state_map, data, label):
		(result_l1, result_l2, result_l3) = ({}, {}, {})
		if len(label) > 0:
			result_l1[''] = [label[0]]
		if len(label) > 1:
			result_l2[''] = ['  %s' % label[1]]
		if len(label) > 2:
			result_l3[''] = ['    %s' % label[2]] + label[3:]
		entry_stats_iter = self._get_entry_stats(state_map, data)
		for (state, cnt, time_mean, time_stddev, time_min, time_max) in entry_stats_iter:
			result_l1[state] = cnt
			result_l2[state] = '%s +/- %s' % (str_time_short(time_mean), str_time_short(time_stddev))
			result_l3[state] = '%s ... %s' % (str_time_short(time_min), str_time_short(time_max))
		yield result_l1
		yield result_l2
		yield result_l3

	def _get_entry_stats(self, state_map, data):
		(tmp_m0, tmp_m1, tmp_m2, tmp_ma, tmp_mi) = ({}, {}, {}, {}, {})
		for raw_state in data:
			state = state_map.get(raw_state, state_map.get(None))
			tmp_m0[state] = tmp_m0.get(state, 0) + len(data[raw_state])
			tmp_m1[state] = tmp_m1.get(state, 0) + sum(data[raw_state])
			tmp_m2[state] = tmp_m2.get(state, 0) + sum(imap(lambda x: x * x, data[raw_state]))
			tmp_ma[state] = max(data[raw_state] + [tmp_ma.get(state, 0)])
			tmp_mi[state] = min(data[raw_state] + [tmp_ma.get(state, 1e10)])
		for state in tmp_m0:
			mean = tmp_m1[state] / tmp_m0[state]
			stddev = math.sqrt(tmp_m2[state] / tmp_m0[state] - mean * mean)
			yield (state, tmp_m0[state], mean, stddev, tmp_mi[state], tmp_ma[state])

	def _get_hierachical_stats_dict(self, job_db, jobnum_list):
		overview = self._get_report_info_list(job_db, jobnum_list)

		def _fill_dict(result, items, idx_list=None, indent=0):
			if not idx_list:
				for entry in items:
					result.setdefault(entry[0], []).append(entry[1])
				return result

			def _get_category_key(entry):
				idx = idx_list[0]
				if idx < len(entry):
					return entry[idx]
				return 'N/A'
			class_map = {}
			for entry in items:
				class_map.setdefault(_get_category_key(entry), []).append(entry)
			tmp = {}
			for class_key in class_map:
				child_info = _fill_dict(result.setdefault(class_key, {}),
					class_map[class_key], idx_list[1:], indent + 1)
				for key in child_info:
					tmp.setdefault(key, []).extend(child_info[key])
			result[None] = tmp
			return tmp
		display_dict = {}
		_fill_dict(display_dict, overview, self._idx_list)
		return display_dict

	def _get_report_info_list(self, job_db, jobnum_list):
		result = []
		t_now = time.time()
		for jobnum in jobnum_list:
			job_obj = job_db.get_job_transient(jobnum)
			runtime = parse_str(job_obj.get('runtime'), int, 0)
			for attempt in job_obj.history:
				if (attempt != job_obj.attempt) and not self._use_history:
					continue
				if (attempt == job_obj.attempt) and (job_obj.state == Job.SUCCESS):
					time_info = runtime
				elif (attempt == job_obj.attempt - 1) and (job_obj.state != Job.SUCCESS):
					time_info = runtime
				elif attempt == job_obj.attempt:
					time_info = t_now - float(job_obj.submitted)
				else:
					time_info = 0
				state = job_obj.state
				if attempt != job_obj.attempt:
					state = Job.FAILED
				site_queue_str = job_obj.history[attempt]
				if site_queue_str == 'N/A':
					dest_info = [site_queue_str]
				else:
					dest_info = site_queue_str.split('/')
				wms_name = job_obj.gc_id.split('.')[1]
				endpoint = 'N/A'
				if 'http:' in job_obj.gc_id:
					endpoint = job_obj.gc_id.split(':')[1].split('/')[0]
				result.append([state, time_info, wms_name, endpoint] + dest_info)
		return result
