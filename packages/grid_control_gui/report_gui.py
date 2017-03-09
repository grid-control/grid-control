# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.job_db import Job, JobClass
from grid_control.report import ConsoleReport
from grid_control.utils.parsing import parse_str
from grid_control_gui.ansi import Console
from grid_control_gui.report_colorbar import JobProgressBar
from python_compat import ifilter, imap, irange, lfilter, lmap, set, sorted


class CategoryBaseReport(ConsoleReport):
	def __init__(self, job_db, task, jobs=None, config_str=''):
		ConsoleReport.__init__(self, job_db, task, jobs, config_str)
		map_cat2jobs = {}
		map_cat2desc = {}
		# Assignment of jobs to categories (depending on variables and using datasetnick if available)
		job_config_dict = {}
		vn_list = []
		for jobnum in self._jobs:
			if task:
				job_config_dict = task.get_job_dict(jobnum)
			vn_list = sorted(ifilter(lambda var: '!' not in repr(var), job_config_dict.keys()))
			if 'DATASETSPLIT' in vn_list:
				vn_list.remove('DATASETSPLIT')
				vn_list.append('DATASETNICK')
			cat_key = str.join('|', imap(lambda vn: '%s=%s' % (vn, job_config_dict[vn]), vn_list))
			map_cat2jobs.setdefault(cat_key, []).append(jobnum)
			if cat_key not in map_cat2desc:
				map_cat2desc[cat_key] = dict(imap(lambda var: (var, job_config_dict[var]), vn_list))
		# Kill redundant keys from description - seed with last vn_list
		common_var_dict = dict(imap(lambda var: (var, job_config_dict[var]), vn_list))
		for cat_key in map_cat2desc:
			for key in list(common_var_dict.keys()):
				if key not in map_cat2desc[cat_key].keys():
					common_var_dict.pop(key)
				elif common_var_dict[key] != map_cat2desc[cat_key][key]:
					common_var_dict.pop(key)
		for cat_key in map_cat2desc:
			for common_key in common_var_dict:
				map_cat2desc[cat_key].pop(common_key)
		# Generate job-category map with efficient int keys - catNum becomes the new cat_key
		self._job2cat = {}
		self._map_cat2desc = {}
		for cat_num, cat_key in enumerate(sorted(map_cat2jobs)):
			self._map_cat2desc[cat_num] = map_cat2desc[cat_key]
			self._job2cat.update(dict.fromkeys(map_cat2jobs[cat_key], cat_num))

	def _format_desc(self, desc, others):
		if isinstance(desc, str):
			result = desc
		else:
			desc = dict(desc)  # perform copy to allow dict.pop(...) calls
			tmp = []
			if 'DATASETNICK' in desc:
				tmp = ['Dataset: %s' % desc.pop('DATASETNICK')]
			result = str.join(', ', tmp + lmap(lambda key: '%s = %s' % (key, desc[key]), desc))
		if others > 1:
			result += ' (%d subtasks)' % others
		return result

	def _get_category_state_summary(self, job_db):
		cat_state_dict = {}
		for jobnum in self._jobs:
			job_state = job_db.get_job_transient(jobnum).state
			cat_key = self._job2cat[jobnum]
			cat_dict = cat_state_dict.setdefault(cat_key, dict())
			cat_dict[job_state] = cat_dict.get(job_state, 0) + 1
		# (<state overview>, <descriptions>, <#subcategories>)
		return (cat_state_dict, dict(self._map_cat2desc), {})


class AdaptiveBaseReport(CategoryBaseReport):
	def __init__(self, job_db, task, jobs=None, config_str=''):
		CategoryBaseReport.__init__(self, job_db, task, jobs, config_str)
		self._cat_max = int(config_str)
		self._cat_cur = self._cat_max

	def _clear_category_desc(self, var_key_result, map_cat2desc):
		for var_key in sorted(var_key_result, reverse=True):
			if var_key == 'DATASETNICK':  # Never remove dataset infos
				continue
			if sum(imap(len, var_key_result[var_key])) - len(var_key_result[var_key]) == 0:
				cat_desc_set = set()
				for cat_key in map_cat2desc:
					desc_dict = dict(map_cat2desc[cat_key])
					desc_dict.pop(var_key)
					desc = self._format_desc(desc_dict, 0)
					if desc in cat_desc_set:
						break
					cat_desc_set.add(desc)
				if len(cat_desc_set) == len(map_cat2desc):
					for cat_key in map_cat2desc:
						if var_key in map_cat2desc[cat_key]:
							map_cat2desc[cat_key].pop(var_key)

	def _get_category_state_summary(self, job_db):
		(cat_state_dict, map_cat2desc, cat_subcat_dict) = CategoryBaseReport._get_category_state_summary(
			self, job_db)
		# Used for quick calculations
		map_cat2len = {}
		for cat_key in cat_state_dict:
			map_cat2len[cat_key] = sum(cat_state_dict[cat_key].values())

		# Merge successfully completed categories
		def _single_or_successful(cat_key):
			return (len(cat_state_dict[cat_key]) == 1) and (Job.SUCCESS in cat_state_dict[cat_key])
		self._merge_categories(cat_state_dict, map_cat2desc, cat_subcat_dict, map_cat2len,
			'Completed subtasks', lfilter(_single_or_successful, cat_state_dict))

		# Next merge steps shouldn't see non-dict cat_keys in map_cat2desc
		def _select_non_dict(cat_key):
			return not isinstance(map_cat2desc[cat_key], dict)
		hidden_desc = {}
		for cat_key in ifilter(_select_non_dict, list(map_cat2desc)):
			hidden_desc[cat_key] = map_cat2desc.pop(cat_key)
		# Merge categories till goal is reached
		self._merge_categories_with_goal(cat_state_dict, map_cat2desc,
			cat_subcat_dict, map_cat2len, hidden_desc)
		# Remove redundant variables from description
		var_key_result = self._get_possible_merge_categories(map_cat2desc)
		self._clear_category_desc(var_key_result, map_cat2desc)
		# Restore hidden descriptions
		map_cat2desc.update(hidden_desc)
		# Enforce category maximum - merge categories with the least amount of jobs
		if len(cat_state_dict) != self._cat_max:
			self._merge_categories(cat_state_dict, map_cat2desc, cat_subcat_dict,
				map_cat2len, 'Remaining subtasks',
				sorted(cat_state_dict, key=lambda cat_key: -map_cat2len[cat_key])[self._cat_max - 1:])
		# Finalize descriptions:
		if len(map_cat2desc) == 1:
			map_cat2desc[list(map_cat2desc.keys())[0]] = 'All jobs'
		return (cat_state_dict, map_cat2desc, cat_subcat_dict)

	def _get_possible_merge_categories(self, map_cat2desc):
		# Get dictionary with categories that will get merged when removing a variable
		def _eq_dict(dict_a, dict_b, key):
			# Merge parameters to reach category goal - NP hard problem, so be greedy and quick!
			dict_a = dict(dict_a)
			dict_b = dict(dict_b)
			dict_a.pop(key)
			dict_b.pop(key)
			return dict_a == dict_b

		var_key_result = {}
		cat_key_search_dict = {}
		for cat_key in map_cat2desc:
			for var_key in map_cat2desc[cat_key]:
				if var_key not in cat_key_search_dict:
					cat_key_search = set(map_cat2desc.keys())
				else:
					cat_key_search = cat_key_search_dict[var_key]
				if cat_key_search:
					matches = lfilter(lambda ck: _eq_dict(map_cat2desc[cat_key],
						map_cat2desc[ck], var_key), cat_key_search)
					if matches:
						cat_key_search_dict[var_key] = cat_key_search.difference(set(matches))
						var_key_result.setdefault(var_key, []).append(matches)
		return var_key_result

	def _merge_categories(self, cat_state_dict, map_cat2desc, cat_subcat_dict,
			map_cat2len, new_cat_desc, cat_key_list):
		# Function to merge categories together
		if not cat_key_list:
			return
		newcat_key = max(cat_state_dict.keys()) + 1
		new_states = {}
		new_len = 0
		new_subcat_len = 0
		for cat_key in cat_key_list:
			old_states = cat_state_dict.pop(cat_key)
			for state_key in old_states:
				new_states[state_key] = new_states.get(state_key, 0) + old_states[state_key]
			map_cat2desc.pop(cat_key)
			new_len += map_cat2len.pop(cat_key)
			new_subcat_len += cat_subcat_dict.pop(cat_key, 1)
		cat_state_dict[newcat_key] = new_states
		map_cat2desc[newcat_key] = new_cat_desc
		map_cat2len[newcat_key] = new_len
		cat_subcat_dict[newcat_key] = new_subcat_len
		return newcat_key

	def _merge_categories_with_goal(self, cat_state_dict, map_cat2desc,
			cat_subcat_dict, map_cat2len, hidden_desc):
		# Merge categories till goal is reached
		while True:
			delta_goal = max(0, (len(map_cat2desc) + len(hidden_desc)) - self._cat_max)
			var_key_result = self._get_possible_merge_categories(map_cat2desc)
			(var_key_merge, var_key_merge_delta) = (None, 0)
			for var_key in sorted(var_key_result):
				delta = sum(imap(len, var_key_result[var_key])) - len(var_key_result[var_key])
				if (delta <= delta_goal) and (delta > var_key_merge_delta):
					(var_key_merge, var_key_merge_delta) = (var_key, delta)
			if var_key_merge:
				for merge_list in var_key_result[var_key_merge]:
					var_key_merge_desc = dict(map_cat2desc[merge_list[0]])
					var_key_merge_desc.pop(var_key_merge)
					self._merge_categories(cat_state_dict, map_cat2desc, cat_subcat_dict, map_cat2len,
						var_key_merge_desc, merge_list)
			else:
				break


class ModuleReport(CategoryBaseReport):
	alias_list = ['module']

	def show_report(self, job_db):
		(cat_state_dict, map_cat2desc, _) = CategoryBaseReport._get_category_state_summary(self, job_db)
		infos = []
		head = set()
		map_state2cat = {Job.SUCCESS: 'SUCCESS', Job.FAILED: 'FAILED',
			Job.RUNNING: 'RUNNING', Job.DONE: 'RUNNING'}
		for cat_key in map_cat2desc:
			tmp = dict(map_cat2desc[cat_key])
			head.update(tmp.keys())
			for state_key in cat_state_dict[cat_key]:
				state = map_state2cat.get(state_key, 'WAITING')
				tmp[state] = tmp.get(state, 0) + cat_state_dict[cat_key][state_key]
			infos.append(tmp)

		cat_order = ['WAITING', 'RUNNING', 'FAILED', 'SUCCESS']
		utils.display_table(lmap(lambda x: (x, x), sorted(head) + cat_order),
			infos, 'c' * len(head), fmt=dict.fromkeys(cat_order, lambda x: '%7d' % parse_str(x, int, 0)))


class GUIReport(AdaptiveBaseReport):
	alias_list = ['modern']

	def __init__(self, job_db, task, jobs=None, config_str=''):
		(self._max_y, self._max_x) = Console.getmaxyx()
		self._max_x -= 10  # Padding
		AdaptiveBaseReport.__init__(self, job_db, task, jobs, config_str or str(int(self._max_y / 5)))

	def get_height(self):
		return self._cat_cur * 2 + 3

	def show_report(self, job_db):
		(cat_state_dict, map_cat2desc, cat_subcat_dict) = self._get_category_state_summary(job_db)
		self._cat_cur = len(cat_state_dict)

		def _sum_cat(cat_key, states):
			return sum(imap(lambda z: cat_state_dict[cat_key].get(z, 0), states))

		self._print_gui_header('Status report for task:')
		for cat_key in cat_state_dict:  # sorted(cat_state_dict, key=lambda x: -self._categories[x][0]):
			desc = self._format_desc(map_cat2desc[cat_key], cat_subcat_dict.get(cat_key, 0))
			completed = _sum_cat(cat_key, [Job.SUCCESS])
			total = sum(cat_state_dict[cat_key].values())
			self._print_truncated(Console.fmt(desc, [Console.BOLD]), self._max_x - 24,
				'(%5d jobs, %6.2f%%  )' % (total, 100 * completed / float(total)))
			progressbar = JobProgressBar(sum(cat_state_dict[cat_key].values()),
				width=max(0, self._max_x - 24))
			progressbar.update(completed,
				_sum_cat(cat_key, JobClass.ATWMS.state_list),
				_sum_cat(cat_key, [Job.RUNNING, Job.DONE]),
				_sum_cat(cat_key, [Job.ABORTED, Job.CANCELLED, Job.FAILED]))
			self._print_truncated(progressbar, self._max_x)
		for dummy in irange(self._cat_max - len(cat_state_dict)):
			self._output.info(' ' * self._max_x)
			self._output.info(' ' * self._max_x)

	def _print_gui_header(self, message):
		self._print_truncated('-' * (self._max_x - 24), self._max_x)
		header = self._get_header(self._max_x - len(message) - 1)
		self._print_truncated('%s %s' % (message, header), self._max_x)
		self._print_truncated('-' * (self._max_x - 24), self._max_x)

	def _print_truncated(self, value, width, rvalue=''):
		if len(value) + len(rvalue) > width:
			value = str(value)[:width - 3 - len(rvalue)] + '...'
		self._output.info(str(value) + ' ' * (width - (len(value) + len(rvalue))) + str(rvalue) + '\n')
