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

from grid_control.gc_exceptions import UserError
from grid_control.job_db import JobClass
from grid_control.report import TableReport
from grid_control.utils.parsing import parse_str
from grid_control_gui.cat_manager import JobCategoryManager
from python_compat import imap, itemgetter, lmap, lzip, set, sorted


class ModuleReport(TableReport):
	alias_list = ['module']

	def __init__(self, config, name, job_db, task=None):
		TableReport.__init__(self, config, name, job_db, task)
		self._cat_manager = JobCategoryManager(config, job_db, task)

	def show_report(self, job_db, jobnum_list):
		(cat_state_dict, map_cat2desc, _) = self._cat_manager.get_category_infos(job_db, jobnum_list)
		infos = []
		head = set()
		(state_class_map, cat_order) = self._get_js_class_infos()
		for cat_key in map_cat2desc:
			tmp = dict(map_cat2desc[cat_key])
			head.update(tmp.keys())
			for job_state in cat_state_dict[cat_key]:
				job_class_name = state_class_map.get(job_state, 'N/A')
				tmp[job_class_name] = tmp.get(job_class_name, 0) + cat_state_dict[cat_key][job_state]
			infos.append(tmp)
		self._show_table(lmap(lambda x: (x, x), sorted(head) + cat_order),
			infos, align_str='c' * len(head),
			fmt_dict=dict.fromkeys(cat_order, lambda x: '%7d' % parse_str(x, int, 0)))

	def _get_js_class_infos(self):
		job_class_list = [('AT WMS', JobClass.ATWMS), ('RUNNING', JobClass.RUNNING),
			('FAILING', JobClass.FAILING), ('SUCCESS', JobClass.SUCCESS)]
		state_class_map = {}
		for (job_class_name, job_class) in job_class_list:
			for job_state in job_class.state_list:
				state_class_map[job_state] = job_class_name
		return (state_class_map, lmap(itemgetter(0), job_class_list))


class VariablesReport(TableReport):
	alias_list = ['variables', 'vars']

	def __init__(self, config, name, job_db, task=None):
		TableReport.__init__(self, config, name, job_db, task)
		if not task:
			raise UserError('Variables report needs task information!')
		self._task = task

	def show_report(self, job_db, jobnum_list):
		(header_list, job_env_dict_list, vn_set) = ([], [], set())
		for jobnum in jobnum_list:
			job_env_dict = self._task.get_job_dict(jobnum)
			vn_set.update(job_env_dict)
			job_env_dict.update(self._task.get_transient_variables())
			job_env_dict_list.append(job_env_dict)
		header_list.extend(imap(lambda key: (key, '<%s>' % key), self._task.get_transient_variables()))
		self._show_table(sorted(header_list + lzip(vn_set, vn_set)), job_env_dict_list)
