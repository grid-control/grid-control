# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import os, time
from grid_control.event_base import LocalEventHandler, RemoteEventHandler
from grid_control.job_db import Job
from grid_control.utils import get_local_username, get_path_share, get_version
from grid_control.utils.algos import dict_union, filter_dict
from grid_control.utils.thread_tools import GCThreadPool
from grid_control_cms.DashboardAPI.DashboardAPI import DashboardAPI
from python_compat import identity


class DashboardLocal(LocalEventHandler):
	alias_list = ['dashboard']
	config_section_list = LocalEventHandler.config_section_list + ['dashboard']

	def __init__(self, config, name, task):
		LocalEventHandler.__init__(self, config, name, task)
		self._app = config.get('application', 'shellscript', on_change=None)
		self._dashboard_timeout = config.get_time('dashboard timeout', 5, on_change=None)
		self._tasktype = config.get('task', 'analysis', on_change=None)
		self._taskname = config.get('task name', '@GC_TASK_ID@_@DATASETNICK@', on_change=None)
		self._map_status_job2dashboard = {Job.DONE: 'DONE', Job.FAILED: 'DONE', Job.SUCCESS: 'DONE',
			Job.RUNNING: 'RUNNING', Job.ABORTED: 'ABORTED', Job.CANCELLED: 'CANCELLED'}
		self._tp = GCThreadPool()

	def on_job_output(self, wms, job_obj, jobnum, exit_code):
		self._update_dashboard(wms, job_obj, jobnum, job_obj, {'ExeExitCode': exit_code})

	def on_job_submit(self, wms, job_obj, jobnum):
		# Called on job submission
		token = wms.get_access_token(job_obj.gc_id)
		job_config_dict = self._task.get_job_dict(jobnum)
		self._start_publish(job_obj, jobnum, 'submission', [{'user': get_local_username(),
			'GridName': '/CN=%s' % token.get_user_name(), 'CMSUser': token.get_user_name(),
			'tool': 'grid-control', 'JSToolVersion': get_version(),
			'SubmissionType': 'direct', 'tool_ui': os.environ.get('HOSTNAME', ''),
			'application': job_config_dict.get('SCRAM_PROJECTVERSION', self._app),
			'exe': job_config_dict.get('CMSSW_EXEC', 'shellscript'), 'taskType': self._tasktype,
			'scheduler': wms.get_object_name(), 'vo': token.get_group(),
			'nevtJob': job_config_dict.get('MAX_EVENTS', 0),
			'datasetFull': job_config_dict.get('DATASETPATH', 'none')}])

	def on_job_update(self, wms, job_obj, jobnum, data):
		self._update_dashboard(wms, job_obj, jobnum, job_obj, {})

	def on_workflow_finish(self):
		self._tp.wait_and_drop(self._dashboard_timeout)

	def _publish(self, job_obj, jobnum, task_id, usermsg):
		(_, backend, wms_id) = job_obj.gc_id.split('.', 2)
		dash_id = '%s_%s' % (jobnum, wms_id)
		if 'http' not in job_obj.gc_id:
			dash_id = '%s_https://%s:/%s' % (jobnum, backend, wms_id)
		msg = dict_union({'taskId': task_id, 'jobId': dash_id, 'sid': wms_id}, *usermsg)
		DashboardAPI(task_id, dash_id).publish(**filter_dict(msg, value_filter=identity))

	def _start_publish(self, job_obj, jobnum, desc, msg):
		task_id = self._task.substitute_variables('dashboard task id', self._taskname, jobnum,
			additional_var_dict={'DATASETNICK': ''}).strip('_')
		self._tp.start_daemon('Notifying dashboard about %s of job %d' % (desc, jobnum),
			self._publish, job_obj, jobnum, task_id, msg)

	def _update_dashboard(self, wms, job_obj, jobnum, data, add_dict):
		# Called on job status update and output
		# Translate status into dashboard status message
		status_dashboard = self._map_status_job2dashboard.get(job_obj.state, 'PENDING')
		self._start_publish(job_obj, jobnum, 'status', [{'StatusValue': status_dashboard,
			'StatusValueReason': data.get('reason', status_dashboard).upper(),
			'StatusEnterTime': data.get('timestamp', time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())),
			'StatusDestination': job_obj.get_job_location()}, add_dict])


class DashboardRemote(RemoteEventHandler):
	alias_list = ['dashboard']
	config_section_list = RemoteEventHandler.config_section_list + ['dashboard']

	def __init__(self, config, name):
		RemoteEventHandler.__init__(self, config, name)
		self._app = config.get('application', 'shellscript', on_change=None)
		self._taskname = config.get('task name', '@GC_TASK_ID@_@DATASETNICK@', on_change=None)

	def get_file_list(self):
		yield get_path_share('mon.dashboard.sh', pkg='grid_control_cms')
		for fn in ('DashboardAPI.py', 'Logger.py', 'apmon.py', 'report.py'):
			yield get_path_share('..', 'DashboardAPI', fn, pkg='grid_control_cms')

	def get_mon_env_dict(self):
		result = {'TASK_NAME': self._taskname, 'DB_EXEC': self._app, 'DATASETNICK': ''}
		result.update(RemoteEventHandler.get_mon_env_dict(self))
		return result

	def get_script(self):
		yield get_path_share('mon.dashboard.sh', pkg='grid_control_cms')
