# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import sys, time, logging
from grid_control.config import create_config
from grid_control.gc_exceptions import InstallationError
from grid_control.gui import GUI
from grid_control.job_db import Job, JobClass
from grid_control.job_selector import ClassSelector
from grid_control.report import Report
from grid_control.utils import get_local_username, get_path_share
from grid_control.utils.file_tools import SafeFile
from hpfwk import format_exception
from python_compat import StringBuffer, imap, json, lmap, lzip, md5_hex, sorted


class CPNavbar(object):
	def __init__(self, title_link_list, name=None):
		(self._title_link_list, self._name) = (title_link_list, name)

	def get_body(self):
		return _tag('div', str.join('\n', imap(lambda title_link:
			_tag('a', title_link[0], href=title_link[1]), self._title_link_list)),
			Class='topnav', id=self._name or '')

	def get_stylesheet(self):
		return str.join('\n', ['.topnav { background-color: #333333; overflow: hidden; }',
			'.topnav a { float: left; display: block; color: #7799aa; text-align: center;',
			'padding: 14px 16px; text-decoration: none; font-size: 16px; }',
			'.topnav a:hover { background-color: #7799aa; color: #333333; }',
			'.topnav a.active { background-color: #7799aa; color: white; }'])


class CPProgressBar(object):
	def __init__(self, value_min=0, progress=0, value_max=100, total_width=300):
		self._width = total_width
		self._done = round(((progress - value_min) / float(value_max - value_min)) * 100.0)

	def get_body(self):
		pbar = _tag('div', '%s%%' % self._done, style='width:%d%%;' % self._done, Class='progressbar')
		return _tag('div', pbar, Class='progress', style='width:%dpx' % self._width)

	def get_stylesheet(self):
		return str.join('\n', ['.progressbar {background-color:green;}',
			'.progress {padding:2px;background-color:white;border:1px solid black;text-align:center;}'])


class CPTable(object):
	def __init__(self, head, data, fmt_dict=None, pivot=True):
		(self._head, self._data, self._fmt_dict, self._pivot) = (head, data, fmt_dict, pivot)

	def get_body(self):
		fmt_dict = self._fmt_dict or {}
		lookup_dict = lmap(lambda id_name: (id_name[0], fmt_dict.get(id_name[0], str)), self._head)
		header_list = lmap(lambda id_name: _tag('th', id_name[1]), self._head)

		def _make_entry_list(entry):
			return lmap(lambda id_fmt: _tag('td', id_fmt[1](entry.get(id_fmt[0]))), lookup_dict)
		row_list = [header_list] + lmap(_make_entry_list, self._data)
		width_str = 'width:100%;'
		if not self._pivot:
			row_list = lzip(*row_list)
			width_str = ''
		return _tag('table', str.join('', lmap(lambda row: _tag('tr', str.join('', row)), row_list)),
			style=width_str, border=1)

	def get_stylesheet(self):
		common = 'font-size:12px;border-color:#7799aa;border-width:1px'
		return str.join('\n', [
			'table {%s;color:#333333;border-collapse:collapse;}' % common,
			'th {%s;background-color:#7799aa;padding:8px;border-style:solid;text-align:left;}' % common,
			'tr {%s;background-color:#ffffff;}' % common,
			'td {%s;padding:8px;border-style:solid;}' % common,
		])


class CPTag(object):
	def __init__(self, value, content='', **kwargs):
		self._str = _tag(value, content, **kwargs)

	def get_body(self):
		return self._str

	def get_stylesheet(self):
		return ''


class CPWebserver(GUI):
	alias_list = ['cherrypy']

	def __init__(self, config, workflow):
		try:
			import cherrypy
		except Exception:
			raise InstallationError('cherrypy is not installed!')
		self._cherrypy = cherrypy
		GUI.__init__(self, config, workflow)
		(self._config, self._workflow) = (config, workflow)
		self._log = logging.getLogger('webgui')
		self._title = self._workflow.task.get_description().task_name
		self._image_dict = {}
		self._user = config.get('username', get_local_username(), on_change=None)
		self._password = config.get('password', md5_hex(self._title + str(time.time())),
			on_change=None, persistent=True)
		self._hide_login = config.get_bool('hide login', False, on_change=None)
		self._port = config.get_int('port', 12345, on_change=None)
		self._navbar = CPNavbar([
			('Jobs', '/jobs'),
			('Config', '/show_config'),
			('Workflow Graph', '/workflow_graph'),
			('grid-control task: %s' % self._title, ''),
			('Show request info', '/show_request_info'),
		])
		# public web APIs
		self._make_public(self._index)
		self._make_public(self._jobs)
		self._make_public(self._show_config)
		self._make_public(self._show_request_info)
		self._make_public(self._workflow_graph)
		self._make_public(self._workflow_graph_image)

	def end_interface(self):
		self._cherrypy.engine.exit()
		self._cherrypy.server.stop()

	def start_interface(self):
		# password in variable name removes it from debug log!
		password_dict = {self._user: self._password}
		basic_auth = {
			'tools.auth_basic.on': True,
			'tools.auth_basic.realm': 'gridcontrol',
			'tools.auth_basic.checkpassword': self._cherrypy.lib.auth_basic.checkpassword_dict(password_dict)
		}
		self._cherrypy.config.update({'log.screen': False, 'log.access_file': '', 'log.error_file': ''})
		self._log.log_time(logging.INFO, 'Started web server at localhost:%d' % self._port)
		if not self._hide_login:
			self._log.log_time(logging.INFO, 'Login with: user=%r password=%r', self._user, self._password)
		logging.getLogger('cherrypy').propagate = False
		self._cherrypy.engine.timeout_monitor.unsubscribe()
		self._cherrypy.engine.autoreload.unsubscribe()
		self._cherrypy.server.socket_port = self._port
		self._cherrypy.tree.mount(self, '/', {'/': basic_auth})
		self._cherrypy.engine.start()

	def _index(self):
		logo_str = SafeFile(get_path_share('logo.txt'), 'r').read_close()
		return _get_html_page([self._navbar, CPTag('code', _tag('pre', logo_str))])

	def _jobs(self, job=None):
		job_db = self._workflow.job_manager.job_db
		pbar = CPProgressBar(0, job_db.get_job_len(ClassSelector(JobClass.SUCCESS)), len(job_db), 300)
		element_list = [self._navbar, pbar]

		if job is not None:
			jobnum = int(job)
			info = self._workflow.task.get_job_dict(jobnum)
			element_list.append(CPTag('h1', 'Detailed job information for job %d' % jobnum))
			element_list.append(CPTable(lzip(sorted(info), sorted(info)), [info], pivot=False))

		def _fmt_time(value):
			return time.strftime('%Y-%m-%d %T', time.localtime(value))

		def _iter_job_objs():
			for jobnum in self._workflow.job_manager.job_db.get_job_list():
				result = self._workflow.job_manager.job_db.get_job_transient(jobnum).__dict__
				result['jobnum'] = jobnum
				yield result

		header_list = [
			('jobnum', 'Job'), ('state', 'Status'), ('attempt', 'Attempt'),
			('gc_id', 'WMS ID'), ('SITE', 'Site'), ('QUEUE', 'Queue'), ('submitted', 'Submitted')
		]
		fmt_dict = {
			'jobnum': lambda x: '<a href="jobs?job=%s">%s</a>' % (x, x),
			'state': Job.enum2str, 'submitted': _fmt_time
		}
		element_list.append(CPTag('h1', 'List of jobs'))
		element_list.append(CPTable(header_list, _iter_job_objs(), fmt_dict=fmt_dict, pivot=True))
		return _get_html_page(element_list)

	def _make_public(self, fun):  # expose wrapped function call to the web server
		def _wrapper(*args, **kwargs):
			try:
				return fun(*args, **kwargs)
			except Exception:
				msg = 'Error while processing request %s, args=%s, kwargs=%s' % (fun.__name__,
					repr(args), repr(kwargs))
				self._log.exception(msg)
				exc_msg = format_exception(sys.exc_info(), show_threads=0)
				raise self._cherrypy.HTTPError(500, msg + exc_msg.replace('\n', '\0').replace('\0', '\n\n'))
		_wrapper.exposed = True
		setattr(self, fun.__name__.lstrip('_'), _wrapper)

	def _show_config(self):
		buffer = StringBuffer()
		try:
			self._config.change_view(view_class='SimpleConfigView', set_sections=None).write(buffer)
			return _get_html_page([self._navbar, CPTag('pre', _tag('code', buffer.getvalue()))])
		finally:
			buffer.close()

	def _show_request_info(self):
		request_str = json.dumps(self._cherrypy.request.__dict__, indent=2, sort_keys=True, default=repr)
		return _get_html_page([self._navbar, CPTag('code', _tag('pre', request_str))])

	def _workflow_graph(self):
		return _get_html_page([self._navbar, CPTag('img', src='/workflow_graph_image')])

	def _workflow_graph_image(self):
		graph_report = Report.create_instance('PluginReport',
			create_config(), 'plugin', None, task=self._workflow)  # HACK ("plugin" report: task->workflow)
		graph_report.show_report(self._workflow.job_manager.job_db,
			self._workflow.job_manager.job_db.get_job_list())
		self._cherrypy.response.headers['Content-Type'] = 'image/png'
		return SafeFile('plugin_graph.png').read_close()


def _get_html_page(html_obj_list):
	html_stylesheets = str.join('\n', imap(lambda html_obj: html_obj.get_stylesheet(), html_obj_list))
	html_body = str.join('\n', imap(lambda html_obj: html_obj.get_body(), html_obj_list))
	return _tag('html',
		_tag('head', _tag('style', html_stylesheets, Type='text/css')) + _tag('body', '\n' + html_body))


def _tag(value, content='', **kwargs):
	attr_str = str.join('',
		imap(lambda key_value: ' %s="%s"' % (key_value[0].lower(), key_value[1]), kwargs.items()))
	if not content:
		return '<%s%s/>' % (value, attr_str)
	return '<%s%s>%s</%s>' % (value, attr_str, content, value)
