# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

import time
from grid_control.gc_exceptions import InstallationError
from grid_control.gui import GUI
from grid_control.job_db import Job
from grid_control.utils import wait
from python_compat import StringBuffer, imap, lmap, lzip, sorted


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


class CPWebserver(GUI):
	alias_list = ['cherrypy']

	def __init__(self, config, workflow):
		try:
			import cherrypy
		except Exception:
			raise InstallationError('cherrypy is not installed!')
		self._cherrypy = cherrypy
		GUI.__init__(self, config, workflow)
		(self._config, self._workflow, self._counter) = (config, workflow, 0)
		self._title = self._workflow.task.get_description().task_name

	def show_config(self):
		buffer = StringBuffer()
		try:
			self._config.write(buffer)
			return _tag('pre', _tag('code', buffer.getvalue()))
		finally:
			buffer.close()
	show_config.exposed = True

	def image(self):
		self._cherrypy.response.headers['Content-Type'] = 'image/png'
		return ''
	image.exposed = True

	def jobs(self, *args, **kw):
		element_list = [CPProgressBar(0, min(100, self._counter), 100, 300)]
		if 'job' in kw:
			jobnum = int(kw['job'])
			info = self._workflow.task.get_job_dict(jobnum)
			element_list.append(CPTable(lzip(sorted(info), sorted(info)), [info], pivot=False))

		def _fmt_time(value):
			return time.strftime('%Y-%m-%d %T', time.localtime(value))

		def _iter_job_objs():
			for jobnum in self._workflow.job_manager.job_db.get_job_list():
				result = self._workflow.job_manager.job_db.get_job_transient(jobnum).__dict__
				result['jobnum'] = jobnum
				result.update(result['dict'])
				yield result

		header_list = [
			('jobnum', 'Job'), ('state', 'Status'), ('attempt', 'Attempt'),
			('gc_id', 'WMS ID'), ('SITE', 'Site'), ('QUEUE', 'Queue'), ('submitted', 'Submitted')
		]
		fmt_dict = {
			'jobnum': lambda x: '<a href="jobs?job=%s">%s</a>' % (x, x),
			'state': Job.enum2str, 'submitted': _fmt_time
		}
		element_list.append(CPTable(header_list, _iter_job_objs(), fmt_dict=fmt_dict, pivot=True))
		return _get_html_page(element_list)
	jobs.exposed = True

	def show_request_info(self):
		return _get_html_page([_tag('code', self._cherrypy.request.__dict__)])
	show_request_info.exposed = True

	def index(self):
		return _get_html_page([
			CPNavbar([('Jobs', 'jobs'), ('Config', 'show_config'), ('Workflow Graph', 'image'),
				('grid-control task: %s' % self._title, ''), ('Show request info', 'show_request_info')]),
		])
	index.exposed = True

	def end_interface(self):
		self._cherrypy.engine.exit()
		self._cherrypy.server.stop()

	def start_interface(self):
		basic_auth = {'tools.auth_basic.on': True, 'tools.auth_basic.realm': 'earth',
			'tools.auth_basic.checkpassword':
				self._cherrypy.lib.auth_basic.checkpassword_dict({'user': '123'})}
		self._cherrypy.log.screen = False
		self._cherrypy.engine.timeout_monitor.unsubscribe()
		self._cherrypy.engine.autoreload.unsubscribe()
		self._cherrypy.server.socket_port = 12345
		self._cherrypy.tree.mount(self, '/', {'/': basic_auth})
		self._cherrypy.engine.start()

	def _process_queue(self, timeout):
		self._counter += 1
		wait(timeout)


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
