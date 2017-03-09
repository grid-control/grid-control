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
from grid_control import utils
from grid_control.gc_exceptions import InstallationError
from grid_control.gui import GUI
from grid_control.job_db import Job
from grid_control_gui.plugin_graph import get_graph_image, get_workflow_graph
from hpfwk import clear_current_exception
from python_compat import lmap, lzip, sorted


try:
	import cherrypy
except Exception:
	clear_current_exception()
	cherrypy = None  # pylint:disable=invalid-name


class CPProgressBar(object):
	def __init__(self, value_min=0, progress=0, value_max=100, total_width=300):
		self._width = total_width
		self._done = round(((progress - value_min) / float(value_max - value_min)) * 100.0)

	def __str__(self):
		return """
<div style="width:%dpx;padding:2px;background-color:white;border:1px solid black;text-align:center">
	<div style="width:%dpx;background-color:green;"> %s%%
	</div>
</div>""" % (self._width, int(self._width * self._done / 100), int(self._done))


class TabularHTML(object):
	def __init__(self, head, data, fmt=None, top=True):
		self._table = """
<style type="text/css">
	table {font-size:12px;color:#333333;border-width: 1px;border-color: #7799aa;border-collapse: collapse;}
	th {font-size:12px;background-color:#aacccc;border-width: 1px;padding: 8px;border-style: solid;border-color: #7799aa;text-align:left;}
	tr {background-color:#ffffff;}
	td {font-size:12px;border-width: 1px;padding: 8px;border-style: solid;border-color: #7799aa;}
</style>"""
		fmt = fmt or {}
		lookup_dict = lmap(lambda id_name: (id_name[0], fmt.get(id_name[0], str)), head)
		header_list = lmap(lambda id_name: '<th>%s</th>' % id_name[1], head)

		def _make_entry_list(entry):
			return lmap(lambda id_fmt: '<td>%s</td>' % id_fmt[1](entry.get(id_fmt[0])), lookup_dict)
		row_list = [header_list] + lmap(_make_entry_list, data)
		if not top:
			row_list = lzip(*row_list)
		rows = lmap(lambda row: '\t<tr>%s</tr>\n' % str.join('', row), row_list)
		if top:
			width_str = 'width:100%;'
		else:
			width_str = ''
		self._table += '<table style="%s" border="1">\n%s</table>' % (width_str, str.join('', rows))

	def __str__(self):
		return self._table


class CPWebserver(GUI):
	def __init__(self, config, workflow):
		if not cherrypy:
			raise InstallationError('cherrypy is not installed!')
		GUI.__init__(self, config, workflow)
		self._counter = 0
		self._workflow = workflow

	def image(self):
		cherrypy.response.headers['Content-Type'] = 'image/png'
		return get_graph_image(get_workflow_graph(self._workflow))
	image.exposed = True

	def jobs(self, *args, **kw):
		result = '<body>'
		result += str(CPProgressBar(0, min(100, self._counter), 100, 300))
		if 'job' in kw:
			jobnum = int(kw['job'])
			info = self._workflow.task.get_job_dict(jobnum)
			result += str(TabularHTML(lzip(sorted(info), sorted(info)), [info], top=False))

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
			('gc_id', 'WMS ID'), ('dest', 'Destination'), ('submitted', 'Submitted')
		]
		fmt_dict = {
			'jobnum': lambda x: '<a href="jobs?job=%s">%s</a>' % (x, x),
			'state': Job.enum2str, 'submitted': _fmt_time
		}
		result += str(TabularHTML(header_list, _iter_job_objs(), fmt=fmt_dict, top=True))
		result += '</body>'
		return result
	jobs.exposed = True

	def index(self):
		result = '<body>'
		result += '<a href="jobs">go to jobs</a>'
		result += '<div>%s</div>' % cherrypy.request.__dict__
		result += '</body>'
		return result
	index.exposed = True

	def start_display(self):
		basic_auth = {'tools.auth_basic.on': True, 'tools.auth_basic.realm': 'earth',
			'tools.auth_basic.checkpassword': cherrypy.lib.auth_basic.checkpassword_dict({'user': '123'})}
		cherrypy.log.screen = False
		cherrypy.engine.autoreload.unsubscribe()
		cherrypy.server.socket_port = 12345
		cherrypy.tree.mount(self, '/', {'/': basic_auth})
		cherrypy.engine.start()
		self._workflow.process(wait=self._process_queue)
		cherrypy.engine.exit()
		cherrypy.server.stop()

	def _process_queue(self, timeout):
		self._counter += 1
		utils.wait(timeout)
