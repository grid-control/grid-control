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

import time
from grid_control import utils
from grid_control.gc_exceptions import InstallationError
from grid_control.gc_plugin import NamedPlugin
from grid_control.gui import GUI
from grid_control.job_db import Job
from grid_control.utils.process_base import LocalProcess
from hpfwk import Plugin
from python_compat import lmap, lzip, set, sorted

try:
	import cherrypy
except Exception:
	cherrypy = None

class CPProgressBar(object):
	def __init__(self, minValue = 0, progress = 0, maxValue = 100, totalWidth = 300):
		self.width = totalWidth
		self.done = round(((progress - minValue) / float(maxValue - minValue)) * 100.0)

	def __str__(self):
		return """
<div style="width:%dpx;padding:2px;background-color:white;border:1px solid black;text-align:center">
	<div style="width:%dpx;background-color:green;"> %s%%
	</div>
</div>""" % (self.width, int(self.width * self.done / 100), int(self.done))


class TabularHTML(object):
	def __init__(self, head, data, fmt = None, top = True):
		self.table = """
<style type="text/css">
	table {font-size:12px;color:#333333;border-width: 1px;border-color: #7799aa;border-collapse: collapse;}
	th {font-size:12px;background-color:#aacccc;border-width: 1px;padding: 8px;border-style: solid;border-color: #7799aa;text-align:left;}
	tr {background-color:#ffffff;}
	td {font-size:12px;border-width: 1px;padding: 8px;border-style: solid;border-color: #7799aa;}
</style>"""
		fmt = fmt or {}
		lookupDict = lmap(lambda id_name: (id_name[0], fmt.get(id_name[0], str)), head)
		headerList = lmap(lambda id_name: '<th>%s</th>' % id_name[1], head)
		def entryList(entry):
			return lmap(lambda id_fmt: '<td>%s</td>' % id_fmt[1](entry.get(id_fmt[0])), lookupDict)
		rowList = [headerList] + lmap(entryList, data)
		if not top:
			rowList = lzip(*rowList)
		rows = lmap(lambda row: '\t<tr>%s</tr>\n' % str.join('', row), rowList)
		if top:
			widthStr = 'width:100%;'
		else:
			widthStr = ''
		self.table += '<table style="%s" border="1">\n%s</table>' % (widthStr, str.join('', rows))

	def __str__(self):
		return self.table


def getGraph(instance, graph = None, visited = None):
	graph = graph or {}
	visited = visited or set()
	children = []
	for attr in dir(instance):
		child = getattr(instance, attr)
		try:
			children.extend(child)
			children.extend(child.values())
		except Exception:
			children.append(child)
	for child in children:
		try:
			if 'grid_control' not in child.__module__:
				continue
			if child.__class__.__name__ in ['instancemethod', 'function', 'type']:
				continue
			graph.setdefault(instance, []).append(child)
			if child not in visited:
				visited.add(child)
				getGraph(child, graph, visited)
		except Exception:
			pass
	return graph


def getNodeLabel(instance):
	result = instance.__class__.__name__
	if isinstance(instance, NamedPlugin):
		if instance.getObjectName().lower() != instance.__class__.__name__.lower():
			result += ' (%s)' % instance.getObjectName()
	return result


def getNodeParent(cls):
	clsOld = None
	while (Plugin not in cls.__bases__) and (clsOld != cls):
		clsOld = cls
		try:
			cls = cls.__bases__[0]
		except Exception:
			pass


def getNodeColor(instance, color_map):
	cnum = color_map.setdefault(getNodeParent(instance.__class__), max(color_map.values() + [0]) + 1)
	if cnum < 12:
		return '/set312/%d' % cnum
	return '/set19/%d' % (cnum % 12 + 1)


class CPWebserver(GUI):
	def __init__(self, config, workflow):
		if not cherrypy:
			raise InstallationError('cherrypy is not installed!')
		GUI.__init__(self, config, workflow)
		self.counter = 0

	def processQueue(self, timeout):
		self.counter += 1
		utils.wait(timeout)

	def _get_workflow_graph(self):
		graph = getGraph(self._workflow)
		classCluster= {}
		for entry in graph:
			classCluster.setdefault(getNodeParent(entry.__class__), []).append(entry)

		clusters = ''

		globalNodes = []
		colors = {}
		for classClusterEntries in classCluster.values():
			if len(classClusterEntries) == 1:
				globalNodes.append(classClusterEntries[0])
			clusters += 'subgraph cluster_0 {'
			for node in classClusterEntries:
				clusters += '%s [label="%s", fillcolor="%s", style="filled"]\n' % (hash(node), getNodeLabel(node), getNodeColor(node, colors))
			clusters += '}\n'

		edgeStr = ''
		for entry in graph:
			for child in graph[entry]:
				try:
					hEntry = hash(entry)
					edgeStr += '%s -> %s\n' % (hEntry, hash(child))
				except Exception:
					pass
		return "digraph mygraph { overlap=False; ranksep=1.5; %s; %s; }\n" % (clusters, edgeStr)

	def image(self):
		proc = LocalProcess('neato', '-Tpng')
		proc.stdin.write(self._get_workflow_graph())
		proc.stdin.close()
		cherrypy.response.headers['Content-Type'] = 'image/png'
		return proc.get_output(timeout = 20)
	image.exposed = True

	def jobs(self, *args, **kw):
		result = '<body>'
		result += str(CPProgressBar(0, min(100, self.counter), 100, 300))
		if 'job' in kw:
			jobNum = int(kw['job'])
			info = self.task.getJobConfig(jobNum)
			result += str(TabularHTML(lzip(sorted(info), sorted(info)), [info], top = False))
		def getJobObjs():
			for jobNum in self.jobMgr.jobDB.getJobs():
				result = self.jobMgr.jobDB.get(jobNum).__dict__
				result['jobNum'] = jobNum
				result.update(result['dict'])
				yield result
		fmtTime = lambda t: time.strftime('%Y-%m-%d %T', time.localtime(t))
		result += str(TabularHTML([
				('jobNum', 'Job'), ('state', 'Status'), ('attempt', 'Attempt'),
				('wmsId', 'WMS ID'), ('dest', 'Destination'), ('submitted', 'Submitted')
			], getJobObjs(),
			fmt = {
				'jobNum': lambda x: '<a href="jobs?job=%s">%s</a>' % (x, x),
				'state': Job.enum2str, 'submitted': fmtTime
			}, top = True))
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

	def displayWorkflow(self):
		basic_auth = {'tools.auth_basic.on': True, 'tools.auth_basic.realm': 'earth',
			'tools.auth_basic.checkpassword': cherrypy.lib.auth_basic.checkpassword_dict({'user' : '123'})}
		cherrypy.log.screen = False
		cherrypy.engine.autoreload.unsubscribe()
		cherrypy.server.socket_port = 12345
		cherrypy.tree.mount(self, '/', {'/' : basic_auth})
		cherrypy.engine.start()
		self._workflow.jobCycle(wait = self.processQueue)
		cherrypy.engine.exit()
		cherrypy.server.stop()
