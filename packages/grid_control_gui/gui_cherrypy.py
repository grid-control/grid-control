#-#  Copyright 2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from grid_control import GUI, utils, Job
import time, os, tempfile
try:
	import cherrypy
except:
	print "cherrypy package must be installed!"
	raise

class CPProgressBar:
	def __init__(self, minValue = 0, progress = 0, maxValue = 100, totalWidth = 300):
		self.width = totalWidth
		self.done = round(((progress - minValue) / float(maxValue - minValue)) * 100.0)

	def __str__(self):
		return """
<div style="width:%dpx;padding:2px;background-color:white;border:1px solid black;text-align:center">
	<div style="width:%dpx;background-color:green;"> %s%%
	</div>
</div>""" % (self.width, int(self.width * self.done / 100), int(self.done))

class TabularHTML:
	def __init__(self, head, data, fmt = {}, top = True):
		self.table = """
<style type="text/css">
	table {font-size:12px;color:#333333;border-width: 1px;border-color: #7799aa;border-collapse: collapse;}
	th {font-size:12px;background-color:#aacccc;border-width: 1px;padding: 8px;border-style: solid;border-color: #7799aa;text-align:left;}
	tr {background-color:#ffffff;}
	td {font-size:12px;border-width: 1px;padding: 8px;border-style: solid;border-color: #7799aa;}
</style>"""
		lookupDict = map(lambda (id, name): (id, fmt.get(id, str)), head)
		headerList = map(lambda (id, name): '<th>%s</th>' % name, head)
		entryList = lambda entry: map(lambda (id, fmt): '<td>%s</td>' % fmt(entry.get(id)), lookupDict)
		rowList = [headerList] + map(entryList, data)
		if not top:
			rowList = zip(*rowList)
		rows = map(lambda row: '\t<tr>%s</tr>\n' % str.join('', row), rowList)
		if top:
			widthStr = 'width:100%;'
		else:
			widthStr = ''
		self.table += '<table style="%s" border="1">\n%s</table>' % (widthStr, str.join('', rows))

	def __str__(self):
		return self.table

class CPWebserver(GUI):
	def __init__(self, jobCycle, jobMgr, task):
		GUI.__init__(self, jobCycle, jobMgr, task)
		self.counter = 0

	def processQueue(self, timeout):
		self.counter += 1
		utils.wait(timeout)

	def image(self):
		cherrypy.response.headers['Content-Type']= 'image/png'
		nodes = ["MetadataSplitter", "RunSplitter"]
		edges = [("MetadataSplitter", "RunSplitter")]
		nodeStr = str.join('', map(lambda x: '%s [label="%s", fillcolor="/set312/1", style="filled"]\n' % (x, x), nodes))
		edgeStr = str.join('', map(lambda x: '%s -> %s' % x, edges))
		inp = "digraph mygraph { overlap=False; ranksep=1.5; %s; %s; }" % (nodeStr, edgeStr)

		fd, fn = tempfile.mkstemp()
		os.fdopen(fd, 'w').write(inp)
		proc = utils.LoggedProcess('neato', '%s -Tpng' % fn)
		result = proc.getOutput()
		utils.removeFiles([fn])
		return result
	image.exposed = True

	def jobs(self, *args, **kw):
		result = '<body>'
		result += str(CPProgressBar(0, min(100, self.counter), 100, 300))
		if 'job' in kw:
			jobNum = int(kw['job'])
			info = self.task.getJobConfig(jobNum)
			result += str(TabularHTML(zip(sorted(info), sorted(info)), [info], top = False))
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
				'state': lambda s: Job.states[s],
				'submitted': fmtTime
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

	def run(self):
		basic_auth = {'tools.auth_basic.on': True, 'tools.auth_basic.realm': 'earth',
			'tools.auth_basic.checkpassword': cherrypy.lib.auth_basic.checkpassword_dict({'user' : '123'})}
		cherrypy.log.screen = False
		cherrypy.engine.autoreload.unsubscribe()
		cherrypy.server.socket_port = 12345
		cherrypy.tree.mount(self, '/', {'/' : basic_auth})
		cherrypy.engine.start()
		self.jobCycle(wait = self.processQueue)
		cherrypy.engine.exit()
		cherrypy.server.stop()
