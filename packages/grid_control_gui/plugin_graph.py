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

from grid_control.gc_plugin import ConfigurablePlugin, NamedPlugin
from grid_control.utils.process_base import LocalProcess
from hpfwk import Plugin, clear_current_exception
from python_compat import any, imap, md5_hex, set, sorted

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
			if child in (None, True, False):
				continue
			graph.setdefault(instance, []).append(child)
			if child not in visited:
				visited.add(child)
				getGraph(child, graph, visited)
		except Exception:
			clear_current_exception()
	return graph


def getNodeName(instance):
	return instance.__class__.__name__ + '_' + md5_hex(repr(hash(instance)))


def getNodeLabel(instance):
	result = instance.__class__.__name__
	if isinstance(instance, NamedPlugin):
		if instance.getObjectName().lower() != instance.__class__.__name__.lower():
			result += ' (%s)' % instance.getObjectName()
	return result


def getNodeParent(cls):
	cls_old = None
	while True:
		if (cls == cls_old) or any(imap(lambda x: x in cls.__bases__, [Plugin, ConfigurablePlugin, NamedPlugin])):
			break
		try:
			cls = cls.__bases__[0]
		except Exception:
			break
	return cls


def getNodeColor(instance, color_map):
	cnum = color_map.setdefault(getNodeParent(instance.__class__), max(color_map.values() + [0]) + 1)
	return '/set312/%d' % (cnum % 12 + 1)


def get_workflow_graph(workflow):
	graph = getGraph(workflow)
	classCluster = {}
	for entry in graph:
		classCluster.setdefault(getNodeParent(entry.__class__), []).append(entry)
	clusters = ''

	globalNodes = []
	colors = {}
	for (cluster_id, classClusterEntries) in enumerate(classCluster.values()):
		if len(classClusterEntries) == 1:
			globalNodes.append(classClusterEntries[0])
		clusters += 'subgraph cluster_%d {' % cluster_id
		for node in classClusterEntries:
			clusters += '%s [label="%s", fillcolor="%s", style="filled"];\n' % (getNodeName(node), getNodeLabel(node), getNodeColor(node, colors))
		clusters += '}\n'

	edgeStr = ''
	for entry in sorted(graph, key = lambda x: x.__class__.__name__):
		for child in sorted(set(graph[entry]), key = lambda x: x.__class__.__name__):
			edgeStr += '%s -> %s;\n' % (getNodeName(entry), getNodeName(child))
	header = 'digraph mygraph {\nmargin=0;\noverlap=scale;splines=True;\n'
	footer = '}\n'
	return header + clusters + edgeStr + footer


def get_graph_image(graph_dot):
	proc = LocalProcess('twopi', '-Tpng')
	proc.stdin.write(graph_dot)
	proc.stdin.close()
	if proc.status(timeout = 20) is None:
		return 'Unable to render graph!'
	return proc.stdout.read_log() or 'Empty render result!'
