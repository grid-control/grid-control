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
from grid_control.report import ImageReport
from grid_control.utils.process_base import LocalProcess
from hpfwk import Plugin, clear_current_exception
from python_compat import BytesBuffer, any, imap, lfilter, lidfilter, lmap, set, sorted


class PluginReport(ImageReport):
	alias_list = ['plugin']

	def __init__(self, config, name, job_db, task=None):
		ImageReport.__init__(self, config, name, job_db, task)
		self._root = task

	def show_report(self, job_db, jobnum_list):
		self._show_image('plugin_graph.png',
			BytesBuffer(_get_graph_image(_get_plugin_graph(self._root))))


def _get_graph(instance, graph=None, visited=None):
	graph = graph or {}
	children = _get_instance_children(instance)
	visited = visited or set()
	visited.add(instance)

	for child in children:
		child_module = ''
		if hasattr(child, '__module__'):
			child_module = child.__module__ or ''
		child_name = ''
		if hasattr(child, '__name__'):
			child_name = child.__name__ or ''
		child_class_name = child.__class__.__name__ or ''

		if 'grid_control' not in child_module:
			continue
		if 'testsuite' in child_name:
			continue
		if not issubclass(child.__class__, Plugin):
			continue
		if child_class_name in ['instancemethod', 'function', 'type', 'method-wrapper']:
			continue
		if child in (None, True, False):
			continue
		graph.setdefault(instance, []).append(child)
		if child not in visited:
			_get_graph(child, graph, visited)

	return (graph, list(visited))


def _get_graph_image(graph_dot):
	proc = LocalProcess('twopi', '-Tpng')
	proc.stdin.write(graph_dot)
	proc.stdin.close()
	if proc.status(timeout=20) is None:
		raise Exception('Unable to render graph: ' + repr(proc))
	result = proc.stdout.read_log()
	if not result:
		raise Exception('Empty render result from ' + repr(proc))
	return result


def _get_instance_children(instance):
	children = []
	for attr in dir(instance):
		try:
			child = getattr(instance, attr)
			try:
				children.extend(child)
				children.extend(child.values())
			except Exception:
				clear_current_exception()
				children.append(child)
		except Exception:
			clear_current_exception()
	return children


def _get_node_color(instance, color_map):
	cnum = color_map.setdefault(_get_node_parent(instance.__class__),
		max(list(color_map.values()) + [0]) + 1)
	return '/set312/%d' % (cnum % 12 + 1)


def _get_node_label(instance):
	names = [instance.__class__.__name__, repr(instance)]
	if hasattr(instance.__class__, 'alias_list'):
		names.extend(lidfilter(instance.__class__.get_class_name_list()))
	result = sorted(names, key=len)[0]
	if isinstance(instance, NamedPlugin):
		if instance.get_object_name().lower() != instance.__class__.__name__.lower():
			result += ' (%s)' % instance.get_object_name()

	prefix = ''
	if hasattr(instance.__class__, 'config_tag_name'):
		prefix = '%s:' % instance.config_tag_name
	else:
		base_blacklist = ['ConfigurablePlugin', 'NamedPlugin', 'Plugin', 'object']
		base_list = lmap(lambda cls: cls.__name__.split('.')[-1], instance.__class__.iter_class_bases())
		base_list = lfilter(lambda x: x not in base_blacklist, base_list)
		prefix = '%s:' % base_list[-1]
	return prefix + result


def _get_node_name(instance, node_names):
	return node_names.setdefault(instance, instance.__class__.__name__ + '_%03d' % len(node_names))


def _get_node_parent(cls):
	cls_old = None
	base_cls_list = [Plugin, ConfigurablePlugin, NamedPlugin]
	while True:
		if (cls == cls_old) or any(imap(cls.__bases__.__contains__, base_cls_list)):
			break
		try:
			cls = cls.__bases__[0]
		except Exception:
			clear_current_exception()
			break
	return cls


def _get_plugin_graph(root):
	(graph, node_list) = _get_graph(root)

	# Process nodes
	node_str_list = []
	map_node2name = {}
	map_node2color = {}
	for node in sorted(node_list, key=lambda x: x.__class__.__name__):
		node_props = {
			'label': '"%s"' % _get_node_label(node),
			'fillcolor': '"%s"' % _get_node_color(node, map_node2color),
			'style': '"filled"',
		}
		if node == root:
			node_props['root'] = 'True'
		node_prop_str = str.join('; ', imap(lambda key: '%s = %s' % (key, node_props[key]), node_props))
		node_str_list.append('%s [%s];\n' % (_get_node_name(node, map_node2name), node_prop_str))

	# Process edges
	edge_str_list = []
	for entry in sorted(graph, key=lambda x: x.__class__.__name__):
		for child in sorted(set(graph[entry]), key=lambda x: x.__class__.__name__):
			edge_str_list.append('%s -> %s;\n' % (_get_node_name(entry, map_node2name),
				_get_node_name(child, map_node2name)))

	cluster_str_list = []

	dot_header = 'digraph mygraph {\nmargin=0;\nedge [len=2];\noverlap=compress;splines=True;\n'
	dot_format_string_list = [dot_header] + node_str_list + cluster_str_list + edge_str_list + ['}\n']
	return str.join('', dot_format_string_list)
