#!/usr/bin/env python
# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import sys
from gc_scripts import Plugin, ScriptOptions, display_plugin_list, get_plugin_list
from python_compat import lidfilter, lmap


def _main():
	parser = ScriptOptions(usage='%s [OPTIONS] <BasePlugin>')
	parser.add_bool(None, 'a', 'show_all', default=False, help='Show plugins without user alias')
	parser.add_bool(None, 'p', 'parents', default=False, help='Show plugin parents')
	parser.add_bool(None, 'c', 'children', default=False, help='Show plugin children')
	options = parser.script_parse()
	if len(options.args) != 1:
		parser.exit_with_usage()
	pname = options.args[0]
	if options.opts.parents:
		def _get_cls_info(cls):
			return {'Name': cls.__name__, 'Alias': str.join(', ', lidfilter(cls.get_class_name_list()[1:]))}
		display_plugin_list(lmap(_get_cls_info, Plugin.get_class(pname).iter_class_bases()),
			show_all=True, sort_key=None, title='Parents of plugin %r' % pname)
	else:
		sort_key = 'Name'
		if options.opts.children:
			sort_key = 'Inherit'
		display_plugin_list(get_plugin_list(pname, inherit_prefix=options.opts.children),
			show_all=options.opts.children or options.opts.show_all,
			sort_key=sort_key, title='Available plugins of type %r' % pname)


if __name__ == '__main__':
	sys.exit(_main())
