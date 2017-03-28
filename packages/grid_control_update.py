#!/usr/bin/env python
# | Copyright 2014-2017 Karlsruhe Institute of Technology
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

import os, sys


def update_plugin_files():
	base_dir = os.path.abspath(os.path.dirname(__file__))
	sys.path.append(base_dir)
	from hpfwk.hpf_plugin import create_plugin_file

	def _select(path):
		for pat in ['/share', '_compat_', '/requests', '/xmpp']:
			if pat in path:
				return False
		return True

	package_list = os.listdir(base_dir)
	package_list.sort()
	for package in package_list:
		package = os.path.abspath(os.path.join(base_dir, package))
		if os.path.isdir(package):
			create_plugin_file(package, _select)


if __name__ == '__main__':
	update_plugin_files()
