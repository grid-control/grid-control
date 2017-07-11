# | Copyright 2007-2017 Karlsruhe Institute of Technology
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

__version__ = '1.9.82'


def _init_grid_control():
	if not _init_grid_control.flag:
		_init_grid_control.flag = True
		import os, sys
		packages_base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
		sys.path.insert(1, packages_base_path)  # packages bundled with grid-control have priority
		os.environ['GC_PACKAGES_PATH'] = packages_base_path  # Store grid-control base path

		from hpfwk import init_hpf_plugins
		package_name_list = os.listdir(packages_base_path)
		package_name_list.sort()
		for package_name in package_name_list:
			init_hpf_plugins(os.path.join(packages_base_path, package_name))

		from grid_control.logging_setup import logging_defaults
		logging_defaults()
_init_grid_control.flag = False  # <global-state>

_init_grid_control()
