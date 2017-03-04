#!/usr/bin/env python
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

import os, sys


def main():
	try:  # try to load the globally installed grid_control_api module
		api_installed = __import__('grid_control_api')
		del sys.modules['grid_control_api']  # and remove it to load the selected one
	except Exception:
		api_installed = None
	sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], 'packages')))
	__import__('hpfwk').clear_current_exception()  # cleanup after failed grid_control_api import
	api_current = __import__('grid_control_api')
	do_install_check = (os.environ.get('GC_INSTALL_CHECK', 'true').lower() == 'true')
	if do_install_check and ((api_installed or api_current) != api_current):
		sys.stdout.write('Using the grid-control installation at %s\n' % sys.path[0])
	api_current.gc_run()

main()
