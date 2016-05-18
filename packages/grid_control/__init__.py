# | Copyright 2007-2016 Karlsruhe Institute of Technology
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

def initGC():
	import os, sys
	basePath = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	sys.path.insert(1, basePath) # packages bundled with grid-control have priority
	os.environ['GC_PACKAGES_PATH'] = basePath # Store grid-control base path in enviroment variable
	from hpfwk import init_hpf_plugins
	from grid_control.logging_setup import logging_defaults
	init_hpf_plugins(basePath)
	logging_defaults()
initGC()

__version__ = '$Revision: 1864$'[11:-1]
