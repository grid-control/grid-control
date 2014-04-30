#-#  Copyright 2010-2014 Karlsruhe Institute of Technology
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

def initGC():
	import os, sys
	sys.path.insert(1, os.path.dirname(__file__))
	import grid_control.utils
	sys.path.append(grid_control.utils.cleanPath(os.getcwd()))

	# Package discovery
	for pkgName in filter(lambda x: x != 'grid_control', os.listdir(os.path.dirname(__file__))):
		if os.path.exists(os.path.join(os.path.dirname(__file__), pkgName, '__init__.py')):
			grid_control.abstract.LoadableObject.pkgPaths.append(pkgName)

	for pkgName in ['grid_control'] + grid_control.abstract.LoadableObject.pkgPaths:
		sys.path.insert(2, os.path.join(os.path.dirname(__file__), pkgName))
		yield pkgName

for pkgName in initGC():
	exec('from %s import *' % pkgName)
