#-#  Copyright 2014-2016 Karlsruhe Institute of Technology
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

if __name__ == '__main__':
	import os, sys, random
	from python_compat import sorted, set
	blacklist = ['./requests/packages/chardet/chardetect.py']
	# import everything
	def recurse(root):
		tmp = root.lstrip('./').split('/')
		files = os.listdir(root)
		random.shuffle(files)
		for entry in filter(lambda x: x != __file__, files):
			path = os.path.join(root, entry)
			if entry.startswith('.') or entry.endswith('pyc') or entry.startswith('__') or path in blacklist:
				continue
			if os.path.isdir(path) and os.path.exists(os.path.join(path, '__init__.py')):
				if tmp == ['']:
					yield('import %s' % (entry))
				else:
					yield('from %s import %s' % (str.join('.', tmp), entry))
				for x in recurse(path):
					yield x
			elif os.path.isfile(path) and path.endswith('.py'):
				entry = entry.replace('.py', '')
				if tmp == ['']:
					yield('from %s import *' % (entry))
				else:
					yield('from %s.%s import *' % (str.join('.', tmp), entry))
	sys.path.append(os.path.dirname(__file__))

	def sc(x, y):
		try:
			return issubclass(x, y)
		except Exception:
			pass
		return None

	clsList = []
	from hpfwk import Plugin, NamedPlugin

	for imp in recurse('.'):
		try:
			exec(imp)
			str = __builtins__.str # undo unicode magic by externals
			clsList.extend(filter(lambda x: sc(x, Plugin), map(eval, list(dir()))))
		except Exception:
			raise
			print('Unable to exec "%s"!' % imp)

	topClasses = [Plugin, NamedPlugin]
	def getBaseNames(cls):
		for topClass in topClasses:
			if (topClass in cls.__bases__) and (cls not in topClasses):
				return ['%s.%s' % (cls.__module__, cls.__name__)]
		result = []
		for clsBase in cls.__bases__:
			result.extend(getBaseNames(clsBase))
		return result

	packages = {}
	for cls in filter(getBaseNames, set(clsList)):
		packages.setdefault(cls.__module__.split('.')[0], {}).setdefault(str.join(';', getBaseNames(cls)), []).append(cls)

	for package in packages:
		output = []
		for baseClass in sorted(packages[package]):
			outputLine = '%s:\n' % baseClass
			for cls in sorted(packages[package][baseClass], key = lambda x: (x.__module__, x.__name__)):
				if cls not in topClasses:
					outputLine += '%s\t%s\n' % (cls.__module__, str.join(' ', cls.getClassNames()))
			output.append(outputLine)
		open(os.path.join(package, '.PLUGINS'), 'wb').write(str.join('\n', output))
