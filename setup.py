#!/usr/bin/env python
import os, sys
from setuptools import setup, find_packages

fp = open(os.path.join(os.path.dirname(__file__), 'packages', 'grid_control', '__init__.py'))
version = None
try:
	for line in fp.readlines():
		if line.startswith('__version__'):
			version = eval(line.split('=')[1])
finally:
	fp.close()
assert(version)

packages = []
for pkg in find_packages('packages'):
	if (pkg in ['grid_control_gui.xmpp']) or pkg.startswith('requests'):
		continue
	packages.append(pkg)

fp = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = fp.read()
fp.close()

py_modules = ['gcSettings', 'gcTool', 'python_compat', 'python_compat_popen2']
if sys.version_info[0] < 3:
	py_modules.extend(['python_compat_json', 'python_compat_tarfile', 'python_compat_urllib2'])

setup(
	name = 'grid-control',
	version = str.join('.', [str(int(version[:-3])), str(int(version[-3])), str(int(version[-2:]))]),
	description = 'The Swiss Army knife of job submission tools',
	long_description = long_description,
	url = 'https://github.com/grid-control/grid-control',
	author = 'Fred Stober et al.',
	author_email = 'grid-control-dev@googlegroups.com',
	license = 'License :: OSI Approved :: Apache Software License',
	platforms = ['Operating System :: OS Independent'],
	classifiers = [
		'Development Status :: 5 - Production/Stable',
		'Intended Audience :: Science/Research',
		'Topic :: Scientific/Engineering :: Information Analysis',
		'Topic :: System :: Clustering',
		'Topic :: System :: Distributed Computing',
		'License :: OSI Approved :: Apache Software License',
		'Programming Language :: Python :: 2',
		'Programming Language :: Python :: 2.3',
		'Programming Language :: Python :: 2.4',
		'Programming Language :: Python :: 2.5',
		'Programming Language :: Python :: 2.6',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 3',
		'Programming Language :: Python :: 3.0',
		'Programming Language :: Python :: 3.1',
		'Programming Language :: Python :: 3.2',
		'Programming Language :: Python :: 3.3',
		'Programming Language :: Python :: 3.4',
		'Programming Language :: Python :: 3.5',
	],
	keywords = 'grid cloud batch jobs processing analysis HEP CMS',
	zip_safe = False,
	packages = packages,
	package_dir = {'':'packages'},
	include_package_data = True,
	data_files = [
		('docs', ['docs/LICENSE', 'docs/NOTICE', 'docs/documentation.conf']),
	],
	package_data = {
		'': ['.PLUGINS', 'share/*'],
	},
	scripts = ['GC', 'go.py'],
	py_modules = py_modules,
	entry_points = {
		'console_scripts': [
			'gridcontrol=gcTool:run',
		],
	},
)
