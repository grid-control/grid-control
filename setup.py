#!/usr/bin/env python
import os, glob
from setuptools import setup, find_packages

setup(
	name='grid-control',
	version='1.6.53',
	description='The Swiss Army knife of job submission tools',
	long_description='The Swiss Army knife of job submission tools',
	url='https://github.com/grid-control/grid-control',
	author='Fred Stober et al.',
	author_email='grid-control-dev@googlegroups.com',
	license='License :: OSI Approved :: Apache Software License',
	platforms=['Operating System :: OS Independent'],
	classifiers=[
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
	packages = find_packages('packages'),
	package_dir = {'':'packages'},
	include_package_data = True,
	data_files = [
		('docs', ['docs/LICENSE', 'docs/NOTICE', 'docs/documentation.conf']),
	],
	package_data = {
		'': ['.PLUGINS', 'share/*'],
	},
	scripts = ['GC', 'go.py'],
	py_modules = ['gcSettings', 'gcTool', 'python_compat',
		'python_compat_json', 'python_compat_popen2',
		'python_compat_tarfile', 'python_compat_urllib2'],
	entry_points = {
		'console_scripts': [
			'gridcontrol=gcTool:run',
		],
	},
)
