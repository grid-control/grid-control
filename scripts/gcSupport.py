#!/usr/bin/env python
# | Copyright 2009-2016 Karlsruhe Institute of Technology
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

import os, sys, time, fcntl, logging


# add python subdirectory from where exec was started to search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'packages')))

from grid_control import utils
from grid_control.config import create_config
from grid_control.job_db import Job, JobClass
from grid_control.job_selector import ClassSelector, JobSelector
from grid_control.logging_setup import LogLevelEnum, parse_logging_args
from grid_control.output_processor import FileInfoProcessor, JobInfoProcessor, JobResult
from grid_control.utils.activity import Activity
from grid_control.utils.cmd_options import Options
from hpfwk import Plugin, clear_current_exception
from python_compat import ifilter, imap, lmap, sorted, tarfile


def scriptOptions(parser, args = None, arg_keys = None):
	parser.add_bool(None, ' ', 'parseable', default = False, help = 'Output tabular data in parseable format')
	parser.add_bool(None, ' ', 'pivot',     default = False, help = 'Output pivoted tabular data')
	parser.add_text(None, ' ', 'textwidth', default = 100,   help = 'Output tabular data with selected width')
	parser.add_accu(None, 'v', 'verbose',   default = 0,     help = 'Increase verbosity')
	parser.add_list(None, ' ', 'logging',                    help = 'Increase verbosity')
	(opts, args, config_dict) = parser.parse(args, arg_keys)
	logging.getLogger().setLevel(logging.DEFAULT - opts.verbose)
	for (logger_name, logger_level) in parse_logging_args(opts.logging):
		logging.getLogger(logger_name).setLevel(LogLevelEnum.str2enum(logger_level))
	if opts.parseable:
		utils.display_table.mode = 'parseable'
	elif opts.pivot:
		utils.display_table.mode = 'longlist'
	utils.display_table.wraplen = int(opts.textwidth)
	return utils.Result(opts = opts, args = args, config_dict = config_dict, parser = parser)


def getConfig(config_file = None, config_dict = None, section = None, additional = None):
	if config_dict and section:
		config_dict = {section: config_dict}
	config = create_config(config_file, config_dict, use_default_files = True, additional = additional)
	if section:
		return config.change_view(add_sections = [section])
	return config


class FileMutex:
	def __init__(self, lockfile):
		first = time.time()
		self._lockfile = lockfile
		while os.path.exists(self._lockfile):
			if first and (time.time() - first > 10):
				logging.info('Trying to aquire lock file %s ...', lockfile)
				first = False
			time.sleep(0.2)
		self._fd = open(self._lockfile, 'w')
		fcntl.flock(self._fd, fcntl.LOCK_EX)

	def release(self):
		if self._fd:
			fcntl.flock(self._fd, fcntl.LOCK_UN)
			self._fd.close()
			self._fd = None
		try:
			if os.path.exists(self._lockfile):
				os.unlink(self._lockfile)
		except Exception:
			clear_current_exception()

	def __del__(self):
		self.release()

def initGC(args):
	if len(args) > 0:
		config = getConfig(args[0])
		userSelector = None
		if len(args) != 1:
			userSelector = JobSelector.create(args[1])
		return (config, Plugin.create_instance('TextFileJobDB', config, job_selector = userSelector))
	sys.stderr.write('Syntax: %s <config file> [<job id>, ...]\n\n' % sys.argv[0])
	sys.exit(os.EX_USAGE)


def getCMSSWInfo(tarPath):
	import xml.dom.minidom
	# Read framework report files to get number of events
	tarFile = tarfile.open(tarPath, 'r:gz')
	fwkReports = ifilter(lambda x: os.path.basename(x.name) == 'report.xml', tarFile.getmembers())
	for fwkReport in imap(tarFile.extractfile, fwkReports):
		try:
			yield xml.dom.minidom.parse(fwkReport)
		except Exception:
			logging.exception('Error while parsing %s', tarPath)
			raise


def prettySize(size):
	suffixes = [('B', 2**10), ('K', 2**20), ('M', 2**30), ('G', 2**40), ('T', 2**50)]
	for suf, lim in suffixes:
		if size > lim:
			continue
		else:
			return str(round(size / float(lim / 2**10), 2)) + suf

def get_pluginList(pluginName):
	aliasDict = {}
	cls = Plugin.get_class(pluginName)
	for entry in cls.get_class_info_list():
		depth = entry.pop('depth', 0)
		(alias, name) = entry.popitem()
		aliasDict.setdefault(name, []).append((depth, alias))
	aliasDict.pop(pluginName)

	tableList = []
	for name in aliasDict:
		# sorted by length of name and depth
		by_len_depth = sorted(aliasDict[name], key = lambda d_a: (-len(d_a[1]), d_a[0]))
		# sorted by depth and name
		by_depth_name = sorted(aliasDict[name], key = lambda d_a: (d_a[0], d_a[1]))
		new_name = by_len_depth.pop()[1]
		aliasList = lmap(lambda d_a: d_a[1], by_depth_name)
		aliasList.remove(new_name)
		entry = {'Name': new_name, 'Alias': str.join(', ', aliasList)}
		if ('Multi' not in name) and ('Base' not in name):
			tableList.append(entry)
	return tableList

def displayPluginList(clsList):
	header = [('Name', 'Name')]
	fmt_string = 'l'
	for entry in clsList:
		if entry['Alias']:
			header.append(('Alias', 'Alternate names'))
			fmt_string = 'rl'
			break
	utils.display_table(header, sorted(clsList, key = lambda x: x['Name'].lower()), fmt_string = fmt_string)

__all__ = ['Activity', 'ClassSelector', 'displayPluginList', 'FileInfoProcessor', 'FileMutex',
	'getCMSSWInfo', 'getConfig', 'get_pluginList', 'initGC', 'Job', 'JobClass', 'JobInfoProcessor',
	'JobResult', 'JobSelector', 'Options', 'Plugin', 'scriptOptions', 'utils']
