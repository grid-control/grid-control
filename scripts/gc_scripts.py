#!/usr/bin/env python
# | Copyright 2017 Karlsruhe Institute of Technology
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
from grid_control.job_db import Job, JobClass
from grid_control.job_selector import ClassSelector, JobSelector, TaskNeededException
from grid_control.logging_setup import LogLevelEnum, parse_logging_args
from grid_control.output_processor import FileInfo, FileInfoProcessor
from grid_control.utils.activity import Activity, ProgressActivity
from grid_control.utils.cmd_options import Options
from grid_control_api import gc_create_config, gc_create_workflow
from hpfwk import Plugin, clear_current_exception
from python_compat import ifilter, imap, lmap, sorted, tarfile


def display_plugin_list(cls_list, sort=True, title=None):
	header = [('Name', 'Name')]
	fmt_string = 'l'
	for entry in cls_list:
		if entry['Alias']:
			header.append(('Alias', 'Alternate names'))
			fmt_string = 'rl'
			break
	if sort:
		cls_list = sorted(cls_list, key=lambda x: x['Name'].lower())
	utils.display_table(header, cls_list, fmt_string=fmt_string, title=title)


def display_plugin_list_for(cls_name, sort=True, title=None):
	display_plugin_list(get_plugin_list(cls_name), sort=sort, title=title)


def get_cmssw_info(tar_fn):
	import xml.dom.minidom
	# Read framework report files to get number of events
	cmssw_tar = tarfile.open(tar_fn, 'r:gz')
	fwk_report_list = ifilter(lambda x: os.path.basename(x.name) == 'report.xml',
		cmssw_tar.getmembers())
	for fwk_report_fn in imap(cmssw_tar.extractfile, fwk_report_list):
		try:
			yield xml.dom.minidom.parse(fwk_report_fn)
		except Exception:
			logging.exception('Error while parsing %s', tar_fn)
			raise


def get_plugin_list(pname):
	alias_dict = {}
	cls = Plugin.get_class(pname)
	for entry in cls.get_class_info_list():
		depth = entry.pop('depth', 0)
		(alias, name) = entry.popitem()
		alias_dict.setdefault(name, []).append((depth, alias))
	alias_dict.pop(pname, None)

	table_list = []
	for name in alias_dict:
		# sorted by length of name and depth
		by_len_depth = sorted(alias_dict[name], key=lambda d_a: (-len(d_a[1]), d_a[0]))
		# sorted by depth and name
		by_depth_name = sorted(alias_dict[name], key=lambda d_a: (d_a[0], d_a[1]))
		new_name = by_len_depth.pop()[1]
		alias_list = lmap(lambda d_a: d_a[1], by_depth_name)
		alias_list.remove(new_name)
		entry = {'Name': new_name, 'Alias': str.join(', ', alias_list)}
		if ('Multi' not in name) and ('Base' not in name):
			table_list.append(entry)
	return table_list


def get_script_object(config_file, job_selector_str, only_success=False, require_task=False):
	config = gc_create_config(config_file=config_file, load_only_old_config=True)
	(task, job_selector) = _get_job_selector_and_task(config, job_selector_str, require_task)
	if only_success:
		success_selector = ClassSelector(JobClass.SUCCESS)
		if job_selector:
			job_selector = JobSelector.create_instance('AndJobSelector', success_selector, job_selector)
		else:
			job_selector = success_selector
	new_config = gc_create_config(config_file=config_file)
	jobs_config = new_config.change_view(set_sections=['jobs'])
	job_db = jobs_config.get_plugin('job database', 'TextFileJobDB', cls='JobDB',
		pkwargs={'job_selector': job_selector}, on_change=None)

	class ScriptObject(object):
		def __init__(self, config, new_config, task, job_db):
			(self.config, self.new_config) = (config, new_config)
			(self.task, self.job_db) = (task, job_db)

	return ScriptObject(config, new_config, task, job_db)


def get_script_object_cmdline(args, only_success=False, require_task=False):
	user_selector_str = None
	if len(args) > 1:
		user_selector_str = args[1]
	return get_script_object(args[0], user_selector_str)


def iter_jobnum_output_dn(output_dn, jobnum_list):
	if jobnum_list:
		jobnum_list.sort()
		progress = ProgressActivity('Processing output directory', jobnum_list[-1])
		for jobnum in jobnum_list:
			progress.update_progress(jobnum)
			yield (jobnum, os.path.join(output_dn, 'job_%d' % jobnum))
		progress.finish()


def iter_output_files(output_dn):
	fip = FileInfoProcessor()
	for fi in fip.process(output_dn):
		yield fi


def str_file_size(size):
	suffixes = [('B', 2**10), ('K', 2**20), ('M', 2**30), ('G', 2**40), ('T', 2**50)]
	for suf, lim in suffixes:
		if size > lim:
			continue
		else:
			return str(round(size / float(lim / 2**10), 2)) + suf


class FileMutex(object):
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

	def __del__(self):
		self.release()

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


class ScriptOptions(Options):
	def exit_with_usage(self, usage=None, msg=None):
		utils.exit_with_usage(usage or self.usage(), msg)

	def script_parse(self, arg_keys=None, verbose_short='v'):
		self.add_bool(None, None, 'parseable', default=False,
			help='Output tabular data in parseable format')
		self.add_bool(None, None, 'pivot', default=False,
			help='Output pivoted tabular data')
		self.add_text(None, None, 'textwidth', default=100,
			help='Output tabular data with selected width')
		self.add_accu(None, verbose_short, 'verbose', default=0, help='Increase verbosity')
		self.add_list(None, None, 'logging', help='Increase verbosity')
		(opts, args, config_dict) = self.parse(arg_keys=arg_keys)
		logging.getLogger().setLevel(logging.DEFAULT - opts.verbose)
		for (logger_name, logger_level) in parse_logging_args(opts.logging):
			logging.getLogger(logger_name).setLevel(LogLevelEnum.str2enum(logger_level))
		if opts.parseable:
			utils.display_table.mode = 'parseable'
		elif opts.pivot:
			utils.display_table.mode = 'longlist'
		utils.display_table.wraplen = int(opts.textwidth)
		return utils.Result(opts=opts, args=args, config_dict=config_dict, parser=self)


def _get_job_selector_and_task(config, job_selector_str, require_task):
	if not require_task:
		try:  # try to build job selector without task
			return (None, JobSelector.create(job_selector_str))
		except TaskNeededException:
			clear_current_exception()
	task = gc_create_workflow(config, abort='task').task
	return (task, JobSelector.create(job_selector_str, task=task))


__all__ = ['Activity', 'ClassSelector', 'display_plugin_list', 'display_plugin_list_for',
	'FileInfo', 'FileInfoProcessor', 'FileMutex', 'gc_create_config', 'get_cmssw_info',
	'get_plugin_list', 'get_script_object', 'get_script_object_cmdline', 'iter_jobnum_output_dn',
	'iter_output_files', 'Job', 'JobClass', 'JobSelector', 'Plugin', 'ScriptOptions', 'utils']
