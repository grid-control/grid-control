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

import os, sys, time, random, logging
from gc_scripts import FileInfo, FileInfoProcessor, Job, Plugin, ScriptOptions, get_script_object_cmdline, str_file_size, utils  # pylint:disable=line-too-long
from grid_control.backends.storage import se_copy, se_exists, se_mkdir, se_rm
from grid_control.utils.data_structures import make_enum
from grid_control.utils.thread_tools import GCEvent, start_daemon
from hpfwk import clear_current_exception, get_thread_state
from python_compat import all, any, imap, lfilter, md5, sorted


try:
	from grid_control_gui.ansi import Console
except Exception:
	clear_current_exception()
	Console = None  # pylint:disable=invalid-name
try:
	from grid_control_gui.report_textbar import BasicProgressBar
except Exception:
	clear_current_exception()
	BasicProgressBar = None  # pylint:disable=invalid-name


JobDownloadStatus = make_enum(['JOB_OK', 'JOB_ALREADY', 'JOB_NO_OUTPUT',  # pylint:disable=invalid-name
	'JOB_PROCESSING', 'JOB_FAILED', 'JOB_RETRY', 'JOB_INCOMPLETE'])
FileDownloadStatus = make_enum(['FILE_OK', 'FILE_EXISTS', 'FILE_TIMEOUT',  # pylint:disable=invalid-name
	'FILE_SE_BLACKLIST', 'FILE_HASH_FAILED', 'FILE_TRANSFER_FAILED', 'FILE_MKDIR_FAILED'])


def check_token(token):
	if time.time() - check_token.last_check > 10:
		check_token.last_check = time.time()
		if not token.can_submit(20 * 60, True):
			sys.stdout.flush()
			sys.stderr.write('\n\nPlease renew access token!\n')
			sys.exit(os.EX_UNAVAILABLE)
check_token.last_check = 0


def check_hash(opts, local_se_path, fi_idx, fi, job_download_display):
	# Verify => compute md5hash
	if opts.verify_md5:
		try:
			local_hash = md5sum(local_se_path.replace('file://', ''))
		except KeyboardInterrupt:
			raise
		except Exception:
			clear_current_exception()
			local_hash = None
		job_download_display.check_hash(fi_idx, local_hash)
		if fi[FileInfo.Hash] != local_hash:
			return FileDownloadStatus.FILE_HASH_FAILED
	else:
		job_download_display.check_hash(fi_idx)
	return FileDownloadStatus.FILE_OK


def cleanup_files(opts, fi_list, download_failed, job_download_display):
	job_download_display.cleanup_files()
	for (fi_idx, fi) in enumerate(fi_list):
		def _delete(file_se_path, where, what):
			if se_exists(file_se_path).status(timeout=10) == 0:
				job_download_display.cleanup_files(fi_idx,
					'Deleting file %s from %s...' % (fi[FileInfo.NameDest], where))
				delete_se_path(file_se_path, what)

		(source_se_path, target_se_path, local_se_path) = get_fi_path_tuple(opts, fi)
		# Remove downloaded files in case of failure
		if (download_failed and opts.rm_local_fail) or (not download_failed and opts.rm_local_ok):
			_delete(target_se_path, 'target', 'target file')
		# Remove SE files in case of failure
		if (download_failed and opts.rm_se_fail) or (not download_failed and opts.rm_se_ok):
			_delete(source_se_path, 'source', 'source file')
		# Always clean up local tmp files
		if target_se_path != local_se_path:
			_delete(local_se_path, 'local', 'local tmp file')
	job_download_display.cleanup_files(-1)


def delete_se_path(se_path, msg):
	rm_proc = se_rm(se_path)
	if rm_proc.status(timeout=60) != 0:
		logging.critical('\t\tUnable to remove %s!', msg)
		logging.critical('%s\n%s\n', rm_proc.stdout.read(timeout=0), rm_proc.stderr.read(timeout=0))


def display_download_result(download_result_dict, jobnum_list):
	def _iter_download_results(cls):
		for stat in sorted(download_result_dict, key=download_result_dict.get):
			if download_result_dict[stat] and (stat in cls.enum_value_list):
				yield {0: cls.enum2str(stat), 1: download_result_dict[stat]}
		yield '='
		yield {0: 'Total', 1: len(jobnum_list)}

	if download_result_dict:
		utils.display_table([(0, 'Status'), (1, '')],
			_iter_download_results(JobDownloadStatus), title='Job status overview')
		utils.display_table([(0, 'Status'), (1, '')],
			_iter_download_results(FileDownloadStatus), title='File status overview')


def download_multithreaded(opts, work_dn, _inc_download_result, job_db, token, jobnum_list):
	(thread_display_list, error_msg_list, jobnum_list_todo) = ([], [], list(jobnum_list))
	jobnum_list_todo.reverse()

	Console.setscrreg(3 * opts.threads)
	while True:
		# remove finished transfers
		thread_display_list = lfilter(lambda thread_display: get_thread_state(thread_display[0]),
			thread_display_list)
		# add new transfers
		while len(thread_display_list) < opts.threads and len(jobnum_list_todo):
			jobnum = jobnum_list_todo.pop()
			job_download_display = ThreadedJobDownloadDisplay(opts, jobnum, error_msg_list)
			download_thread = start_daemon('Download %s' % jobnum, process_job_files,
				opts, work_dn, _inc_download_result, job_db, token, jobnum, job_download_display)
			thread_display_list.append((download_thread, job_download_display))
		# display transfers
		Console.erase()
		Console.move(0)
		for (_, job_download_display) in thread_display_list:
			sys.stdout.write(job_download_display.get_display_str() + '\n')
		Console.move(3 * opts.threads + 1)
		sys.stdout.flush()
		if len(thread_display_list) == 0:
			break
		check_token(token)
		time.sleep(0.1)


def download_single_file(opts, job_download_display, jobnum, fi_idx, fi):
	(source_se_path, target_se_path, local_se_path) = get_fi_path_tuple(opts, fi)

	# Copy files to local folder
	if opts.select_se:
		if not any(imap(lambda se_name: se_name in fi[FileInfo.Path], opts.select_se)):
			job_download_display.error_file(fi_idx, 'skipping file on blacklisted SE!')
			return FileDownloadStatus.FILE_SE_BLACKLIST
	if opts.skip_existing and (se_exists(target_se_path).status(timeout=10) == 0):
		job_download_display.error_file(fi_idx, 'skipping already existing file!')
		return FileDownloadStatus.FILE_EXISTS
	try:
		if se_exists(os.path.dirname(target_se_path)).status(timeout=10) != 0:
			se_mkdir(os.path.dirname(target_se_path)).status(timeout=10)
	except Exception:
		clear_current_exception()
		job_download_display.error_file(fi_idx, 'error while creating target directory!')
		return FileDownloadStatus.FILE_MKDIR_FAILED

	if 'file://' in target_se_path:
		local_se_path = target_se_path
	download_result = download_single_file_monitored(jobnum, job_download_display,
			fi_idx, source_se_path, target_se_path, local_se_path)
	if download_result is not None:
		return download_result

	return check_hash(opts, local_se_path, fi_idx, fi, job_download_display)


def download_single_file_monitored(jobnum, job_download_display,
		fi_idx, source_se_path, target_se_path, local_fn):
	copy_timeout_event = GCEvent()
	copy_ended_event = GCEvent()
	monitor_thread = start_daemon('Download monitor %s' % jobnum, monitor_transfer_progress,
		job_download_display, fi_idx, local_fn, copy_ended_event, copy_timeout_event)

	cp_proc = se_copy(source_se_path, target_se_path, tmp=local_fn)
	while (cp_proc.status(timeout=0) is None) and not copy_timeout_event.wait(timeout=0.05):
		pass
	time.sleep(2)
	copy_ended_event.set()
	monitor_thread.join()

	if copy_timeout_event.is_set():
		cp_proc.terminate(timeout=1)
		job_download_display.error_file(fi_idx, 'Transfer timeout')
		return FileDownloadStatus.FILE_TIMEOUT
	elif cp_proc.status(timeout=0) != 0:
		job_download_display.error_file(fi_idx, 'Transfer error %s' % cp_proc.status(timeout=0))
		return FileDownloadStatus.FILE_TRANSFER_FAILED


def get_fi_path_tuple(opts, fi):
	source_se_path = os.path.join(fi[FileInfo.Path], fi[FileInfo.NameDest])
	target_se_path = os.path.join(opts.output, fi[FileInfo.NameDest])
	local_se_path = 'file://' + os.path.join(opts.tmp_dir, 'dlfs.%s' % fi[FileInfo.NameDest])
	return (source_se_path, target_se_path, local_se_path)


def get_se_host(se_path):
	return se_path.split('://')[-1].split('/')[0].split(':')[0]


def loop_download(opts, args):
	# Init everything in each loop to pick up changes
	script_obj = get_script_object_cmdline(args, only_success=True)
	token = Plugin.get_class('AccessToken').create_instance(opts.token, script_obj.new_config, 'token')
	work_dn = script_obj.config.get_work_path()

	# Create SE output dir
	if not opts.output:
		opts.output = os.path.join(work_dn, 'se_output')
	if '://' not in opts.output:
		opts.output = 'file:///%s' % os.path.abspath(opts.output)

	download_result_dict = {}

	def _inc_download_result(dstat):
		download_result_dict[dstat] = download_result_dict.get(dstat, 0) + 1

	job_db = script_obj.job_db
	jobnum_list = job_db.get_job_list()
	if opts.shuffle:
		random.shuffle(jobnum_list)
	else:
		jobnum_list.sort()

	if opts.threads:
		download_multithreaded(opts, work_dn, _inc_download_result, job_db, token, jobnum_list)
	else:
		for jobnum in jobnum_list:
			check_token(token)
			display = JobDownloadDisplay(opts, jobnum)
			process_job_files(opts, work_dn, _inc_download_result, job_db, token, jobnum, display)

	# Print overview
	display_download_result(download_result_dict, jobnum_list)
	# return True if download is finished
	num_success = sum(imap(lambda jds: download_result_dict.get(jds, 0), [
		JobDownloadStatus.JOB_OK, JobDownloadStatus.JOB_ALREADY, JobDownloadStatus.JOB_INCOMPLETE]))
	return num_success == len(jobnum_list)


def md5sum(filename):
	md5_obj = md5()
	blocksize = 4096 * 1024  # use 4M blocksize:
	fp = open(filename, 'r')
	while True:
		buffer_str = fp.read(blocksize)
		md5_obj.update(buffer_str)
		if len(buffer_str) != blocksize:
			break
	return md5_obj.hexdigest()


def monitor_transfer_progress(job_download_display, fi_idx, local_se_path,
		copy_ended_event, copy_timeout_event):
	local_fn = local_se_path.replace('file://', '')
	(file_size_cur, file_size_old) = (0, 0)
	(start_time, old_time, last_transfer_time) = (time.time(), time.time(), time.time())
	job_download_display.monitor_transfer_progress(fi_idx)
	while not copy_ended_event.wait(0.1):  # Loop until monitor lock is available
		if file_size_cur != file_size_old:
			last_transfer_time = time.time()
		if time.time() - last_transfer_time > 5 * 60:  # No size change in the last 5min!
			job_download_display.error_file(fi_idx, 'Transfer timeout!')
			copy_timeout_event.set()
			break
		if os.path.exists(local_fn):
			file_size_cur = os.path.getsize(local_fn)
			job_download_display.monitor_transfer_progress(fi_idx,
				file_size_cur, file_size_old, start_time, old_time)
			(file_size_old, old_time) = (file_size_cur, time.time())
		else:
			start_time = time.time()
	job_download_display.monitor_transfer_progress(fi_idx,
		file_size_cur, file_size_old, start_time, None)


def process_job_files(opts, work_dn, _inc_download_result,
		job_db, token, jobnum, job_download_display):
	job_obj = job_db.get_job(jobnum)
	# Only run over finished and not yet downloaded jobs
	if job_obj.state != Job.SUCCESS:
		job_download_display.error('Job has not yet finished successfully!')
		return _inc_download_result(JobDownloadStatus.JOB_PROCESSING)
	if job_obj.get('download') == 'True' and not opts.mark_ignore_dl:
		job_download_display.error('All files already downloaded!')
		return _inc_download_result(JobDownloadStatus.JOB_ALREADY)

	# Read the file hash entries from job info file
	fi_list = FileInfoProcessor().process(os.path.join(work_dn, 'output', 'job_%d' % jobnum)) or []
	job_download_display.process_job_files_begin(fi_list)
	download_failed = False
	if not fi_list:
		if opts.mark_empty_fail:
			download_failed = True
		else:
			return _inc_download_result(JobDownloadStatus.JOB_NO_OUTPUT)

	download_result_list = []
	for (fi_idx, fi) in enumerate(fi_list):
		download_result = download_single_file(opts, job_download_display, jobnum, fi_idx, fi)
		_inc_download_result(download_result)
		download_result_list.append(download_result)

	download_failed = any(imap(lambda fds: fds in download_result_list, [
		FileDownloadStatus.FILE_TIMEOUT, FileDownloadStatus.FILE_HASH_FAILED,
		FileDownloadStatus.FILE_TRANSFER_FAILED, FileDownloadStatus.FILE_MKDIR_FAILED]))
	download_success = all(imap(lambda fds: fds in [FileDownloadStatus.FILE_OK,
		FileDownloadStatus.FILE_EXISTS], download_result_list))

	# Ignore the first opts.retry number of failed jobs
	retry_count = int(job_obj.get('download attempt', 0))
	if fi_list and download_failed and opts.retry and (retry_count < int(opts.retry)):
		job_download_display.error('Download attempt #%d failed!' % (retry_count + 1))
		job_obj.set('download attempt', str(retry_count + 1))
		job_db.commit(jobnum, job_obj)
		return _inc_download_result(JobDownloadStatus.RETRY)

	cleanup_files(opts, fi_list, download_failed, job_download_display)

	if download_failed:
		_inc_download_result(JobDownloadStatus.JOB_FAILED)
		if opts.mark_fail:
			# Mark job as failed to trigger resubmission
			job_obj.state = Job.FAILED
	elif download_success:
		_inc_download_result(JobDownloadStatus.JOB_OK)
		if opts.mark_dl:
			# Mark as downloaded
			job_obj.set('download', 'True')
	else:  # eg. because of SE blacklist
		_inc_download_result(JobDownloadStatus.JOB_INCOMPLETE)

	# Save new job status infos
	job_db.commit(jobnum, job_obj)
	job_download_display.process_job_files_end()
	time.sleep(float(opts.slowdown))


class Display(object):
	def _match_result(self, result):
		if Console is None:
			if not result:
				return 'FAIL'
			return 'MATCH'
		if not result:
			return Console.fmt('FAIL', [Console.COLOR_RED])
		return Console.fmt('MATCH', [Console.COLOR_GREEN])

	def _rate(self, cur_size, ref_size, ref_time):
		return str_file_size(((cur_size - ref_size) / max(1., time.time() - ref_time))) + '/s'


class JobDownloadDisplay(Display):
	def __init__(self, opts, jobnum):
		(self._jobnum, self._fi_list) = (jobnum, [])
		(self._show_host, self._show_bar) = (opts.show_host, opts.show_bar)
		self._bar = None

	def check_hash(self, fi_idx, local_hash=None):
		remote_hash = self._fi_list[fi_idx][FileInfo.Hash]
		if local_hash:
			match_str = self._match_result(remote_hash == local_hash)
			self._write(' |    Local  hash: %s [%s]\n' % (local_hash, match_str))
		self._write(' |    Remote hash: %s\n' % remote_hash)

	def cleanup_files(self, fi_idx=None, msg=''):
		self._write(' - %s\r' % msg)

	def error(self, msg):
		self._write('Job %d: %s\n' % (self._jobnum, msg.strip()))

	def error_file(self, fi_idx, msg):
		self._write(' + File %d: %s\n' % (fi_idx, msg.strip()))

	def monitor_transfer_progress(self, fi_idx, cur_size=None, old_size=0, start_time=0, old_time=0):
		if cur_size is None:
			return self._write(' + File %d: %s\n' % (fi_idx, self._build_transfer_intro_str(fi_idx)))
		elif old_time is None:
			self._bar = None
			return self._write('\n')
		self._write(' | => %s\r' % self._build_transfer_info_str(fi_idx,
			cur_size, old_size, start_time, old_time))

	def process_job_files_begin(self, fi_list):
		self._fi_list = fi_list
		self._write('Job %d: (%s file%s)\n' % (self._jobnum, len(fi_list), ('s', '')[len(fi_list) == 1]))

	def process_job_files_end(self):
		self._write('\n')

	def _build_transfer_info_str(self, fi_idx, cur_size, old_size, start_time, old_time):
		fi = self._fi_list[fi_idx]
		output_str = ''
		if self._show_host:
			output_str += '[%s] ' % get_se_host(fi[FileInfo.Path])
		if old_time:
			if self._bar:
				self._bar.update(cur_size)
				return '%s (%7s avg.)' % (str(self._bar), self._rate(cur_size, 0, start_time))
			size_str = str_file_size(cur_size)
			output_str += '(%7s - %7s avg. - %7s inst.)' % (size_str,
				self._rate(cur_size, 0, start_time), self._rate(cur_size, old_size, old_time))
			if fi.get(FileInfo.Size) is not None:
				output_str += ' <%5.1f%%>' % (cur_size / float(fi[FileInfo.Size]) * 100)
		return output_str

	def _build_transfer_intro_str(self, fi_idx):
		fi = self._fi_list[fi_idx]
		output_str = '%s -> %s ' % (fi[FileInfo.NameLocal], fi[FileInfo.NameDest])
		if fi.get(FileInfo.Size) is not None:
			output_str += '[%s]' % str_file_size(fi.get(FileInfo.Size))
			self._bar = BasicProgressBar(value_max=fi.get(FileInfo.Size), total_width=45)
		return output_str

	def _write(self, msg):
		sys.stdout.write(msg)  # always stay in one line (except for manual newlines)


class ThreadedJobDownloadDisplay(JobDownloadDisplay):
	def __init__(self, opts, jobnum, error_msg_list):
		JobDownloadDisplay.__init__(self, opts, jobnum)
		self._output_str_list = ['', '', '']
		self._error_output = error_msg_list

	def check_hash(self, fi_idx, local_hash=None):
		remote_hash = self._fi_list[fi_idx][FileInfo.Hash]
		self._output_str_list[2] = self._match_result(remote_hash == local_hash)

	def cleanup_files(self, fi_idx=None, msg=''):
		self._output_str_list[2] = msg

	def error(self, msg):
		self._output_str_list[2] = msg
		self._error_output.append('Job %d: %s' % (self._jobnum, msg))

	def error_file(self, fi_idx, msg):
		self._output_str_list[2] = msg
		self._error_output.append('Job %d - File %d: %s' % (self._jobnum, fi_idx, msg))

	def get_display_str(self):
		return str.join('\n', imap(str.strip, imap(str, self._output_str_list)))

	def monitor_transfer_progress(self, fi_idx, cur_size=None, old_size=0, start_time=0, old_time=0):
		if fi_idx > len(self._fi_list):
			return
		intro_str = '[%s-%s] ' % (self._jobnum, fi_idx)
		if cur_size is None:
			self._output_str_list[0] = intro_str + self._build_transfer_intro_str(fi_idx)
		elif old_time is None:
			self._bar = None
		else:
			self._output_str_list[1] = intro_str + self._build_transfer_info_str(fi_idx,
				cur_size, old_size, start_time, old_time)

	def process_job_files_begin(self, fi_list):
		self._fi_list = fi_list
		self._output_str_list = ['', '', '']

	def process_job_files_end(self):
		pass


def _main():
	options = _parse_cmd_line()
	opts = options.opts

	# Disable loop mode if it is pointless
	if (opts.loop and not opts.skip_existing) and (opts.mark_ignore_dl or not opts.mark_dl):
		sys.stderr.write('Loop mode was disabled to avoid continuously downloading the same files\n')
		(opts.loop, opts.infinite) = (False, False)

	while True:
		try:
			if (loop_download(opts, options.args) or not opts.loop) and not opts.infinite:
				break
			time.sleep(60)
		except KeyboardInterrupt:
			logging.critical('\n\nDownload aborted!\n')
			sys.exit(os.EX_TEMPFAIL)


def _parse_cmd_line():
	help_msg = '\n\nDEFAULT: The default is to download the SE file and check them with MD5 hashes.'
	help_msg += '\n * In case all files are transferred sucessfully, the job is marked'
	help_msg += '\n   as already downloaded, so that the files are not copied again.'
	help_msg += '\n * Failed transfer attempts will mark the job as failed, so that it'
	help_msg += '\n   can be resubmitted.\n'
	parser = ScriptOptions(usage='%s [OPTIONS] <config file>' + help_msg)

	def _add_bool_opt(group, short_pair, option_base, help_base, default=False,
			option_prefix_pair=('', 'no'), help_prefix_pair=('', 'do not '), dest=None):
		def _create_help(idx):
			help_def = ''
			if (default and (idx == 0)) or ((not default) and (idx == 1)):
				help_def = ' [Default]'
			return help_prefix_pair[idx] + help_base + help_def

		def _create_opt(idx):
			return str.join('-', option_prefix_pair[idx].split() + option_base.split())

		parser.add_flag(group, short_pair or '  ', (_create_opt(0), _create_opt(1)),
			default=default, dest=dest, help_pair=(_create_help(0), _create_help(1)))

	_add_bool_opt(None, 'v ', 'verify-md5', default=True,
		help_base='MD5 verification of SE files', help_prefix_pair=('enable ', 'disable '))
	_add_bool_opt(None, 'l ', 'loop', default=False,
		help_base='loop over jobs until all files are successfully processed')
	_add_bool_opt(None, 'L ', 'infinite', default=False,
		help_base='process jobs in an infinite loop')
	_add_bool_opt(None, None, 'shuffle', default=False,
		help_base='shuffle download order')
	_add_bool_opt(None, None, '', default=False,
		option_prefix_pair=('skip-existing', 'overwrite'), dest='skip_existing',
		help_base='files which are already on local disk', help_prefix_pair=('skip ', 'overwrite '))

	parser.section('jobs', 'Job state / flag handling')
	_add_bool_opt('jobs', None, 'mark-dl', default=True,
		help_base='mark sucessfully downloaded jobs as such')
	_add_bool_opt('jobs', None, 'mark-dl', default=False,
		option_prefix_pair=('ignore', 'use'), dest='mark_ignore_dl',
		help_base='mark about sucessfully downloaded jobs', help_prefix_pair=('ignore ', 'use '))
	_add_bool_opt('jobs', None, 'mark-fail', default=True,
		help_base='mark jobs failing verification as such')
	_add_bool_opt('jobs', None, 'mark-empty-fail', default=False,
		help_base='mark jobs without any files as failed')

	parser.section('file', 'Local / SE file handling')
	option_help_base_list = [
		('local-ok', 'files of successful jobs in local directory'),
		('local-fail', 'files of failed jobs in local directory'),
		('se-ok', 'files of successful jobs on SE'),
		('se-fail', 'files of failed jobs on the SE'),
	]
	for (option, help_base) in option_help_base_list:
		_add_bool_opt('file', None, option, default=False, option_prefix_pair=('rm', 'keep'),
			help_base=help_base, help_prefix_pair=('remove ', 'keep '))

	parser.add_text(None, 'o', 'output', default=None,
		help='specify the local output directory')
	parser.add_text(None, 'O', 'tmp-dir', default='/tmp',
		help='specify the local tmp directory')
	parser.add_text(None, 'T', 'token', default='VomsProxy',
		help='specify the access token used to determine ability to download ' +
			'- VomsProxy or TrivialAccessToken')
	parser.add_list(None, 'S', 'select-se', default=None,
		help='specify the SE paths to process')
	parser.add_text(None, 'r', 'retry',
		help='how often should a transfer be attempted [Default: 0]')
	if Console is not None:
		parser.add_text(None, 't', 'threads', default=0,
			help='how many parallel download threads should be used to download files ' +
				'[Default: no multithreading]')
	parser.add_text(None, None, 'slowdown', default=2,
		help='specify time between downloads [Default: 2 sec]')
	parser.add_bool(None, None, 'show-host', default=False,
		help='show SE hostname during download')
	if BasicProgressBar is not None:
		parser.add_bool(None, None, 'show-bar', default=False,
			help='show progress bar during download')

	parser.section('short', 'Shortcuts')
	parser.add_fset('short', 'm', 'move',
		help='Move files from SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail ' +
			'--rm-se-fail --rm-local-fail --rm-se-ok --keep-local-ok')
	parser.add_fset('short', 'c', 'copy',
		help='Copy files from SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail ' +
			'--rm-se-fail --rm-local-fail --keep-se-ok --keep-local-ok')
	parser.add_fset('short', 'j', 'just-copy',
		help='Just copy files from SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --skip-existing --no-mark-dl --ignore-mark-dl --no-mark-fail ' +
			'--keep-se-fail --keep-local-fail --keep-se-ok --keep-local-ok')
	parser.add_fset('short', 's', 'smart-copy',
		help='Copy correct files from SE, but remember already downloaded ' +
			'files and delete corrupt files - shorthand for: '.ljust(100) + '%s',
		flag_set='--verify-md5 --mark-dl --mark-fail --rm-se-fail ' +
			'--rm-local-fail --keep-se-ok --keep-local-ok')
	parser.add_fset('short', 'V', 'just-verify',
		help='Just verify files on SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --no-mark-dl --keep-se-fail ' +
			'--rm-local-fail --keep-se-ok --rm-local-ok --ignore-mark-dl')
	parser.add_fset('short', 'D', 'just-delete',
		help='Just delete all finished files on SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--skip-existing --rm-se-fail --rm-se-ok --rm-local-fail ' +
			'--keep-local-ok --no-mark-dl --ignore-mark-dl')

	options = parser.script_parse(verbose_short=None)
	if len(options.args) != 1:  # we need exactly one positional argument (config file)
		parser.exit_with_usage(msg='Config file not specified!')
	options.opts.threads = int(options.opts.threads)
	return options


if __name__ == '__main__':
	sys.exit(_main())
