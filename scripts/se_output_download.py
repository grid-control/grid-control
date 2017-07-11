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

import os, sys, time, random, signal, logging
from gc_scripts import ConsoleTable, FileInfo, FileInfoProcessor, Job, ScriptOptions, get_script_object, handle_abort_interrupt, str_file_size  # pylint:disable=line-too-long
from grid_control.backends import AccessToken
from grid_control.backends.storage import se_copy, se_exists, se_mkdir, se_rm
from grid_control.logging_setup import ProcessArchiveHandler
from grid_control.utils import wait
from grid_control.utils.activity import Activity, ProgressActivity
from grid_control.utils.data_structures import make_enum
from grid_control.utils.thread_tools import GCEvent, GCLock, GCThreadPool, start_daemon
from hpfwk import clear_current_exception, ignore_exception
from python_compat import all, any, imap, md5, resolve_fun, sorted


try:
	from grid_control_gui.ansi import ANSI
except Exception:
	clear_current_exception()
	ANSI = None  # pylint:disable=invalid-name
try:
	from grid_control_gui.report_bar import ProgressBarActivity
except Exception:
	clear_current_exception()
	ProgressBarActivity = ProgressActivity  # pylint:disable=invalid-name

log = logging.getLogger('se_output_download')  # pylint:disable=invalid-name
logging.getLogger('logging.process').disabled = True

get_thread_state = resolve_fun('threading:Thread.is_alive', 'threading:Thread.isAlive')  # pylint:disable=invalid-name

JobDownloadStatus = make_enum(['JOB_OK', 'JOB_ALREADY', 'JOB_NO_OUTPUT',  # pylint:disable=invalid-name
	'JOB_PROCESSING', 'JOB_FAILED', 'JOB_RETRY', 'JOB_INCOMPLETE'])
FileDownloadStatus = make_enum(['FILE_OK', 'FILE_EXISTS', 'FILE_TIMEOUT',  # pylint:disable=invalid-name
	'FILE_SE_BLACKLIST', 'FILE_HASH_FAILED', 'FILE_TRANSFER_FAILED', 'FILE_MKDIR_FAILED'])


def accepted_se(opts, fi):
	return any(imap(fi[FileInfo.Path].__contains__, opts.select_se)) or not opts.select_se


def check_token(token):
	if time.time() - check_token.last_check > 10:
		check_token.last_check = time.time()
		if not token.can_submit(20 * 60, True):
			log.critical('\nPlease renew access token')
			sys.exit(os.EX_UNAVAILABLE)
check_token.last_check = 0  # <global-state>


def delete_files(opts, jobnum, fi_list, download_failed, show_se_skip=False):
	for (fi_idx, fi) in enumerate(fi_list):
		def _delete(file_se_path, where, what):
			if se_exists(file_se_path).status(timeout=10, terminate=True) == 0:
				activity = Activity('Deleting file %s from %s' % (fi[FileInfo.NameDest], where))
				rm_proc = se_rm(file_se_path)
				if rm_proc.status(timeout=60, terminate=True) == 0:
					log.info(log_intro(jobnum, fi_idx) + 'Deleted file %s', file_se_path)
				else:
					log.log_process(rm_proc, msg=log_intro(jobnum, fi_idx) + 'Unable to remove %s' % what)
				activity.finish()

		if accepted_se(opts, fi):
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
		elif show_se_skip:
			log.info(log_intro(jobnum, fi_idx) + 'Skipping file on blacklisted SE')


def delete_job(opts, work_dn, status_mon, job_db, job_obj, jobnum):
	activity = Activity('Deleting output files')
	try:
		if (job_obj.get('deleted') == 'True') and not opts.mark_ignore_rm:
			return status_mon.register_job_result(jobnum, 'Files are already deleted',
				JobDownloadStatus.JOB_ALREADY)
		if (job_obj.get('download') != 'True') and not opts.mark_ignore_dl:
			return status_mon.register_job_result(jobnum, 'Files are not yet downloaded',
				JobDownloadStatus.JOB_INCOMPLETE)
		fi_list = FileInfoProcessor().process(os.path.join(work_dn, 'output', 'job_%d' % jobnum)) or []
		if not fi_list:
			return status_mon.register_job_result(jobnum, 'Job has no output files',
				JobDownloadStatus.JOB_NO_OUTPUT)
		job_successful = job_obj.state != Job.SUCCESS
		delete_files(opts, jobnum, fi_list, download_failed=job_successful, show_se_skip=True)
		set_job_prop(job_db, jobnum, job_obj, 'deleted', 'True')
		status_mon.register_job_result(jobnum, 'All files deleted', JobDownloadStatus.JOB_OK)
	finally:
		activity.finish()


def download_job(opts, work_dn, status_mon, job_db, job_obj, jobnum):
	if job_obj.get('download') == 'True' and not opts.mark_ignore_dl:
		return status_mon.register_job_result(jobnum, 'All files already downloaded',
			JobDownloadStatus.JOB_ALREADY)

	# Read the file hash entries from job info file
	fi_list = FileInfoProcessor().process(os.path.join(work_dn, 'output', 'job_%d' % jobnum)) or []
	is_download_failed = False
	if not fi_list:
		if opts.mark_empty_fail:
			is_download_failed = True
		else:
			return status_mon.register_job_result(jobnum, 'Job has no output files',
				JobDownloadStatus.JOB_NO_OUTPUT)

	download_result_list = []
	progress = ProgressActivity('Processing output files', len(fi_list))
	for (fi_idx, fi) in enumerate(fi_list):
		progress.update_progress(fi_idx, msg='Processing output file %r' % fi[FileInfo.NameDest])
		download_result_list.append(download_single_file(opts, jobnum, fi_idx, fi, status_mon))
	progress.finish()

	is_download_failed = is_download_failed or any(imap(download_result_list.__contains__, [
		FileDownloadStatus.FILE_TIMEOUT, FileDownloadStatus.FILE_HASH_FAILED,
		FileDownloadStatus.FILE_TRANSFER_FAILED, FileDownloadStatus.FILE_MKDIR_FAILED]))
	is_download_success = all(imap([FileDownloadStatus.FILE_OK,
		FileDownloadStatus.FILE_EXISTS].__contains__, download_result_list))

	# Ignore the first opts.retry number of failed jobs
	retry_count = int(job_obj.get('download attempt', 0))
	if fi_list and is_download_failed and opts.retry and (retry_count < int(opts.retry)):
		set_job_prop(job_db, jobnum, job_obj, 'download attempt', str(retry_count + 1))
		return status_mon.register_job_result(jobnum, 'Download attempt #%d failed' % retry_count + 1,
			JobDownloadStatus.RETRY)

	delete_files(opts, jobnum, fi_list, is_download_failed)

	if is_download_failed:
		if opts.mark_fail:
			# Mark job as failed to trigger resubmission
			job_obj.state = Job.FAILED
			job_db.commit(jobnum, job_obj)
		status_mon.register_job_result(jobnum, 'Download failed', JobDownloadStatus.JOB_FAILED)
	elif is_download_success:
		if opts.mark_dl:
			# Mark as downloaded
			set_job_prop(job_db, jobnum, job_obj, 'download', 'True')
		status_mon.register_job_result(jobnum, 'Download successful', JobDownloadStatus.JOB_OK)
	else:
		# eg. because of SE blacklist
		status_mon.register_job_result(jobnum, 'Download incomplete', JobDownloadStatus.JOB_INCOMPLETE)


def download_monitor(jobnum, fi_idx, fi, local_se_path, copy_ended_event, copy_timeout_event):
	def _get_file_size():
		local_fn = local_se_path.replace('file://', '')
		if os.path.exists(local_fn):
			return os.path.getsize(local_fn)

	def _update_progress(progress, cur_file_size, old_file_size, start_time, old_time):
		if cur_file_size is not None:
			progress.update_progress(cur_file_size,
				msg='%7s - %7s avg. - %7s inst.' % (str_file_size(cur_file_size),
					rate(cur_file_size, 0, start_time),
					rate(cur_file_size, old_file_size or 0, old_time)))

	(cur_file_size, old_file_size) = (None, None)
	(start_time, old_time, last_transfer_time) = (time.time(), time.time(), time.time())
	progress = ProgressBarActivity('<download stalling>', fi[FileInfo.Size])

	while not copy_ended_event.wait(0.1):  # Loop until monitor lock is available
		(cur_file_size, cur_time) = (_get_file_size(), time.time())
		if cur_time - last_transfer_time > 5 * 60:
			copy_timeout_event.set()  # Trigger timeout when size is unchanged for > 5min
		elif cur_file_size is None:
			start_time = cur_time
		else:
			if cur_file_size != old_file_size:
				last_transfer_time = cur_time
			if (cur_file_size != old_file_size) or (cur_time - old_time > 1):
				# update progress when size changes - or more than 1 sec elapsed
				_update_progress(progress, cur_file_size, old_file_size, start_time, old_time)
				(old_file_size, old_time) = (cur_file_size, cur_time)
	_update_progress(progress, cur_file_size, old_file_size, start_time, old_time)
	log.info(log_intro(jobnum, fi_idx) + progress.get_msg().rstrip('.'))
	progress.finish()


def download_single_file(opts, jobnum, fi_idx, fi, status_mon):
	(source_se_path, target_se_path, local_se_path) = get_fi_path_tuple(opts, fi)
	show_file_info(jobnum, fi_idx, fi)

	# Copy files to local folder
	if not accepted_se(opts, fi):
		return status_mon.register_file_result(jobnum, fi_idx, 'skipping file on blacklisted SE',
			FileDownloadStatus.FILE_SE_BLACKLIST)
	activity_check = Activity('Checking file existance')
	try:
		if opts.skip_existing and (se_exists(target_se_path).status(timeout=10, terminate=True) == 0):
			return status_mon.register_file_result(jobnum, fi_idx, 'skipping already existing file',
				FileDownloadStatus.FILE_EXISTS)
	finally:
		activity_check.finish()
	if se_exists(os.path.dirname(target_se_path)).status(timeout=10, terminate=True) != 0:
		activity = Activity('Creating target directory')
		try:
			mkdir_proc = se_mkdir(os.path.dirname(target_se_path))
			if mkdir_proc.status(timeout=10, terminate=True) != 0:
				return status_mon.register_file_result(jobnum, fi_idx, 'unable to create target dir',
					FileDownloadStatus.FILE_MKDIR_FAILED, proc=mkdir_proc)
		finally:
			activity.finish()

	if 'file://' in target_se_path:
		local_se_path = target_se_path
	copy_timeout_event = GCEvent()
	copy_ended_event = GCEvent()
	monitor_thread = start_daemon('Download monitor %s' % jobnum, download_monitor,
		jobnum, fi_idx, fi, local_se_path, copy_ended_event, copy_timeout_event)

	cp_proc = se_copy(source_se_path, target_se_path, tmp=local_se_path)
	while (cp_proc.status(timeout=0) is None) and not copy_timeout_event.wait(timeout=0.1):
		pass
	copy_ended_event.set()
	monitor_thread.join()

	if copy_timeout_event.is_set():
		cp_proc.terminate(timeout=1)
		return status_mon.register_file_result(jobnum, fi_idx, 'Transfer timeout',
			FileDownloadStatus.FILE_TIMEOUT)
	elif cp_proc.status(timeout=0, terminate=True) != 0:
		return status_mon.register_file_result(jobnum, fi_idx, 'Transfer error',
			FileDownloadStatus.FILE_TIMEOUT, proc=cp_proc)
	return hash_verify(opts, status_mon, local_se_path, jobnum, fi_idx, fi)


def get_fi_path_tuple(opts, fi):
	source_se_path = os.path.join(fi[FileInfo.Path], fi[FileInfo.NameDest])
	target_se_path = os.path.join(opts.output, fi[FileInfo.NameDest])
	local_se_path = 'file://' + os.path.join(opts.tmp_dir, 'dlfs.%s' % fi[FileInfo.NameDest])
	return (source_se_path, target_se_path, local_se_path)


def get_se_host(se_path):
	return se_path.split('://')[-1].split('/')[0].split(':')[0]


def hash_calc(filename):
	md5_obj = md5()
	blocksize = 4 * 1024 * 1024  # use 4M blocksize:
	fp = open(filename, 'rb')
	pos = 0
	progress = ProgressBarActivity('Calculating checksum', os.path.getsize(filename))
	while True:
		buffer_str = fp.read(blocksize)
		md5_obj.update(buffer_str)
		pos += blocksize
		progress.update_progress(pos)
		if len(buffer_str) != blocksize:
			break
	progress.finish()
	return md5_obj.hexdigest()


def hash_verify(opts, status_mon, local_se_path, jobnum, fi_idx, fi):
	if not opts.verify_md5:
		return status_mon.register_file_result(jobnum, fi_idx, 'Download successful',
			FileDownloadStatus.FILE_OK)
	# Verify => compute md5hash
	remote_hash = fi[FileInfo.Hash]
	activity = Activity('Verifying checksum')
	try:
		local_hash = ignore_exception(Exception, None, hash_calc, local_se_path.replace('file://', ''))
		if local_hash is None:
			return status_mon.register_file_result(jobnum, fi_idx, 'Unable to calculate checksum',
				FileDownloadStatus.FILE_HASH_FAILED)
	finally:
		activity.finish()
	hash_match = fi[FileInfo.Hash] == local_hash
	match_map = {True: 'MATCH', False: 'FAIL'}
	if ANSI is not None:
		match_map = {True: ANSI.reset + ANSI.color_green + 'MATCH' + ANSI.reset,
			False: ANSI.reset + ANSI.color_red + 'FAIL' + ANSI.reset}
	msg = '\tLocal  hash: %s\n' % local_hash + \
		log_intro(jobnum, fi_idx) + '\tRemote hash: %s\n' % remote_hash + \
		log_intro(jobnum, fi_idx) + 'Checksum comparison: ' + match_map[hash_match]
	if hash_match:
		return status_mon.register_file_result(jobnum, fi_idx, msg, FileDownloadStatus.FILE_OK)
	return status_mon.register_file_result(jobnum, fi_idx, msg, FileDownloadStatus.FILE_HASH_FAILED)


def log_intro(jobnum, fi_idx=None):
	result = ['Job %5d' % jobnum]
	if fi_idx is not None:
		result.append('File %2d' % fi_idx)
	return str.join(' | ', result) + ' | '


def process_all(opts, args):
	# Init everything in each loop to pick up changes
	script_obj = get_script_object(args[0], opts.job_selector, only_success=False)
	token = AccessToken.create_instance(opts.token, script_obj.new_config, 'token')
	work_dn = script_obj.config.get_work_path()
	if process_all.first:
		logging.getLogger().addHandler(ProcessArchiveHandler(os.path.join(work_dn, 'error.tar')))
		process_all.first = False

	# Create SE output dir
	if not opts.output:
		opts.output = os.path.join(work_dn, 'se_output')
	if '://' not in opts.output:
		opts.output = 'file:///%s' % os.path.abspath(opts.output)

	job_db = script_obj.job_db
	jobnum_list = job_db.get_job_list()
	status_mon = StatusMonitor(len(jobnum_list))
	if opts.shuffle:
		random.shuffle(jobnum_list)
	else:
		jobnum_list.sort()

	if opts.threads:
		activity = Activity('Processing jobs')
		pool = GCThreadPool(opts.threads)
		for jobnum in jobnum_list:
			pool.start_daemon('Processing job %d' % jobnum, process_job,
				opts, work_dn, status_mon, job_db, token, jobnum)
		pool.wait_and_drop()
		activity.finish()
	else:
		progress = ProgressActivity('Processing job', max(jobnum_list) + 1)
		for jobnum in jobnum_list:
			progress.update_progress(jobnum)
			process_job(opts, work_dn, status_mon, job_db, token, jobnum)
		progress.finish()

	# Print overview
	if not opts.hide_results:
		status_mon.show_results()
	return status_mon.is_finished()
process_all.first = True  # <global-state>


def process_job(opts, work_dn, status_mon, job_db, token, jobnum):
	check_token(token)
	job_obj = job_db.get_job(jobnum)
	# Only run over finished and not yet downloaded jobs
	if (job_obj.state != Job.SUCCESS) and opts.job_success:
		return status_mon.register_job_result(jobnum, 'Job has not yet finished successfully',
			JobDownloadStatus.JOB_PROCESSING)
	if opts.delete:
		delete_job(opts, work_dn, status_mon, job_db, job_obj, jobnum)
	else:
		download_job(opts, work_dn, status_mon, job_db, job_obj, jobnum)
	time.sleep(float(opts.slowdown))


def rate(cur_size, ref_size, ref_time):
	return str_file_size(((cur_size - ref_size) / max(1., time.time() - ref_time))) + '/s'


def set_job_prop(job_db, jobnum, job_obj, key, value):
	job_obj.set(key, value)
	job_db.commit(jobnum, job_obj)


def show_file_info(jobnum, fi_idx, fi):
	log.info(log_intro(jobnum, fi_idx) + 'Name: %s -> %s',
		fi[FileInfo.NameLocal], fi[FileInfo.NameDest])
	se_host = get_se_host(fi[FileInfo.Path])
	if se_host:
		se_host = ' (%s)' % se_host
	log.info(log_intro(jobnum, fi_idx) + 'Path: %s' + se_host, fi[FileInfo.Path])
	if fi[FileInfo.Size] is not None:
		log.info(log_intro(jobnum, fi_idx) + 'Size: %s', str_file_size(fi[FileInfo.Size]))


class StatusMonitor(object):
	def __init__(self, num_jobs):
		self._result = {}
		self._lock = GCLock()
		self._num_jobs = num_jobs

	def is_finished(self):
		num_success = sum(imap(lambda jds: self._result.get(jds, 0), [
			JobDownloadStatus.JOB_OK, JobDownloadStatus.JOB_ALREADY, JobDownloadStatus.JOB_INCOMPLETE]))
		return num_success == self._num_jobs

	def register_file_result(self, jobnum, fi_idx, msg, status, proc=None):
		if proc:
			log.log_process(proc, msg=log_intro(jobnum, fi_idx) + msg)
		else:
			log.info(log_intro(jobnum, fi_idx) + msg)
		self._register_result(status)
		return status  # returned file status is actually used later

	def register_job_result(self, jobnum, msg, status):
		log.info(log_intro(jobnum) + msg)
		self._register_result(status)

	def show_results(self):
		def _iter_download_results(cls):
			marker = False
			for stat in sorted(self._result, key=self._result.get):
				if self._result[stat] and (stat in cls.enum_value_list):
					yield {0: cls.enum2str(stat), 1: self._result[stat]}
					marker = True
			if marker:
				yield '='
			yield {0: 'Total', 1: self._num_jobs}

		if self._result:
			ConsoleTable.create([(0, 'Status'), (1, '')],
				_iter_download_results(JobDownloadStatus), title='Job status overview')
			ConsoleTable.create([(0, 'Status'), (1, '')],
				_iter_download_results(FileDownloadStatus), title='File status overview')

	def _register_result(self, status):
		self._lock.acquire()
		try:
			self._result[status] = self._result.get(status, 0) + 1
		finally:
			self._lock.release()


def _main():
	options = _parse_cmd_line()
	opts = options.opts
	signal.signal(signal.SIGINT, handle_abort_interrupt)

	# Disable loop mode if it is pointless
	if (opts.loop and not opts.skip_existing) and (opts.mark_ignore_dl or not opts.mark_dl):
		log.info('Loop mode was disabled to avoid continuously downloading the same files\n')
		(opts.loop, opts.infinite) = (False, False)

	while True:
		if (process_all(opts, options.args) or not opts.loop) and not opts.infinite:
			break
		wait(60)


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

	_add_bool_opt(None, 'l ', 'loop', default=False,
		help_base='loop over jobs until all files are successfully processed')
	_add_bool_opt(None, 'L ', 'infinite', default=False,
		help_base='process jobs in an infinite loop')
	_add_bool_opt(None, None, 'shuffle', default=False,
		help_base='shuffle job processing order')
	parser.add_text(None, 'J', 'job-selector', default=None,
		help='specify the job selector')
	parser.add_text(None, 'T', 'token', default='VomsProxy',
		help='specify the access token used to determine ability to download / delete' +
			' - VomsProxy or TrivialAccessToken')
	parser.add_list(None, 'S', 'select-se', default=None,
		help='specify the SE paths to process')
	parser.add_bool(None, 'd', 'delete', default=False,
		help='perform file deletions')
	parser.add_bool(None, 'R', 'hide-results', default=False,
		help='specify if the transfer overview should be hidden')
	parser.add_text(None, 't', 'threads', default=0,
		help='how many jobs should be processed in parallel [Default: no multithreading]')
	parser.add_text(None, None, 'slowdown', default=2,
		help='specify delay between processing jobs [Default: 2 sec]')

	parser.section('jobs', 'Job state / flag handling')
	_add_bool_opt('jobs', None, 'job-success', default=True,
		help_base='only select successful jobs')
	_add_bool_opt('jobs', None, 'mark-rm', default=False,
		option_prefix_pair=('ignore', 'use'), dest='mark_ignore_rm',
		help_base='mark about successfully removed jobs', help_prefix_pair=('ignore ', 'use '))
	_add_bool_opt('jobs', None, 'mark-dl', default=True,
		help_base='mark successfully downloaded jobs as such')
	_add_bool_opt('jobs', None, 'mark-dl', default=False,
		option_prefix_pair=('ignore', 'use'), dest='mark_ignore_dl',
		help_base='mark about successfully downloaded jobs', help_prefix_pair=('ignore ', 'use '))
	_add_bool_opt('jobs', None, 'mark-fail', default=True,
		help_base='mark jobs failing verification as such')
	_add_bool_opt('jobs', None, 'mark-empty-fail', default=False,
		help_base='mark jobs without any files as failed')

	parser.section('down', 'Download options')
	_add_bool_opt('down', 'v ', 'verify-md5', default=True,
		help_base='MD5 verification of SE files', help_prefix_pair=('enable ', 'disable '))
	_add_bool_opt('down', None, '', default=False,
		option_prefix_pair=('skip-existing', 'overwrite'), dest='skip_existing',
		help_base='files which are already on local disk', help_prefix_pair=('skip ', 'overwrite '))
	parser.add_text('down', 'o', 'output', default=None,
		help='specify the local output directory')
	parser.add_text('down', 'O', 'tmp-dir', default='/tmp',
		help='specify the local tmp directory')
	parser.add_text('down', 'r', 'retry',
		help='how often should a transfer be attempted [Default: 0]')

	parser.section('file', 'Local / SE file handling during download')
	option_help_base_list = [
		('local-ok', 'files of successful jobs in local directory'),
		('local-fail', 'files of failed jobs in local directory'),
		('se-ok', 'files of successful jobs on SE'),
		('se-fail', 'files of failed jobs on the SE'),
	]
	for (option, help_base) in option_help_base_list:
		_add_bool_opt('file', None, option, default=False, option_prefix_pair=('rm', 'keep'),
			help_base=help_base, help_prefix_pair=('remove ', 'keep '))

	parser.section('short_delete', 'Shortcuts for delete options')
	parser.add_fset('short_delete', 'D', 'just-delete',
		help='Delete files from SE and local area - shorthand for:'.ljust(100) + '%s',
		flag_set='--delete --use-mark-rm --ignore-mark-dl ' +
			'--rm-se-fail --rm-local-fail --rm-se-ok --rm-local-ok')

	parser.section('short_down', 'Shortcuts for download options')
	parser.add_fset('short_down', 'm', 'move',
		help='Move files from SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail ' +
			'--rm-se-fail --rm-local-fail --rm-se-ok --keep-local-ok')
	parser.add_fset('short_down', 'c', 'copy',
		help='Copy files from SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail ' +
			'--rm-se-fail --rm-local-fail --keep-se-ok --keep-local-ok')
	parser.add_fset('short_down', 'j', 'just-copy',
		help='Just copy files from SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --skip-existing --no-mark-dl --ignore-mark-dl --no-mark-fail ' +
			'--keep-se-fail --keep-local-fail --keep-se-ok --keep-local-ok')
	parser.add_fset('short_down', 's', 'smart-copy',
		help='Copy correct files from SE, but remember already downloaded ' +
			'files and delete corrupt files - shorthand for: '.ljust(100) + '%s',
		flag_set='--verify-md5 --mark-dl --mark-fail --rm-se-fail ' +
			'--rm-local-fail --keep-se-ok --keep-local-ok')
	parser.add_fset('short_down', 'V', 'just-verify',
		help='Just verify files on SE - shorthand for:'.ljust(100) + '%s',
		flag_set='--verify-md5 --no-mark-dl --keep-se-fail ' +
			'--rm-local-fail --keep-se-ok --rm-local-ok --ignore-mark-dl')

	options = parser.script_parse(verbose_short=None)
	if len(options.args) != 1:  # we need exactly one positional argument (config file)
		parser.exit_with_usage(msg='Config file not specified!')
	options.opts.threads = int(options.opts.threads)
	return options


if __name__ == '__main__':
	sys.exit(_main())
