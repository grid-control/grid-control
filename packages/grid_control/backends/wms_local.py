# | Copyright 2009-2017 Karlsruhe Institute of Technology
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

import os, glob, time, shlex, shutil, tempfile
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs, CancelJobs
from grid_control.backends.broker_base import Broker
from grid_control.backends.wms import BackendError, BasicWMS, WMS
from grid_control.utils import ensure_dir_exists, get_path_share, remove_files, resolve_install_path
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import VirtualFile
from grid_control.utils.process_base import LocalProcess
from grid_control.utils.thread_tools import GCLock, with_lock
from hpfwk import AbstractError, ExceptionCollector, ignore_exception
from python_compat import ifilter, imap, ismap, lchain, lfilter, lmap


class SandboxHelper(object):
	def __init__(self, config):
		self._cache = []
		self._path = config.get_path('sandbox path', config.get_work_path('sandbox'), must_exist=False)
		ensure_dir_exists(self._path, 'sandbox base', BackendError)

	def get_path(self):
		return self._path

	def get_sandbox(self, gc_id):
		# Speed up function by caching result of listdir
		def _search_sandbox(source):
			for path in imap(lambda sbox: os.path.join(self._path, sbox), source):
				if os.path.exists(os.path.join(path, gc_id)):
					return path
		result = _search_sandbox(self._cache)
		if result:
			return result
		old_cache = self._cache[:]
		self._cache = lfilter(lambda x: os.path.isdir(os.path.join(self._path, x)),
			os.listdir(self._path))
		return _search_sandbox(ifilter(lambda x: x not in old_cache, self._cache))


class LocalWMS(BasicWMS):
	config_section_list = BasicWMS.config_section_list + ['local']

	def __init__(self, config, name, submit_exec, check_executor, cancel_executor,
			nodes_finder=None, queues_finder=None):
		config.set('broker', 'RandomBroker')
		config.set_int('wait idle', 20)
		config.set_int('wait work', 5)
		self._submit_exec = submit_exec
		self._sandbox_helper = SandboxHelper(config)
		BasicWMS.__init__(self, config, name, check_executor=check_executor,
			cancel_executor=CancelAndPurgeJobs(config, cancel_executor,
				LocalPurgeJobs(config, self._sandbox_helper)))

		def _get_nodes_list():
			if nodes_finder:
				return lmap(lambda x: x['name'], nodes_finder.discover())

		self._broker_site = config.get_plugin('site broker', 'UserBroker', cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pargs=('sites', 'sites', _get_nodes_list))

		def _get_queues_list():
			if queues_finder:
				result = {}
				for entry in queues_finder.discover():
					result[entry.pop('name')] = entry
				return result

		self._broker_queue = config.get_plugin('queue broker', 'UserBroker', cls=Broker,
			bind_kwargs={'inherit': True, 'tags': [self]}, pargs=('queue', 'queues', _get_queues_list))

		self._scratch_path = config.get_list('scratch path', ['TMPDIR', '/tmp'], on_change=True)
		self._submit_opt_list = shlex.split(config.get('submit options', '', on_change=None))
		self._memory = config.get_int('memory', -1, on_change=None)

	def parse_submit_output(self, data):
		raise AbstractError

	def _check_req(self, reqs, req, test=lambda x: x > 0):
		if req in reqs:
			return test(reqs[req])
		return False

	def _get_job_arguments(self, jobnum, sandbox):
		raise AbstractError

	def _get_jobs_output(self, gc_id_jobnum_list):
		if not len(gc_id_jobnum_list):
			raise StopIteration

		activity = Activity('retrieving %d job outputs' % len(gc_id_jobnum_list))
		for gc_id, jobnum in gc_id_jobnum_list:
			path = self._sandbox_helper.get_sandbox(gc_id)
			if path is None:
				yield (jobnum, None)
				continue

			# Cleanup sandbox
			output_fn_list = lchain(imap(lambda pat: glob.glob(os.path.join(path, pat)),
				self._output_fn_list))
			remove_files(ifilter(lambda x: x not in output_fn_list,
				imap(lambda fn: os.path.join(path, fn), os.listdir(path))))

			yield (jobnum, path)
		activity.finish()

	def _get_sandbox_file_list(self, task, sm_list):
		files = BasicWMS._get_sandbox_file_list(self, task, sm_list)
		for idx, auth_fn in enumerate(self._token.get_auth_fn_list()):
			files.append(VirtualFile(('_proxy.dat.%d' % idx).replace('.0', ''), open(auth_fn, 'r').read()))
		return files

	def _get_submit_arguments(self, jobnum, job_name, reqs, sandbox, stdout, stderr):
		raise AbstractError

	def _get_submit_proc(self, jobnum, sandbox, job_name, reqs):
		(stdout, stderr) = (os.path.join(sandbox, 'gc.stdout'), os.path.join(sandbox, 'gc.stderr'))
		submit_args = list(self._submit_opt_list)
		submit_args.extend(shlex.split(self._get_submit_arguments(jobnum, job_name,
			reqs, sandbox, stdout, stderr)))
		submit_args.append(get_path_share('gc-local.sh'))
		submit_args.extend(shlex.split(self._get_job_arguments(jobnum, sandbox)))
		return LocalProcess(self._submit_exec, *submit_args)

	def _submit_job(self, jobnum, task):
		# Submit job and yield (jobnum, WMS ID, other data)
		activity = Activity('submitting job %d' % jobnum)

		try:
			sandbox = tempfile.mkdtemp('', '%s.%04d.' % (task.get_description().task_id, jobnum),
				self._sandbox_helper.get_path())
		except Exception:
			raise BackendError('Unable to create sandbox directory "%s"!' % sandbox)
		sb_prefix = sandbox.replace(self._sandbox_helper.get_path(), '').lstrip('/')

		def _translate_target(desc, src, target):
			return (desc, src, os.path.join(sb_prefix, target))
		self._sm_sb_in.do_transfer(ismap(_translate_target, self._get_in_transfer_info_list(task)))

		self._write_job_config(os.path.join(sandbox, '_jobconfig.sh'), jobnum, task, {
			'GC_SANDBOX': sandbox, 'GC_SCRATCH_SEARCH': str.join(' ', self._scratch_path)})
		reqs = self._broker_site.broker(task.get_requirement_list(jobnum), WMS.SITES)
		reqs = dict(self._broker_queue.broker(reqs, WMS.QUEUES))
		if (self._memory > 0) and (reqs.get(WMS.MEMORY, 0) < self._memory):
			reqs[WMS.MEMORY] = self._memory  # local jobs need higher (more realistic) memory requirements

		job_name = task.get_description(jobnum).job_name
		proc = self._get_submit_proc(jobnum, sandbox, job_name, reqs)
		exit_code = proc.status(timeout=20, terminate=True)
		wms_id_str = proc.stdout.read(timeout=0).strip().strip('\n')
		wms_id = ignore_exception(Exception, None, self.parse_submit_output, wms_id_str)
		activity.finish()

		if exit_code != 0:
			self._log.warning('%s failed:', self._submit_exec)
		elif wms_id is None:
			self._log.warning('%s did not yield job id:\n%s', self._submit_exec, wms_id_str)
		gc_id = self._create_gc_id(wms_id)
		if gc_id is not None:
			open(os.path.join(sandbox, gc_id), 'w')
		else:
			self._log.log_process(proc)
		return (jobnum, gc_id, {'sandbox': sandbox})


class LocalPurgeJobs(CancelJobs):
	purge_lock = GCLock()

	def __init__(self, config, sandbox_helper):
		CancelJobs.__init__(self, config)
		self._sandbox_helper = sandbox_helper

	def execute(self, wms_id_list, wms_name):  # yields list of purged (wms_id,)
		activity = Activity('waiting for jobs to finish')
		time.sleep(5)
		for wms_id in wms_id_list:
			path = self._sandbox_helper.get_sandbox('WMSID.%s.%s' % (wms_name, wms_id))
			if path is None:
				self._log.warning('Sandbox for job %r could not be found', wms_id)
				continue
			with_lock(LocalPurgeJobs.purge_lock, _purge_directory, self._log, path, wms_id)
			yield (wms_id,)
		activity.finish()


class Local(WMS):
	config_section_list = WMS.config_section_list + ['local']

	def __new__(cls, config, name):
		def _create_backend(wms):
			try:
				backend_cls = WMS.get_class(wms)
			except Exception:
				raise BackendError('Unable to load backend class %s' % repr(wms))
			wms_config = config.change_view(view_class='TaggedConfigView', set_classes=[backend_cls])
			return WMS.create_instance(wms, wms_config, name)
		wms = config.get('wms', '')
		if wms:
			return _create_backend(wms)
		exc = ExceptionCollector()
		for cmd, wms in [('sacct', 'SLURM'), ('sgepasswd', 'OGE'), ('pbs-config', 'PBS'),
				('qsub', 'OGE'), ('condor_q', 'Condor'), ('bsub', 'LSF'), ('job_slurm', 'JMS')]:
			try:
				resolve_install_path(cmd)
			except Exception:
				exc.collect()
				continue
			return _create_backend(wms)
		# at this point all backends have failed!
		exc.raise_any(BackendError('No valid local backend found!'))


def _purge_directory(log, path, wms_id):
	try:
		shutil.rmtree(path)
	except Exception:
		log.critical('Unable to delete directory %r: %r', path, os.listdir(path))
		raise BackendError('Sandbox for job %r could not be deleted', wms_id)
