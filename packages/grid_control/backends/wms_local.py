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

import os, glob, time, shutil, tempfile
from grid_control.backends.aspect_cancel import CancelAndPurgeJobs, CancelJobs
from grid_control.backends.broker_base import Broker
from grid_control.backends.script_creator import create_shell_script
from grid_control.backends.storage import StorageManager
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_basic import BasicWMS
from grid_control.utils import ensure_dir_exists, get_path_share, remove_files, resolve_install_path
from grid_control.utils.activity import Activity
from grid_control.utils.file_tools import SafeFile, VirtualFile
from grid_control.utils.thread_tools import GCLock, with_lock
from hpfwk import ExceptionCollector
from python_compat import ifilter, imap, lchain, lfilter


class SandboxHelper(object):
	def __init__(self, config, wms_name):
		(self._cache, self._wms_name) = ([], wms_name)
		self._path = config.get_dn('sandbox path', config.get_work_path('sandbox'), must_exist=False)
		ensure_dir_exists(self._path, 'sandbox base', BackendError)

	def create_sandbox(self):
		# TODO
		pass

	def get_path(self):
		return self._path

	def get_sandbox(self, wms_id):
		marker_fn = self._get_marker_fn(wms_id)

		# Speed up function by caching result of listdir
		def _search_sandbox(source):
			for path in imap(lambda sbox: os.path.join(self._path, sbox), source):
				if os.path.exists(os.path.join(path, marker_fn)):
					return path
		result = _search_sandbox(self._cache)
		if result:
			return result
		old_cache = self._cache[:]
		self._cache = lfilter(lambda x: os.path.isdir(os.path.join(self._path, x)),
			os.listdir(self._path))
		return _search_sandbox(ifilter(lambda x: x not in old_cache, self._cache))

	def mark_sandbox(self, wms_id, sandbox):  # tag sandbox directory
		SafeFile(os.path.join(sandbox, self._get_marker_fn(wms_id)), 'w').write_close('')

	def _get_marker_fn(self, wms_id):
		return 'WMSID.%s.%s' % (self._wms_name, wms_id)


class LocalWMS(BasicWMS):
	config_section_list = BasicWMS.config_section_list + ['local']

	def __init__(self, config, name, broker_list,
			local_submit_executor, check_executor, cancel_executor):
		config.set_int('wait idle', 20)
		config.set_int('wait work', 5)
		self._sandbox_helper = SandboxHelper(config, name)
		self._delay = config.get_bool('delay output', False, on_change=None)
		local_memory_broker = LocalMemoryBroker(config, name)
		BasicWMS.__init__(self, config, name, broker_list + [local_memory_broker],
			check_executor=check_executor,
			cancel_executor=CancelAndPurgeJobs(config, cancel_executor,
				LocalPurgeJobs(config, self._sandbox_helper)))
		self._local_submit_executor = local_submit_executor
		self._sm_sb_in = config.get_plugin('sb input manager', 'LocalSBStorageManager',
			cls=StorageManager, bind_kwargs={'tags': [self]}, pargs=('sandbox', 'sandbox'))

		self._scratch_path_list = config.get_list('scratch path', ['$TMPDIR', '/tmp'], on_change=None)

	def _get_job_input_ft_list(self, jobnum, task):
		input_ft_list = BasicWMS._get_job_input_ft_list(self, jobnum, task)
		for idx, auth_fn in enumerate(self._token.get_auth_fn_list()):
			input_ft_list.append((auth_fn, ('_proxy.dat.%d' % idx).replace('.0', '')))
		return input_ft_list

	def _get_jobs_output(self, gc_id_jobnum_list):
		if not len(gc_id_jobnum_list):
			raise StopIteration

		activity = Activity('retrieving %d job outputs' % len(gc_id_jobnum_list))
		for gc_id, jobnum in gc_id_jobnum_list:
			wms_id = self._split_gc_id(gc_id)[1]
			path = self._sandbox_helper.get_sandbox(wms_id)
			if path is None:
				yield (jobnum, None)
				continue

			# Cleanup sandbox
			sb_out_fn = os.path.join(path, '_gc_sb_out.dat')
			if os.path.exists(sb_out_fn):
				output_fn_list = lchain(imap(lambda pat: glob.glob(os.path.join(path, pat.strip())),
					SafeFile(sb_out_fn).read_close().splitlines()))
				remove_files(ifilter(lambda x: x not in output_fn_list,
					imap(lambda fn: os.path.join(path, fn), os.listdir(path))))

			yield (jobnum, path)
		activity.finish()

	def _submit_job(self, job_desc, exec_fn, arg_list, sb_in_fn_list, sb_out_fn_list, req_list):
		req_list = self._broker.process(req_list)

		# create sandbox
		try:
			sandbox = tempfile.mkdtemp('', '%s.%s.' % (job_desc.task_id, job_desc.job_name),
				self._sandbox_helper.get_path())
		except Exception:
			raise BackendError('Unable to create sandbox directory "%s"!' % sandbox)

		(exec_fn, stdout_fn, stderr_fn) = self._wrap_script_for_local(sandbox, job_desc,
			exec_fn, arg_list, sb_in_fn_list, sb_out_fn_list)

		wms_id = self._local_submit_executor.submit(job_desc.task_id, job_desc.job_name,
			exec_fn, req_list, stdout_fn, stderr_fn)
		if wms_id is not None:
			gc_id = self._create_gc_id(wms_id)
			if gc_id is None:
				self._log.warning('Invalid WMS ID: %s', repr(wms_id))
			else:
				self._sandbox_helper.mark_sandbox(wms_id, sandbox)
		return (gc_id, {'sandbox': sandbox})

	def _wrap_script_for_local(self, sandbox, job_desc, exec_fn, arg_list,
			sb_in_fn_list, sb_out_fn_list):
		# copy sandbox files to sandbox directory
		sb_prefix = sandbox.replace(self._sandbox_helper.get_path(), '').lstrip('/')

		def _translate_target(fn):
			return (fn, fn, os.path.join(sb_prefix, os.path.basename(fn)))
		self._sm_sb_in.do_transfer(imap(_translate_target, sb_in_fn_list))

		# create job wrapper for local jobs - searching sandbox, redirecting stdout/stderr
		def _fmt_list(value):
			return '\'%s\'' % str.join(' ', imap(lambda x: '"%s"' % x, value))

		cfg_lines = [
			'GCLW_EXEC=%s\n' % _fmt_list(['./%s' % os.path.basename(exec_fn)] + arg_list),
			'GCLW_SCRATCH_SEARCH=%s\n' % _fmt_list(self._scratch_path_list + ['$GC_SANDBOX']),
		]
		if self._delay:
			cfg_lines.append('GCLW_OUTPUT_STREAM="%s"\n' % os.path.join(sandbox, 'gc.stdout'))
			cfg_lines.append('GCLW_ERROR_STREAM="%s"\n' % os.path.join(sandbox, 'gc.stderr'))
			stdout_fn = stderr_fn = '/dev/null'
		else:
			(stdout_fn, stderr_fn) = (os.path.join(sandbox, 'gc.stdout'), os.path.join(sandbox, 'gc.stderr'))

		ft_list = [
			exec_fn,
			get_path_share('gc-wrapper-local'),
			VirtualFile('_gc_local.conf', cfg_lines),
		]
		wrapper_fn = os.path.join(sandbox,
			'gc-local-boxed.%s.%s.sh' % (job_desc.task_id, job_desc.job_name))
		create_shell_script(wrapper_fn, ft_list, './gc-wrapper-local ./_gc_local.conf')
		return (wrapper_fn, stdout_fn, stderr_fn)


class LocalMemoryBroker(Broker):
	alias_list = ['memory']

	def __init__(self, config, name, broker_prefix=None, **kwargs):
		Broker.__init__(self, config, name, broker_prefix, **kwargs)
		self._memory = config.get_int('memory', -1, on_change=None)

	def process(self, req_list):
		return req_list + [(WMS.MEMORY, self._memory)]


class LocalPurgeJobs(CancelJobs):
	purge_lock = GCLock()

	def __init__(self, config, sandbox_helper):
		CancelJobs.__init__(self, config)
		self._sandbox_helper = sandbox_helper

	def execute(self, log, wms_id_list):  # yields list of purged (wms_id,)
		activity = Activity('waiting for jobs to finish')
		time.sleep(5)
		for wms_id in wms_id_list:
			path = self._sandbox_helper.get_sandbox(wms_id)
			if path is None:
				log.warning('Sandbox for job %r could not be found', wms_id)
				continue
			with_lock(LocalPurgeJobs.purge_lock, _purge_directory, log, path, wms_id)
			yield (wms_id,)
		activity.finish()


class Local(WMS):
	alias_list = ['']
	config_section_list = WMS.config_section_list + ['local']

	def __new__(cls, config, name):
		def _create_backend(wms):
			try:
				backend_cls = WMS.get_class(wms)
			except Exception:
				raise BackendError('Unable to load backend class %s' % repr(wms))
			wms_config = config.change_view(view_class='TaggedConfigView', set_classes=[backend_cls])
			return WMS.create_instance(wms, wms_config, name)
		wms = config.get('wms', '')  # support old style "backend = local" and "wms = pbs" configuration
		if wms:
			return _create_backend(wms)
		exc = ExceptionCollector()
		(wms_search_dict, wms_search_order) = config.get_dict('wms search list',
			default={'sacct': 'SLURM', 'sgepasswd': 'OGE', 'pbs-config': 'PBS', 'qsub': 'OGE',
				'condor_q': 'Condor', 'bsub': 'LSF', 'job_slurm': 'JMS'},
			default_order=['sacct', 'sgepasswd', 'pbs-config', 'qsub', 'condor_q', 'bsub', 'job_slurm'])
		for cmd in wms_search_order:
			try:
				resolve_install_path(cmd)
			except Exception:
				exc.collect()
				continue
			return _create_backend(wms_search_dict[cmd])
		# at this point all backends have failed!
		exc.raise_any(BackendError('No valid local backend found!'))


def _purge_directory(log, path, wms_id):
	try:
		shutil.rmtree(path)
	except Exception:
		log.critical('Unable to delete directory %r: %r', path, os.listdir(path))
		raise BackendError('Sandbox for job %r could not be deleted', wms_id)
