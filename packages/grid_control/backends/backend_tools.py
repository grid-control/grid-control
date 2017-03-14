# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import os
from grid_control.config import join_config_locations
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils import resolve_install_path, wait
from grid_control.utils.process_base import LocalProcess
from hpfwk import AbstractError, NestedException, clear_current_exception
from python_compat import any, identity, ifilter, imap, irange, lmap, tarfile


class BackendError(NestedException):
	pass


def unpack_wildcard_tar(log, output_dn):
	if os.path.exists(output_dn):
		if 'GC_WC.tar.gz' in os.listdir(output_dn):
			wildcard_tar = os.path.join(output_dn, 'GC_WC.tar.gz')
			try:
				tarfile.TarFile.open(wildcard_tar, 'r:gz').extractall(output_dn)
				os.unlink(wildcard_tar)
			except Exception:
				log.error('Can\'t unpack output files contained in %s', wildcard_tar)
				clear_current_exception()


class BackendDiscovery(ConfigurablePlugin):
	def discover(self):
		raise AbstractError


class BackendExecutor(ConfigurablePlugin):
	def __init__(self, config):
		ConfigurablePlugin.__init__(self, config)
		self._log = None

	def setup(self, log):
		self._log = log

	# log process helper function for backends
	def _filter_proc_log(self, proc, message=None, blacklist=None, discard_list=None, log_empty=True):
		if (not blacklist) and (not discard_list):
			return self._log.log_process(proc)
		blacklist = lmap(str.lower, blacklist or [])
		discard_list = lmap(str.lower, discard_list or [])

		def _is_on_list(line, lst):
			return any(imap(line.__contains__, lst))

		do_log = log_empty  # log if stderr is empty
		for line in ifilter(identity, imap(str.lower, proc.stderr.read_log().splitlines())):
			if _is_on_list(line, discard_list):  # line on discard list -> dont log
				return
			if not _is_on_list(line, blacklist):  # line not on blacklist -> do log
				return self._log.log_process(proc, msg=message)
			do_log = False  # don't log if all stderr lines are blacklisted
		if do_log:
			return self._log.log_process(proc, msg=message)


class ProcessCreator(ConfigurablePlugin):
	def create_proc(self, wms_id_list):
		raise AbstractError


class ForwardingExecutor(BackendExecutor):
	def __init__(self, config, executor):
		BackendExecutor.__init__(self, config)
		self._executor = executor

	def get_status(self):  # FIXME: not part of the BackendExecutor interface!
		return self._executor.get_status()

	def setup(self, log):
		self._executor.setup(log)


class ProcessCreatorViaArguments(ProcessCreator):
	def create_proc(self, wms_id_list):
		return LocalProcess(*self._arguments(wms_id_list))

	def _arguments(self, wms_id_list):
		raise AbstractError


class ProcessCreatorViaStdin(ProcessCreator):
	def create_proc(self, wms_id_list):
		proc = LocalProcess(*self._arguments())
		proc.stdin.write(self._stdin_message(wms_id_list))
		proc.stdin.close()
		return proc

	def _arguments(self):
		raise AbstractError

	def _stdin_message(self, wms_id_list):
		raise AbstractError


class ChunkedExecutor(ForwardingExecutor):
	def __init__(self, config, option_prefix, executor, def_chunk_size=5, def_chunk_interval=5):
		ForwardingExecutor.__init__(self, config, executor)
		self._chunk_size = config.get_int(join_config_locations(option_prefix, 'chunk size'),
			def_chunk_size, on_change=None)
		self._chunk_interval = config.get_int(join_config_locations(option_prefix, 'chunk interval'),
			def_chunk_interval, on_change=None)

	def execute(self, wms_id_list, *args, **kwargs):
		do_wait = False
		chunk_pos_iter = irange(0, len(wms_id_list), self._chunk_size)
		for wms_id_chunk in imap(lambda x: wms_id_list[x:x + self._chunk_size], chunk_pos_iter):
			if do_wait and not wait(self._chunk_interval):
				break
			do_wait = True
			for result in self._executor.execute(wms_id_chunk, *args, **kwargs):
				yield result


class ProcessCreatorAppendArguments(ProcessCreatorViaArguments):
	def __init__(self, config, cmd, args=None, fmt=identity):
		ProcessCreatorViaArguments.__init__(self, config)
		(self._cmd, self._args, self._fmt) = (resolve_install_path(cmd), args or [], fmt)

	def create_proc(self, wms_id_list):
		return LocalProcess(*self._arguments(wms_id_list))

	def _arguments(self, wms_id_list):
		return [self._cmd] + self._args + self._fmt(wms_id_list)
