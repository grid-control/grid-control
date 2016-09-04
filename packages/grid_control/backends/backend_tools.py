# | Copyright 2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.config import appendOption
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.utils.process_base import LocalProcess
from hpfwk import AbstractError, NestedException
from python_compat import any, identity, ifilter, imap, irange, lmap

class BackendError(NestedException):
	pass


class ProcessCreator(ConfigurablePlugin):
	def create_proc(self, wmsIDs):
		raise AbstractError


class ProcessCreatorViaArguments(ProcessCreator):
	def create_proc(self, wmsIDs):
		return LocalProcess(*self._arguments(wmsIDs))

	def _arguments(self, wmsIDs):
		raise AbstractError


class ProcessCreatorAppendArguments(ProcessCreatorViaArguments):
	def __init__(self, config, cmd, args = None, fmt = identity):
		ProcessCreatorViaArguments.__init__(self, config)
		(self._cmd, self._args, self._fmt) = (utils.resolveInstallPath(cmd), args or [], fmt)

	def create_proc(self, wmsIDs):
		return LocalProcess(*self._arguments(wmsIDs))

	def _arguments(self, wmsIDs):
		return [self._cmd] + self._args + self._fmt(wmsIDs)


class ProcessCreatorViaStdin(ProcessCreator):
	def create_proc(self, wmsIDs):
		proc = LocalProcess(*self._arguments())
		proc.stdin.write(self._stdin_message(wmsIDs))
		proc.stdin.close()
		return proc

	def _arguments(self):
		raise AbstractError

	def _stdin_message(self, wmsIDs):
		raise AbstractError


class BackendExecutor(ConfigurablePlugin):
	def setup(self, log):
		self._log = log

	# log process helper function for backends
	def _filter_proc_log(self, proc, message = None, blacklist = None, discardlist = None, log_empty = True):
		if (not blacklist) and (not discardlist):
			return self._log.log_process(proc)
		blacklist = lmap(str.lower, blacklist or [])
		discardlist = lmap(str.lower, discardlist or [])
		def is_on_list(line, lst):
			return any(imap(lambda entry: entry in line, lst))
		do_log = log_empty # log if stderr is empty
		for line in ifilter(identity, imap(str.lower, proc.stderr.read_log().splitlines())):
			if is_on_list(line, discardlist): # line on discard list -> dont log
				return
			if not is_on_list(line, blacklist): # line not on blacklist -> do log
				return self._log.log_process(proc, msg = message)
			do_log = False # don't log if all stderr lines are blacklisted
		if do_log:
			return self._log.log_process(proc, msg = message)


class ForwardingExecutor(BackendExecutor):
	def __init__(self, config, executor):
		BackendExecutor.__init__(self, config)
		self._executor = executor

	def setup(self, log):
		self._executor.setup(log)

	def get_status(self): # FIXME: not part of the BackendExecutor interface!
		return self._executor.get_status()


class ChunkedExecutor(ForwardingExecutor):
	def __init__(self, config, option_prefix, executor, def_chunk_size = 5, def_chunk_interval = 5):
		ForwardingExecutor.__init__(self, config, executor)
		self._chunk_size = config.getInt(appendOption(option_prefix, 'chunk size'), def_chunk_size, onChange = None)
		self._chunk_time = config.getInt(appendOption(option_prefix, 'chunk interval'), def_chunk_interval, onChange = None)

	def execute(self, wmsIDs, *args, **kwargs):
		do_wait = False
		for wmsIDChunk in imap(lambda x: wmsIDs[x:x + self._chunk_size], irange(0, len(wmsIDs), self._chunk_size)):
			if do_wait and not utils.wait(self._chunk_time):
				break
			do_wait = True
			for result in self._executor.execute(wmsIDChunk, *args, **kwargs):
				yield result


class BackendDiscovery(ConfigurablePlugin):
	def discover(self):
		raise AbstractError
