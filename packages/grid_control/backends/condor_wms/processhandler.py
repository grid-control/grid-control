# | Copyright 2013-2017 Karlsruhe Institute of Technology
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

import os, math, stat, time, logging
from grid_control.backends.logged_process import LoggedProcess
from grid_control.backends.wms import BackendError
from grid_control.config import ConfigError
from grid_control.utils import ensure_dir_exists, resolve_install_path
from hpfwk import AbstractError, NestedException, Plugin, clear_current_exception


class TimeoutError(NestedException):
	pass


class CondorProcessError(BackendError):
	def __init__(self, msg, proc):
		(cmd, status, stdout, stderr) = (proc.cmd, proc.wait(), proc.get_output(), proc.get_error())
		BackendError.__init__(msg +
			'\n\tCommand: %s Return code: %s\nstdout: %s\nstderr: %s' % (cmd, status, stdout, stderr))


class ProcessHandler(Plugin):
	# create interface for initializing a set of commands sharing a similar setup
	def __init__(self, **kwargs):
		self._log = logging.getLogger('backend.condor')

	def get_domain(self):
		raise AbstractError

	def logged_copy_from_remote(self, source, dest):
		raise AbstractError

	def logged_copy_to_remote(self, source, dest):
		raise AbstractError

	def logged_execute(self, cmd, args=''):
		raise AbstractError


class LocalProcessHandler(ProcessHandler):
	# local Processes - ensures uniform interfacing as with remote connections
	def get_domain(self):
		return 'localhost'

	def logged_copy_from_remote(self, source, dest):
		return LoggedProcess('cp -r', '%s %s' % (source, dest))

	def logged_copy_to_remote(self, source, dest):
		return LoggedProcess('cp -r', '%s %s' % (source, dest))

	def logged_execute(self, cmd, args=''):
		return LoggedProcess(cmd, args)


class SSHProcessHandler(ProcessHandler):
	# remote Processes via SSH
	# track lifetime and quality of command socket
	# old versions of ssh/gsissh will incorrectly notify children about end of master - rotate sockets
	def __init__(self, **kwargs):
		ProcessHandler.__init__(self, **kwargs)
		ssh_default_args = ' -vvv -o BatchMode=yes -o ForwardX11=no'
		self._shell_cmd = resolve_install_path('ssh') + ssh_default_args
		self._copy_cmd = resolve_install_path('scp') + ssh_default_args + ' -r'
		self._ssh_link_id = 0
		self._ssh_link_args = ''
		self._ssh_link_timestamp = 0
		self._ssh_link_fail_count = 0
		self._ssh_link_master_proc = None
		try:
			self._remote_host = kwargs['remote_host']
		except Exception:
			raise ConfigError('Request to initialize SSH-Type RemoteProcessHandler without remote host.')

		try:
			self._ssh_link_base = os.path.abspath(kwargs['sshLink'])
			# older ssh/gsissh puts a maximum length limit on control paths, use a different one
			if len(self._ssh_link_base) >= 107:
				self._ssh_link_base = os.path.expanduser('~/.ssh/%s' % os.path.basename(self._ssh_link_base))
			self._ssh_link = self._ssh_link_base
			_ssh_link_secure(self._ssh_link, init_dn=True)
			self._get_ssh_link()
		except KeyError:
			clear_current_exception()
			self._ssh_link = False

		# test connection once
		proc_test = self.logged_execute('exit')
		if proc_test.wait() != 0:
			raise CondorProcessError('Failed to validate remote connection.', proc_test)

	def get_domain(self):
		return self._remote_host

	def logged_copy_from_remote(self, source, dest):
		return LoggedProcess(str.join(' ', [self._copy_cmd, self._get_ssh_link(),
			self._remote_path(source), dest]))

	def logged_copy_to_remote(self, source, dest):
		return LoggedProcess(str.join(' ', [self._copy_cmd, self._get_ssh_link(),
			source, self._remote_path(dest)]))

	def logged_execute(self, cmd, args=''):
		return LoggedProcess(str.join('', [self._shell_cmd, self._get_ssh_link(),
			self._remote_host, _format_args_ssh(cmd + ' ' + args)]))

	def _clean_socket(self):
		if not os.path.exists(self._ssh_link):
			self._log.error('No Socket %s', self._ssh_link)
			return True
		self._log.info('Killing Socket %s', self._ssh_link)
		timeout = 0
		while os.path.exists(self._ssh_link):
			self._log.error('exists %d', timeout)
			time.sleep(0.5)
			timeout += 0.5
		return True

	def _create_socket(self, ssh_link_arg, duration=60):
		# Socket creation and cleanup
		args = [self._shell_cmd, '-o ControlMaster=yes', ssh_link_arg,
			self._remote_host, _format_args_ssh('sleep %d' % duration)]
		self._ssh_link_master_proc = LoggedProcess(str.join(' ', args))
		timeout = 0
		while not os.path.exists(self._ssh_link):
			time.sleep(0.5)
			timeout += 0.5
			if timeout == 5:
				self._log.log(logging.INFO1,
					'SSH socket still not available after 5 seconds...\n%s', self._ssh_link)
				self._log.log(logging.INFO2, 'Socket process: %s', self._ssh_link_master_proc.cmd)
			if timeout == 10:
				return False

	def _get_ssh_link(self, sec_min=5, sec_max=20, max_retries=5):
		if not self._ssh_link:
			return ''
		# keep a process active in the background to speed up connecting by providing an active socket
		# if there is a link, ensure it'll still live for minimum lifetime
		if os.path.exists(self._ssh_link) and stat.S_ISSOCK(os.stat(self._ssh_link).st_mode):
			if time.time() - self._ssh_link_timestamp < sec_max - sec_min:
				return self._ssh_link_args
		# stop already existing socket master
		if not self._clean_socket():
			self._ssh_link_fail_count += 1
			if self._ssh_link_fail_count > max_retries:
				self._log.error('Failed to create secure socket %s more than %s times!\n' +
					'Disabling further attempts.', self._ssh_link, max_retries)
				self._ssh_link = False
			return ''
		# rotate socket
		ssh_link_id_cyle = (math.ceil(sec_max / float(sec_max - sec_min)) + 1)
		self._ssh_link_id = (self._ssh_link_id + 1) % ssh_link_id_cyle
		self._ssh_link = self._ssh_link_base + str(self._ssh_link_id)
		new_ssh_link_args = '-o ControlPath=' + self._ssh_link
		# start new socket
		self._create_socket(new_ssh_link_args, sec_max)
		self._ssh_link_timestamp = time.time()
		_ssh_link_secure(self._ssh_link, init_dn=False)
		self._ssh_link_args = new_ssh_link_args
		return self._ssh_link_args

	def _remote_path(self, path):
		return '%s:%s' % (self._remote_host, path)


def _format_args_ssh(args):
	return '\'' + args.replace('\'', "'\\''") + '\''


def _ssh_link_secure(ssh_link_fn, init_dn):
	ssh_link_dn = ensure_dir_exists(os.path.dirname(ssh_link_fn), 'SSH link direcory', BackendError)
	if ssh_link_dn != os.path.dirname(os.path.expanduser('~/.ssh/')):
		try:
			os.chmod(ssh_link_dn, stat.S_IRWXU)
		except Exception:
			raise BackendError('Could not secure directory for SSHLink %s' % ssh_link_dn)
	if init_dn:
		return
	if os.path.exists(ssh_link_fn):
		if not stat.S_ISSOCK(os.stat(ssh_link_fn).st_mode):
			raise BackendError('Non-socket object already exists for SSHLink %s' % ssh_link_fn)
		try:
			os.chmod(ssh_link_fn, stat.S_IRWXU)
		except Exception:
			raise BackendError('Could not secure SSHLink %s' % ssh_link_fn)
