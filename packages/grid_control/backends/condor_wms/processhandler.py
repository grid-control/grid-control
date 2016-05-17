# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, math, stat, time, signal
from grid_control.backends.wms import BackendError
from grid_control.config import ConfigError
from grid_control.utils import LoggedProcess, eprint, resolveInstallPath, vprint
from hpfwk import AbstractError, NestedException, Plugin

class CondorProcessError(BackendError):
	def __init__(self, msg, proc):
		(cmd, status, stdout, stderr) = (proc.cmd, proc.wait(), proc.getOutput(), proc.getError())
		BackendError.__init__(msg + '\n\tCommand: %s Return code: %s\nstdout: %s\nstderr: %s' % (cmd, status, stdout, stderr))

class TimeoutError(NestedException):
	pass

# placeholder for function arguments
defaultArg = object()

# Legacy context implementation: use as "with timeout(3):" or "timeout(3)\n...\ntimeout.cancel()"
class TimeoutContext(object):
	def __init__(self, duration = 1, exception = TimeoutError):
		"""
		Set a timeout to occur in duration, raising exception.
		This implementation is not thread-safe and only one timeout may be active at a time.
		"""
		self._active     = True
		self._duration   = duration
		self._handlerOld = signal.signal( signal.SIGALRM, self._onTimeout )
		if ( signal.alarm( int(duration) ) != 0 ):
			raise TimeoutError("Bug! Timeout set while previous timeout was active.")
	def _onTimeout(self, sigNum, frame):
		raise TimeoutError("Timeout after %d seconds." % self._duration )
	def cancel(self):
		if self._active:
			signal.alarm(0)
			signal.signal( signal.SIGALRM, self._handlerOld )
			self._active = False
	# Context methods
	def __enter__(self):
		return self
	def __exit__(self, exc_type, exc_value, traceback):
		self.cancel()


################################
# Process Handlers
# create interface for initializing a set of commands sharing a similar setup, e.g. remote commands through SSH

# Process Handler:
class ProcessHandler(Plugin):
	def LoggedExecute(self, cmd, args = '', **kwargs):
		raise AbstractError
	def LoggedCopyToRemote(self, source, dest, **kwargs):
		raise AbstractError
	def LoggedCopyFromRemote(self, source, dest, **kwargs):
		raise AbstractError
	def getDomain(self):
		raise AbstractError


# local Processes - ensures uniform interfacing as with remote connections
class LocalProcessHandler(ProcessHandler):
	cpy="cp -r"
	def __init__(self, **kwargs):
		pass
	# return instance of LoggedExecute with input properly wrapped
	def LoggedExecute(self, cmd, args = '', **kwargs):
		return LoggedProcess( cmd , args )

	def LoggedCopyToRemote(self, source, dest, **kwargs):
		return LoggedProcess( self.cpy, " ".join([source, dest]) )

	def LoggedCopyFromRemote(self, source, dest, **kwargs):
		return LoggedProcess( self.cpy, " ".join([source, dest]) )

	def getDomain(self):
		return "localhost"


# remote Processes via SSH
class SSHProcessHandler(ProcessHandler):
	# track lifetime and quality of command socket
	socketTimestamp=0
	socketFailCount=0
	# older versions of ssh/gsissh will propagate an end of master incorrectly to children - rotate sockets
	socketIdNow=0
	def __init__(self, **kwargs):
		self.__initcommands(**kwargs)
		self.defaultArgs="-vvv -o BatchMode=yes -o ForwardX11=no " + kwargs.get("defaultArgs","")
		self.socketArgs=""
		self.socketEnforce=kwargs.get("sshLinkEnforce",True)
		try:
			self.remoteHost = kwargs["remoteHost"]
		except Exception:
			raise ConfigError("Request to initialize SSH-Type RemoteProcessHandler without remote host.")
		try:
			self.sshLinkBase=os.path.abspath(kwargs["sshLink"])
			# older ssh/gsissh puts a maximum length limit on control paths, use a different one
			if ( len(self.sshLinkBase)>= 107):
				self.sshLinkBase=os.path.expanduser("~/.ssh/%s"%os.path.basename(self.sshLinkBase))
			self.sshLink=self.sshLinkBase
			self._secureSSHLink(initDirectory=True)
			self._socketHandler()
		except KeyError:
			self.sshLink=False
		# test connection once
		testProcess = self.LoggedExecute( "exit" )
		if testProcess.wait() != 0:
			testProcess.getError()
			raise CondorProcessError('Failed to validate remote connection.', testProcess)
	def __initcommands(self, **kwargs):
		self.cmd = resolveInstallPath("ssh")
		self.cpy = resolveInstallPath("scp") + " -r"

	# return instance of LoggedExecute with input properly wrapped
	def LoggedExecute(self, cmd, args = '', **kwargs):
		self._socketHandler()
		return LoggedProcess( " ".join([self.cmd, self.defaultArgs, self.socketArgs, self.remoteHost, self._argFormat(cmd + " " + args)]) )
	def LoggedCopyToRemote(self, source, dest, **kwargs):
		self._socketHandler()
		return LoggedProcess( " ".join([self.cpy, self.defaultArgs, self.socketArgs, source, self._remotePath(dest)]) )
	def LoggedCopyFromRemote(self, source, dest, **kwargs):
		self._socketHandler()
		return LoggedProcess( " ".join([self.cpy, self.defaultArgs, self.socketArgs, self._remotePath(source), dest]) )

	# Socket creation and cleanup
	def _CreateSocket(self, duration = 60):
		args = [self.cmd, self.defaultArgs, "-o ControlMaster=yes", self.socketArgsDef, self.remoteHost, self._argFormat("sleep %d" % duration)]
		self.__ControlMaster = LoggedProcess(" ".join(args))
		timeout = 0
		while not os.path.exists(self.sshLink):
			time.sleep(0.5)
			timeout += 0.5
			if timeout == 5:
				vprint("SSH socket still not available after 5 seconds...\n%s" % self.sshLink, level=1)
				vprint('Socket process: %s' % (self.__ControlMaster.cmd), level=2)
			if timeout == 10:
				return False
	def _CleanSocket(self):
		if not os.path.exists(self.sshLink):
			vprint("No Socket %s" % self.sshLink)
			return True
		vprint("Killing Socket %s" % self.sshLink)
#		killSocket = LoggedProcess( " ".join([self.cmd, self.defaultArgs, self.socketArgsDef, "-O exit", self.remoteHost]) )
#		while killSocket.poll() == -1:
#			print "poll", killSocket.poll()
#			time.sleep(0.5)
#			timeout += 0.5
#			if timeout == 5:
#				vprint("Failed to cancel ssh Socket...\n%s" % self.sshLink, level=1)
#				return False
#		print "done", killSocket.poll()
		timeout = 0
		while os.path.exists(self.sshLink):
			vprint("exists %d" % timeout)
			time.sleep(0.5)
			timeout += 0.5
			#if timeout == 5:
			#	vprint("Failed to remove ssh Socket...\n%s" % self.sshLink, level=1)
			#	return False
		return True

	def getDomain(self):
		return self.remoteHost

	# Helper functions
	def _argFormat(self, args):
		return "'" + args.replace("'", "'\\''") + "'"
	def _remotePath(self, path):
		return "%s:%s" % (self.remoteHost,path)

	# handler for creating, validating and publishing/denying ssh link socket
	def _socketHandler(self, maxFailCount=5):
		if self.sshLink:
			if self._refreshSSHLink():
				if self.socketArgs!=self.socketArgsDef:
					self.socketArgs=self.socketArgsDef
			else:
				self.socketFailCount+=1
				if self.socketArgs!="":
					self.socketArgs=""
				if self.socketFailCount>maxFailCount:
					eprint("Failed to create secure socket %s more than %s times!\nDisabling further attempts." % (self.sshLink,maxFailCount))
					self.sshLink=False

	# make sure the link file and directory are properly protected
	# 	@sshLink:	location of the link
	#	@directory:	secure only directory (for initializing)
	def _secureLinkDirectory(self, sshLink, enforce = True):
		sshLinkDir = os.path.dirname(sshLink)
		if not os.path.isdir(sshLinkDir):
			try:
				os.makedirs(sshLinkDir)
			except Exception:
				if self.socketEnforce:
					raise BackendError("Could not create or access directory for SSHLink:\n	%s" % sshLinkDir)
				else:
					return False
		if sshLinkDir!=os.path.dirname(os.path.expanduser("~/.ssh/")):
			try:
				os.chmod(sshLinkDir, stat.S_IRWXU)
			except Exception:
				if self.socketEnforce:
					raise BackendError("Could not secure directory for SSHLink:\n	%s" % sshLinkDir)
				else:
					return False
		return True
	def _secureLinkSocket(self, sshLink, enforce = True):
		if os.path.exists(sshLink):
			if stat.S_ISSOCK(os.stat(sshLink).st_mode):
				try:
					os.chmod(sshLink, stat.S_IRWXU)
				except Exception:
					if self.socketEnforce:
						raise BackendError("Could not secure SSHLink:\n	%s" % sshLink)
					else:
						return False
			else:
				if self.socketEnforce:
					raise BackendError("Non-socket object already exists for SSHLink:\n	%s" % sshLink)
				else:
					return False
		return True
	def _secureSSHLink(self, initDirectory=False):
		if self._secureLinkDirectory(self.sshLink) and (initDirectory or self._secureLinkSocket(self.sshLink)):
			return True
		return False

	# keep a process active in the background to speed up connecting by providing an active socket
	def _refreshSSHLink(self, minSeconds=5, maxSeconds=20):
		# if there is a link, ensure it'll still live for minimum lifetime
		if os.path.exists(self.sshLink) and stat.S_ISSOCK(os.stat(self.sshLink).st_mode):
			if ( time.time() - self.socketTimestamp < maxSeconds-minSeconds ):
				return True
		# stop already existing socket master
		if not self._CleanSocket():
			return False
		# rotate socket
		self.socketIdNow = (self.socketIdNow + 1) % (math.ceil(1.0*maxSeconds/(maxSeconds-minSeconds)) + 1)
		self.sshLink = self.sshLinkBase+str(self.socketIdNow)
		self.socketArgsDef = "-o ControlPath=" + self.sshLink
		# start new socket
		self._CreateSocket(maxSeconds)
		self.socketTimestamp = time.time()
		return self._secureSSHLink()

# remote Processes via GSISSH
class GSISSHProcessHandler(SSHProcessHandler):
	# commands to use - overwritten by inheriting class
	def __initcommands(self, **kwargs):
		resolveInstallPath('gsissh')
		resolveInstallPath('gsiscp')

# Helper class handling commands through remote interfaces
class RemoteProcessHandler(object):
	# enum for connection type - LOCAL exists to ensure uniform interfacing with local programms if needed
	class RPHType:
		enumList = ('LOCAL', 'SSH', 'GSISSH')
		for idx, eType in enumerate(enumList):
			locals()[eType] = idx

	# helper functions - properly prepare argument string for passing via interface
	def _argFormatSSH(self, args):
		return "'" + args.replace("'", "'\\''") + "'"
	def _argFormatLocal(self, args):
		return args

	# template for input for connection types
	RPHTemplate = {
		RPHType.LOCAL: {
			'command'	: "%(args)s %(cmdargs)s %%(cmd)s",
			'copy'		: "cp -r %(args)s %(cpargs)s %%(source)s %%(dest)s",
			'path'		: "%(path)s",
			'argFormat'	: _argFormatLocal
			},
		RPHType.SSH: {
			'command'	: "ssh %%(args)s %%(cmdargs)s %(rhost)s %%%%(cmd)s",
			'copy'		: "scp -r %%(args)s %%(cpargs)s %%%%(source)s %%%%(dest)s",
			'path'		: "%(host)s:%(path)s",
			'argFormat'	: _argFormatSSH
			},
		RPHType.GSISSH: {
			'command'	: "gsissh %%(args)s  %%(cmdargs)s %(rhost)s %%%%(cmd)s",
			'copy'		: "gsiscp -r %%(args)s %%(cpargs)s %%%%(source)s %%%%(dest)s",
			'path'		: "%(host)s:%(path)s",
			'argFormat'	: _argFormatSSH
			},
		}
	def __init__(self, remoteType="", **kwargs):
		self.cmd=False
		# pick requested remote connection
		try:
			self.remoteType = getattr(self.RPHType, remoteType.upper())
			self.cmd = self.RPHTemplate[self.remoteType]["command"]
			self.copy = self.RPHTemplate[self.remoteType]["copy"]
			self.path = self.RPHTemplate[self.remoteType]["path"]
			self.argFormat = self.RPHTemplate[self.remoteType]["argFormat"]
		except Exception:
			raise ConfigError("Request to initialize RemoteProcessHandler of unknown type: %s" % remoteType)
		# destination should be of type: [user@]host
		if self.remoteType==self.RPHType.SSH or self.remoteType==self.RPHType.GSISSH:
			try:
				self.cmd = self.cmd % { "rhost" : kwargs["host"] }
				self.copy = self.copy % { "rhost" : kwargs["host"] }
				self.host = kwargs["host"]
			except Exception:
				raise ConfigError("Request to initialize RemoteProcessHandler of type %s without remote host." % self.RPHType.enumList[self.remoteType])
		# add default arguments for all commands
		self.cmd = self.cmd % { "cmdargs" : kwargs.get("cmdargs",""), "args" : kwargs.get("args","") }
		self.copy = self.copy % { "cpargs" : kwargs.get("cpargs",""), "args" : kwargs.get("args","") }
		# test connection once
		proc = LoggedProcess(self.cmd % { "cmd" : "exit"})
		ret = proc.getAll()[0]
		if ret != 0:
			raise CondorProcessError('Validation of remote connection failed!', proc)
		vprint('Remote interface initialized:\n	Cmd: %s\n	Cp : %s' % (self.cmd,self.copy), level=2)

	# return instance of LoggedExecute with input properly wrapped
	def LoggedExecute(self, cmd, args = '', argFormat=defaultArg):
		if argFormat is defaultArg:
			argFormat=self.argFormat
		return LoggedProcess( self.cmd % { "cmd" : argFormat(self, "%s %s" % ( cmd, args )) } )

	def LoggedCopyToRemote(self, source, dest):
		return LoggedProcess( self.copy % { "source" : source, "dest" : self.path%{"host":self.host,"path":dest} } )

	def LoggedCopyFromRemote(self, source, dest):
		return LoggedProcess( self.copy % { "source" : self.path%{"host":self.host,"path":source}, "dest" : dest } )

	def LoggedCopy(self, source, dest, remoteKey="<remote>"):
		if source.startswith(remoteKey):
			source = self.path%{"host":self.host,"path":source[len(remoteKey):]}
		if dest.startswith(remoteKey):
			dest = self.path%{"host":self.host,"path":dest[len(remoteKey):]}
		return LoggedProcess( self.copy % { "source" : "%s:%s"%(self.host,source), "dest" : dest } )
