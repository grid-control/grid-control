# | Copyright 2014-2016 Karlsruhe Institute of Technology
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

# -*- coding: utf-8 -*-

# core modules
import atexit
import os

# standard modules
import logging
import re
import shutil
import socket
import tempfile
import time

# GC modules
from grid_control.backends.wms import BackendError
from grid_control.gc_exceptions import InstallationError
from grid_control.utils import LoggedProcess, ensureDirExists, resolveInstallPath
from hpfwk import AbstractError, Plugin
from python_compat import irange, lru_cache

# Container to call commands in a generic fashion
class CommandContainer(object):
	def __init__(self, command, args = lambda **kwargs: '', niceCommand = None, niceArg = None):
		self.cmd     = command
		self.args    = args
		self.niceCmd = niceCommand or (lambda **kwargs: command)
		self.niceArg = niceArg or args

# Classes providing methods for interaction with other systems
# via a uniform interface

# create a ProcessAdapter based on URI scheme
# Requires:
#	URI             a URI as scheme://[scheme args] or ""
# Optional:
#	externalScheme  list of scheme names handled externally
#	**kwargs        keyword arguments for adapter initialisation
# Returns:          Tuple of ( ProcessAdapter, UsedURIScheme )
#	ProcessAdapter  the appropriate Adapter or None if external
#	UsedURIScheme   the URI scheme extracted from the URI
def ProcessAdapterFactory(URI, externalSchemes = [], collapseLocal = True, **kwargs):
	"""
	Return an adapter compatible with the given URI
	
	Required:
	URI string
	       The URI to resolve (Defaults to local)
	
	Optional:
	externalSchemes iterable
	       Schemes not to be resolved by the factory
	collapseLocal boolean
	       Attempt to return LocalAdapters when the URI points to localhost
	raises ValueError if the URI does not match an adapter
	"""
	_logger = logging.getLogger('process.adapter.%s' % ProcessAdapterFactory.__name__)
	def getAdapter(URI, externalSchemes = [], **kwargs):
		if URI.split("://")[0] in externalSchemes:
			return None, URI.split("://")[0]
		for Adapter in [ LocalProcessAdapter, SSHProcessAdapter, GSISSHProcessAdapter ]:
			try:
				tmp = Adapter.resolveURI(URI, **kwargs)
				return Adapter(URI = URI, **kwargs), tmp[0]
			except ValueError:        # error in resolving URI
				continue
			except InstallationError: # adapter is not available/broken
				_logger.log(logging.DEBUG3, "URI '%s', rejecting adapter %s [Not available]" % (URI, Adapter.__name__))
				continue
			except BackendError:      # error in establishing connection
				_logger.log(logging.DEBUG3, "URI '%s', rejecting adapter %s [Verification failed]" % (URI, Adapter.__name__))
				continue
		raise ValueError("Failed to match URI '%s' with any Adapter." % URI)
	adapter, scheme = getAdapter(URI, externalSchemes, **kwargs)
	if collapseLocal and adapter.isLoopback() and adapter.getType() != 'local':
		_logger.log(logging.INFO3, 'Swapping adapter of type %s for local adapter (resolving loopback).' % (adapter.__class__.__name__))
		adapter, scheme = getAdapter(adapter.getLoopbackURI(), **kwargs)
	_logger.log(logging.INFO2, "Resolved URI '%s', providing adapter %s" % (URI, adapter.__class__.__name__))
	return adapter, scheme


# Base class defining interface
class ProcessAdapterInterface(Plugin):
	uriScheme = []
	_basepath = ""
	# Default loggers
	_logger = logging.getLogger('process.adapter')
	_log    = _logger.log
	# python internals
	def __init__(self, URI, **kwargs):
		self.URI=URI
		self._errorLog = kwargs.get('errorLog')
		self._initLogger(**kwargs)
		self._log(logging.INFO1, 'Establishing process adapter of type %s' % self.__class__.__name__)
	def __enter__(self):
		raise NotImplementedError
	def __exit__(self, exc_type, exc_value, traceback):
		raise NotImplementedError
	# public interfaces
	def LoggedExecute(self, command, args = '', niceCmd = None, niceArgs = None):
		"""Execute a command via the adapter shell"""
		raise AbstractError
	def LoggedGet(self, source, destination):
		"""Move a source file/folder from the adapter domain to the local domain destination"""
		raise AbstractError
	def LoggedPut(self, source, destination):
		"""Move a source file/folder from the local domain to the adapter domain destination"""
		raise AbstractError
	def LoggedDelete(self, targets):
		"""Delete a file/folder in the adapter domain"""
		raise AbstractError
	def getDomain(self):
		"""Get a descriptive representation of the adapter domain"""
		raise AbstractError
	def getType(self):
		"""Get a descriptive representation of the adapter interfacing method"""
		raise AbstractError
	def getProtocol(self):
		"""Get a descriptive representation of the adapter interfacing protocol"""
		raise AbstractError
	def isLoopback(self):
		"""Check if this adapter is a loopback."""
		return bool(self.getLoopbackURI())
	def getLoopbackURI(self):
		"""Return a local URI if this adapter is a loopback"""
		raise AbstractError
	def getDomainAbsPath(self, path):
		"""Translate any path to an absolute one in the adapter domain"""
		abspath = path
		if not path.startswith("/") and self._basepath:
			abspath = self._basepath + "/" + abspath
		return abspath
	getDomainAbsPath = lru_cache(getDomainAbsPath)
	def getGlobalAbsPath(self, path):
		"""Translate any path to an absolute one in the executing GC domain"""
		raise AbstractError
	# general internal functions
	def resolveURI(cls, URI = None, **kwargs):
		"""
		Extract the adapter information for a given URI
		
		raises ValueError if the URI is not compatible
		"""
		raise ValueError
	resolveURI = classmethod(resolveURI)
	def _initInterfaces(self, **kwargs):
		raise AbstractError

	def _initLogger(cls, **kwargs):
		cls._logger = logging.getLogger('process.adapter.%s' % cls.__name__)
		cls._log = cls._logger.log
	_initLogger = classmethod(_initLogger)

	def _validateConnection(self):
		"""
		Test the connection of this adapter

		raises BackendError if the connection exits unsuccessfully
		raises InstallationError if stdout is not clean
		"""
		self._log(logging.INFO2, "Validating adapter for URI '%s'" % self.URI )
		testProcess = self.LoggedExecute( "exit 0" )
		stdProcess = self.LoggedExecute( "echo stdout; echo stderr >&2; exit 0" )
		for proc in [ testProcess, stdProcess]:
			if proc.wait() != os.EX_OK:
				if self._errorLog:
					proc.logError(self._errorLog)
				raise BackendError("Failure when validating connection to '%s'." % self.getDomain)
		if len(testProcess.getOutput()) != 0 or stdProcess.getOutput() != "stdout\n":
			raise InstallationError("Output of processes from adapter for URI '%s' is either muted or poluted." %  self.URI )


# Access to local system
class LocalProcessAdapter(ProcessAdapterInterface):
	uriScheme = ["","local","localhost"]
	uriRepr   = "[local://][/<path>]"
	def __init__(self, URI, **kwargs):
		ProcessAdapterInterface.__init__(self, URI, **kwargs)
		( _, self._basepath ) = self.resolveURI(URI)
		self._basepath = self._basepath or os.getcwd()
		self._initInterfaces(**kwargs)
	def __enter__(self):
		self
	def __exit__(self, exc_type, exc_value, traceback):
		pass

	def LoggedExecute(self, command, args = '', niceCmd = None, niceArgs = None):
		return LoggedProcess(command, args = args, niceCmd = niceCmd or command, niceArgs = niceArgs or args)

	def LoggedGet(self, source, destination):
		return LoggedProcess(self._copy.cmd, self._copy.args(source=source, destination=destination), niceCmd = self._copy.niceCmd())

	def LoggedPut(self, source, destination):
		return LoggedGet(self, destination, source)

	def LoggedDelete(self, target):
		return LoggedProcess(self._delete.cmd, self._delete.args(target=target), niceCmd = self._delete.niceCmd())

	def getDomain(self):
		return "localhost"
	def getType(self):
		return "local"
	def getProtocol(self):
		return "local"
	def isLoopback(self):
		return True
	def getLoopbackURI(self):
		return self.URI
	def getGlobalAbsPath(self, path):
		return self.getDomainAbsPath(path)

	# general internal functions
	def resolveURI(cls, URI, **kwargs):
		if URI == '':
			return ('', None, None)
		reMatch = re.search(r'(?:(\w*)://)(?:/(.*))?(.*)',URI)
		if not reMatch:
			raise ValueError("URI %s could not be parsed" % URI)
		( scheme, path, leftover ) = reMatch.group(1,2,3)
		cls._log(logging.DEBUG1, "Resolved URI '%s' as %s" % (URI, { 'scheme' : scheme, 'path' : path, 'remainder' : leftover}) )
		if ( scheme ) and ( scheme not in cls.uriScheme ):
			raise ValueError("Got URI of scheme '%s', expected '%s'." % (scheme, "' or '".join(cls.uriScheme)))
		if leftover:
			raise ValueError("URI '%s' yielded unexpected leftover '%s'. Expected URI form %s." % (URI, leftover, cls.uriRepr))
		return ( scheme, path )
	resolveURI = classmethod(resolveURI)

	def createURI(cls, elementMap):
		try:
			return 'localhost:///%s' % elementMap['path']
		except Exception:
			return 'localhost://'
	createURI = classmethod(createURI)

	def _initInterfaces(self, **kwargs):
		try:
			copypath=resolveInstallPath("rsync")
			copynice=lambda **kwargs: "copy via rsync"
		except InstallationError:
			copypath=resolveInstallPath("cp")
			copynice=lambda **kwargs: "copy via cp"
		self._copy = CommandContainer(
			copypath,
			lambda **kwargs: "-r %s %s"  % (kwargs['source'], kwargs['destination']),
			copynice)
		self._delete = CommandContainer(
			resolveInstallPath("rm"),
			lambda *kwargs : "-r " + kwargs['target'],
			lambda **kwargs : "rm")


# Access via SSH
class SSHProcessAdapter(ProcessAdapterInterface):
	uriScheme = ["ssh"]
	uriRepr   = "[ssh://][<user>@]<host>[:<port>][/<path>]"
	def __init__(self, URI, **kwargs):
		"""
		Required:
		URI  string
		       remote target identity as
		       [ssh://][<user>@]<host>[:<port>][/<path>]
		
		Optional:
		retryLimit int
		       limit for any failures before the connection is defunct
		needSocket  bool
		       reject connection if socket cannot be established
		socketDir  string
		       location for placing socket
		socketMinSec  float/int
		       minimum remaining lifetime of socket before refresh
		socketCount  int
		       maximum number of sockets in use
		"""
		ProcessAdapterInterface.__init__(self, URI, **kwargs)
		( _, self._user, self._host, self._port, self._basepath ) = self.resolveURI(URI, **kwargs)
		self._initInterfaces(**kwargs)
		self._initSockets(**kwargs)
		# always clean up on termination, even outside of context
		atexit.register(self.__exit__, None, None, None)
		# test connection once before usage
		self._validateConnection()
		self._basepath = self._basepath or self.LoggedExecute( "pwd" ).getOutput().strip()
	def __enter__(self):
		self
	def __exit__(self, exc_type, exc_value, traceback):
		self._log(logging.DEBUG1,"Exiting context for URI '%s'" % self.URI)
		for socket in self._socketProcs:
			if self._socketProcs[socket].poll() < 0:
				self._socketProcs[socket].kill()
				self._log(logging.DEBUG3,'Terminated master for socket %s - PID: %s' % (socket, self._socketProcs[socket].proc.pid))
		time.sleep(0.2) # delay for sockets to disappear before further cleanup
		shutil.rmtree(self._socketDir)

	# Logged Processes
	def LoggedExecute(self, command, args = '', niceCmd = None, niceArgs = None):
		return LoggedProcess(
			self._exeWrapper.cmd,
			args = self._exeWrapper.args(
				command = command,
				args    = args
				),
			niceCmd  = self._exeWrapper.niceCmd(command=(niceCmd or command)),
			niceArgs = self._exeWrapper.niceArg(args=(niceArgs or args)),
			shell    = False,
			)

	def LoggedGet(self, source, destination):
		return LoggedProcess(
			self._copy.cmd,
			self._copy.args(
				source=self.getGlobalAbsPath(source),
				destination=destination
				),
			niceCmd  = self._copy.niceCmd(),
			niceArgs = self._copy.niceArg(
				source=self.getGlobalAbsPath(source),
				destination=destination
				),
			shell    = False,
			)

	def LoggedPut(self, source, destination):
		return LoggedProcess(
			self._copy.cmd,
			self._copy.args(
				source=source,
				destination=self.getGlobalAbsPath(destination)
				),
			niceCmd  = self._copy.niceCmd(),
			niceArgs = self._copy.niceArg(
				source=source,
				destination=self.getGlobalAbsPath(destination)
				),
			shell    = False,
			)

	def LoggedDelete(self, target):
		return LoggedProcess(
			self._delete.cmd,
			self._delete.args({ "target" : target }),
			niceCmd  = self._delete.niceCmd(),
			niceArgs = self._delete.niceArg({ "target" : target }),
			shell    = False,
			)

	def getDomain(self):
		return self._host
	def getType(self):
		return "ssh"
	def getProtocol(self):
		return "ssh"
	def getLoopbackURI(self, _cachedURI = {}):
		try:
			return _cachedURI[self]
		except KeyError:
			_cachedURI[self] = None
			remoteHostname = self.LoggedExecute('hostname').getOutput(wait=True).strip()
			localHostname  = socket.gethostname().strip()
			remoteAdress   = socket.gethostbyname(remoteHostname)
			localAdress    = socket.gethostbyname(localHostname)
			self._log(logging.DEBUG1, "'Checking host/IP for loopback - local: '%s/%s', remote: '%s/%s'" % (localHostname, localAdress, remoteHostname, remoteAdress) )
			if socket.gethostbyname(remoteHostname) == socket.gethostbyname(localHostname):
				_cachedURI[self] = LocalProcessAdapter.createURI({
					'user' : self._user,
					'host' : self._host,
					'port' : self._port,
					'path' : self._basepath
					})
			return _cachedURI[self]

	def LoggedSocket(self, command="", args = '', niceCmd = None, niceArgs = None):
		return LoggedProcess(
			self._socketWrapper.cmd,
			args = self._socketWrapper.args(
				command = command,
				args    = args
				),
			niceCmd = self._socketWrapper.niceCmd(command=(niceCmd or command)),
			niceArgs = self._exeWrapper.niceArg(args=(niceArgs or args)),
			shell    = False,
			)

	# general internal functions
	def resolveURI(cls, URI, **kwargs):
		reMatch = re.search(r'(?:(\w*)://)?(?:(\w*)@)?([\w.-]*)(?::(\d*))?(?:/(.*))?(.*)',URI)
		if not reMatch:
			raise ValueError("URI %s could not be parsed" % URI)
		( scheme, user, host, port, path, leftover) = reMatch.group(1,2,3,4,5,6)
		cls._log(logging.DEBUG1, "Resolved URI '%s' as %s" % (URI, { 'scheme' : scheme, 'user' : user, 'host' : host, 'port' : port, 'path' : path, 'remainder' : leftover}) )
		if ( scheme ) and ( scheme not in cls.uriScheme ):
			raise ValueError("Got URI of scheme '%s', expected '%s'." % (scheme, "' or '".join(cls.uriScheme)))
		if leftover:
			raise ValueError("URI %s yielded unexpected leftover '%s'. Expected URI form %s." % (URI, leftover, cls.uriRepr))
		if not host:
			raise ValueError("URI %s yielded no hostname. Expected URI form %s." % (URI, cls.uriRepr))
		return ( scheme, user, host, port, path )
	resolveURI = classmethod(resolveURI)

	def _initInterfaces(self, **kwargs):
		def makeArgList(*args):
			argList = []
			for arg in args:
				try:
					if isinstance(arg, str):
						raise
					argList.extend(arg)
				except Exception:
					argList.append(arg)
			return [ arg for arg in argList if arg ]
		portArgs = lambda key : self._port and "-%s%s"%(key, self._port) or ""
		self._exeWrapper = CommandContainer(
			resolveInstallPath("ssh"),
			lambda **kwargs: makeArgList(
				self._getDefaultArgs(),
				self._getValidSocketArgs(),
				portArgs('p'),
				self._host,
				" ".join((kwargs["command"], kwargs.get("args",'')))
				),
			lambda **kwargs: "'%(command)s' [via ssh %(URI)s]" % {
				"command" : kwargs.get("command","<undefined command>"),
				"URI"     : self.URI,
				},
			lambda **kwargs: kwargs.get('args') and "Arguments: '%s'" % kwargs.get('args') or ''
			)
		self._copy = CommandContainer(
			resolveInstallPath("scp"),
			lambda **kwargs: makeArgList(
				self._getDefaultArgs(),
				self._getValidSocketArgs(),
				"-r",
				portArgs('P'),
				kwargs["source"],
				portArgs('P'),
				kwargs["destination"],
				),
			lambda **kwargs: "'scp' [%(URI)s]",
			lambda **kwargs: "Transfer: '%(source)' -> '%(destination)'" % kwargs,
			)
		self._delete = CommandContainer(
			resolveInstallPath("ssh"),
			lambda **kwargs: makeArgList(
				self._getDefaultArgs(),
				self._getValidSocketArgs(),
				portArgs('p'),
				self._host,
				"rm -rf " + kwargs["target"],
				),
			lambda **kwargs: "'rm' [via ssh %(URI)s]" % kwargs,
			lambda **kwargs: "Target: '%(target)s'" % kwargs,
			)
		self._socketWrapper = CommandContainer(
			resolveInstallPath("ssh"),
			lambda **kwargs: makeArgList(
				self._getDefaultArgs(),
				self._getCurrentSocketArgs(),
				portArgs('p'),
				self._host,
				" ".join((kwargs["command"], kwargs.get("args",'')))
				),
			lambda **kwargs: "'%(command)s' [via ssh %(URI)s (master)]" % {
				"command" : kwargs.get("command","<undefined command>"),
				"URI"     : self.URI,
				},
			lambda **kwargs: kwargs.get('args') and "Arguments: '%s'" % kwargs.get('args') or ''
			)

	# Interface specific internal methods
	def _initSockets(self, **kwargs):
		self._needSocket    = kwargs.get("needSocket", True)
		self._socketMinSec  = kwargs.get("socketMinSec", 300)
		self._socketCount   = max(2,kwargs.get("socketCount", 2))
		self._socketIndex   = 0
		self._socketMaxMiss = kwargs.get("socketMaxMiss", 2)
		self._socketMisses  = 0
		# sockets should reside in secure, managed directory
		if kwargs.get("socketDir","") and len(kwargs.get("socketDir")) < 105:
			self._socketDir = kwargs.get("socketDir")
			ensureDirExists(self._socketDir, name = "SSH connection socket container directory")
		else:
			self._socketDir = tempfile.mkdtemp()
		self._log(logging.DEBUG1, 'Using socket directoy %s' % self._socketDir)
		# create list of socket names and corresponding arguments to rotate through
		self._socketList = [ os.path.join(self._socketDir, str(socketIndex)) for socketIndex in irange(self._socketCount) ]
		self._socketArgList = [ ["-oControlMaster=auto","-oControlPath=%s" % socket] for socket in self._socketList ]
		self._socketProcs = {}

	def _incrementSocket(self):
		self._socketIndex = ( self._socketIndex + 1 ) % self._socketCount
	def _getCurrentSocket(self):
		return self._socketList[self._socketIndex]
	def _getCurrentSocketArgs(self):
		return self._socketArgList[self._socketIndex]

	def _getValidSocketArgs(self):
		if self._socketMisses >= self._socketMaxMiss:
			self._socketMisses -= 1
			return []
		# validate that current socket does exist and is fresh enough, else pick next
		try:
			if ( time.time() - os.path.getctime(self._getCurrentSocket()) ) > self._socketMinSec:
				raise OSError
		except OSError:
			self._incrementSocket()
		while not self._validateControlMaster():
			self._socketMisses += 1
			if not self._needSocket:
				self._log(logging.INFO3, 'Failed to validate socket. (%d/%d)' % (self._socketMisses, self._socketMaxMiss))
				if self._socketMisses == self._socketMaxMiss:
					self._socketMisses + self._socketMaxMiss
					self._log(logging.INFO2, 'Disabling failing sockets for %d operations.' % self._socketMaxMiss)
				return []
			if self._socketMisses == self._socketMaxMiss:
				raise BackendError("Repeated failure to create ControlMaster.")
		self._socketMisses = max(self._socketMisses-1, 0)
		return self._getCurrentSocketArgs()

	def _validateControlMaster(self, timeout = 20):
		# socket already exists, so Master is fresh or undying
		if os.path.exists(self._getCurrentSocket()):
			return True
		# create dummy background process, encapsuling sleep to stay alive regardless of SSH version
		socketProcess = self.LoggedSocket("sleep", "%d" % ((self._socketCount - 0.5 ) * self._socketMinSec))
		# validate socket exists
		waitTime = 0
		while not os.path.exists(self._getCurrentSocket()):
			if socketProcess.poll() > 0:
				self._log(logging.DEBUG1, "Failure on ControlMaster socket creation [code: %s]."%socketProcess.poll())
				if self._errorLog:
					socketProcess.logError(self._errorLog)
				return False
			time.sleep(0.5)
			waitTime += 0.5
			if waitTime == timeout:
				self._log(logging.DEBUG1, "Timeout (%ds) on ControlMaster socket creation." % timeout)
				socketProcess.kill()
				if self._errorLog:
					socketProcess.logError(self._errorLog)
				return False
		self._socketProcs[self._getCurrentSocket()] = socketProcess
		return True

	# Helper functions for SSH connections
	def _getDefaultArgs(self):
		"""Provide arguments for ssh container"""
		argString = ["-v", "-oBatchMode=yes", "-oForwardX11=no"]
		return argString

	def getGlobalAbsPath(self, path):
		abspath = (self._user and self._user+"@" or "") + self._host + ":" + self.getDomainAbsPath(path)
		return abspath
	getGlobalAbsPath = lru_cache(getGlobalAbsPath)

# Access via GSISSH
class GSISSHProcessAdapter(SSHProcessAdapter):
	def getProtocol(self):
		return "gsissh"
	def _initInterfaces(self, **kwargs):
		self._exeWrapper = CommandContainer(
			resolveInstallPath("gsissh"),
			lambda **kwargs: "%(port)s %(sshargs)s %(socketArgs)s %(host)s %(payload)s"  % {
				"port"       : (self._port and "-p"+self._port or ""),
				"sshargs"    : self._getDefaultArgs(),
				"socketArgs" : self._getValidSocketArgs(),
				"host"       : self._host,
				"payload"    : self._wrapPayload(kwargs["command"] + " " + kwargs.get("args",''))
				},
			lambda **kwargs: "%(command)s via adapter gsissh [URI %(URI)s]" % {
				"command" : kwargs.get("command","<undefined command>"),
				"URI"     : self.URI,
				},
			)
		self._copy = CommandContainer(
			resolveInstallPath("gsiscp"),
			lambda **kwargs: "%(sshargs)s %(socketArgs)s -r %(port)s %(source)s %(port)s %(destination)s"  % {
				"port"       : (self._port and "-P"+self._port or ""),
				"sshargs"    : self._getDefaultArgs(),
				"socketArgs" : self._getValidSocketArgs(),
				"source"     : kwargs["source"],
				"destination": kwargs["destination"],
				},
			lambda **kwargs: "gsiscp"
			)
		self._delete = CommandContainer(
			resolveInstallPath("gsissh"),
			lambda **kwargs: "%(port)s %(sshargs)s %(socketArgs)s %(payload)s"  % {
				"port"    : (self._port and "-p"+self._port or ""),
				"sshargs" : self._getDefaultArgs(),
				"socketArgs" : self._getValidSocketArgs(),
				"payload" : self._wrapPayload( "rm -rf " + kwargs["target"] )
				},
			lambda **kwargs: "'rm' via gsissh"
			)
		self._socketWrapper = CommandContainer(
			resolveInstallPath("gsissh"),
			lambda **kwargs: "%(port)s %(sshargs)s %(socketArgs)s %(host)s %(payload)s"  % {
				"port"       : (self._port and "-p"+self._port or ""),
				"sshargs"    : self._getDefaultArgs(),
				"socketArgs" : self._getCurrentSocketArgs(),
				"host"       : self._host,
				"payload"    : self._wrapPayload(kwargs["command"] + " " + kwargs.get("args",''))
				},
			lambda **kwargs: "%(command)s via adapter gsissh (master) [URI %(URI)s]" % {
				"command" : kwargs.get("command","<undefined command>"),
				"URI"     : self.URI,
				},
			)
