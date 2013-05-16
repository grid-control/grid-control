from python_compat import *
import sys, os, stat, time, popen2, math
from exceptions import *
from utils import AbstractObject, LoggedProcess, vprint

# placeholder for function arguments
defaultArg = object()


################################
# Process Handlers
# create interface for initializing a set of commands sharing a similar setup, e.g. remote commands through SSH

# Process Handler:
class ProcessHandler(AbstractObject):
	def LoggedProcess(self, cmd, args = '', **kwargs):
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
	# return instance of LoggedProcess with input properly wrapped
	def LoggedProcess(self, cmd, args = '', **kwargs):
		return LoggedProcess( cmd , args )

	def LoggedCopyToRemote(self, source, dest, **kwargs):
		return LoggedProcess( self.cpy, " ".join([source, dest]) )

	def LoggedCopyFromRemote(self, source, dest, **kwargs):
		return LoggedProcess( self.cpy, " ".join([source, dest]) )

	def getDomain(self):
		return "localhost"


# remote Processes via SSH
class SSHProcessHandler(ProcessHandler):
	# commands to use - overwritten by inheriting class
	cmd="ssh"
	cpy="scp -r"
	# track lifetime and quality of command socket
	socketTimestamp=0
	socketFailCount=0
	# older versions of ssh/gsissh will propagate an end of master incorrectly to children - rotate sockets
	socketIdNow=0
	def __init__(self, **kwargs):
		self.defaultArgs="-vvv -o BatchMode=yes  -o ForwardX11=no " + kwargs.get("defaultArgs","")
		self.socketArgs=""
		self.socketEnforce=kwargs.get("sshLinkEnforce",True)
		try:
			self.remoteHost = kwargs["remoteHost"]
			if not self.remoteHost:
				raise RuntimeError("No Host")
		except Exception:
			raise RethrowError("Request to initialize SSH-Type RemoteProcessHandler without remote host.")
		try:
			self.sshLinkBase=os.path.abspath(kwargs["sshLink"])
			# older ssh/gsissh a maximum length limit for control paths...
			if ( len(self.sshLinkBase)>= 107):
				self.sshLinkBase=os.path.expanduser("~/.ssh/%s"%os.path.basename(self.sshLinkBase))
			self.sshLink=self.sshLinkBase
			self._secureSSHLink(initDirectory=True)
			self._socketHandler()
		except KeyError:
			self.sshLink=False
		# test connection once
		testProcess = self.LoggedProcess( "exit" )
		if testProcess.wait() != 0:
			raise RuntimeError("Failed to validate remote connection.\n	Command: %s Return code: %s\n%s" % ( testProcess.cmd, testProcess.wait(), testProcess.getOutput() ) )

	# return instance of LoggedProcess with input properly wrapped
	def LoggedProcess(self, cmd, args = '', **kwargs):
		self._socketHandler()
		return LoggedProcess( " ".join([self.cmd, self.defaultArgs, self.socketArgs, kwargs.get('handlerArgs',""), self.remoteHost, self._argFormat(cmd + " " + args)]) )
	def _SocketProcess(self, cmd, args = '', **kwargs):
		return LoggedProcess( " ".join([self.cmd, self.defaultArgs, self.socketArgsDef, kwargs.get('handlerArgs',""), self.remoteHost, self._argFormat(cmd + " " + args)]) )
	def LoggedCopyToRemote(self, source, dest, **kwargs):
		self._socketHandler()
		return LoggedProcess( " ".join([self.cpy, self.defaultArgs, self.socketArgs, kwargs.get('handlerArgs',""), source, self._remotePath(dest)]) )
	def LoggedCopyFromRemote(self, source, dest, **kwargs):
		self._socketHandler()
		return LoggedProcess( " ".join([self.cpy, self.defaultArgs, self.socketArgs, kwargs.get('handlerArgs',""), self._remotePath(source), dest]) )

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
					print "Failed to create secure socket %s more than %s times!\nDisabling further attempts." % (self.sshLink,maxFailCount)
					self.sshLink=False

	# make sure the link file is properly protected
	# 	@sshLink:	location of the link
	#	@directory:	secure only directory (for initializing)
	def _secureSSHLink(self, initDirectory=False):
		sshLink=os.path.abspath(self.sshLink)
		sshLinkDir=os.path.dirname(self.sshLink)
		# containing directory should be secure
		if not os.path.isdir(sshLinkDir):
			try:
				os.makedirs(sshLinkDir)
			except Exception:
				if self.socketEnforce:
					raise RethrowError("Could not create or access directory for SSHLink:\n	%s" % sshLinkDir)
				else:
					return False
		if initDirectory:
			return True
		if sshLinkDir!=os.path.dirname(os.path.expanduser("~/.ssh/")):
			try:
				os.chmod(sshLinkDir,0700)
			except Exception:
				RethrowError("Could not secure directory for SSHLink:\n	%s" % sshLinkDir)
		# socket link object should be secure against manipulation if it exists
		if os.path.exists(sshLink):
			if stat.S_ISSOCK(os.stat(sshLink).st_mode):
				try:
					os.chmod(sshLink,0700)
				except Exception:
					if self.socketEnforce:
						raise RethrowError("Could not validate security of SSHLink:\n	%s\nThis is a potential security violation!" % sshLink)
					else:
						return False
			else:
				if self.socketEnforce:
					raise RuntimeError("Could not validate security of SSHLink:\n	%s\nThis is a potential security violation!" % sshLink)
				else:
					return False
		return True

	# keep a process active in the background to speed up connecting by providing an active socket
	def _refreshSSHLink(self, minSeconds=40, maxSeconds=60):
		# if there is a link, ensure it'll still live for minimum lifetime
		if os.path.exists(self.sshLink) and stat.S_ISSOCK(os.stat(self.sshLink).st_mode):
			if ( time.time() - self.socketTimestamp < maxSeconds-minSeconds ):
				return True
		# rotate socket
		socketIdMax=math.ceil(maxSeconds/(maxSeconds-minSeconds))
		if socketIdMax==self.socketIdNow:
			self.socketIdNow=0
		else:
			self.socketIdNow+=1
		self.sshLink=self.sshLinkBase+str(self.socketIdNow)
		self.socketArgsDef=" -o ControlMaster=auto  -o ControlPath=" + self.sshLink + " "
		if os.path.exists(self.sshLink):
			os.remove(self.sshLink)
		# send a dummy background process over ssh to keep the connection going
		socketProc=self._SocketProcess("sleep %s" % maxSeconds)
		timeout=0
		while not os.path.exists(self.sshLink):
			time.sleep(0.5)
			timeout+=0.5
			if timeout==6:
				vprint("SSH socket still not available after 6 seconds...\n%s" % self.sshLink, level=1)
				vprint('Socket process: %s' % (socketProc.cmd), level=2)
			if timeout==10:
				return False
		self.socketTimestamp=time.time()
		return self._secureSSHLink()

# remote Processes via GSISSH
class GSISSHProcessHandler(SSHProcessHandler):
	# commands to use - overwritten by inheriting class
	cmd="gsissh"
	cpy="gsiscp -r"

ProcessHandler.dynamicLoaderPath()
