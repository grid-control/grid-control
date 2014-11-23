#-#  Copyright 2013-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

import os, math, stat, time
from grid_control.abstract import LoadableObject
from grid_control.exceptions import AbstractError, GCError, RethrowError, RuntimeError, TimeoutError, TimeoutError
from grid_control.utils import LoggedProcess, eprint, resolveInstallPath, vprint

# placeholder for function arguments
defaultArg = object()

################################
# Process Handlers
# create interface for initializing a set of commands sharing a similar setup, e.g. remote commands through SSH

# Process Handler:
class ProcessHandler(LoadableObject):
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
			raise RethrowError("Request to initialize SSH-Type RemoteProcessHandler without remote host.")
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
		testProcess = self.LoggedProcess( "exit" )
		if testProcess.wait() != 0:
			bla = testProcess.getError() 
			raise RuntimeError("Failed to validate remote connection.\n	Command: %s Return code: %s\n%s" % ( testProcess.cmd, testProcess.wait(), testProcess.getOutput() ) )
	def __initcommands(self, **kwargs):
		self.cmd = resolveInstallPath("ssh")
		self.cpy = resolveInstallPath("scp") + " -r"

	# return instance of LoggedProcess with input properly wrapped
	def LoggedProcess(self, cmd, args = '', **kwargs):
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
		self.__ControlMaster = LoggedProcess( " ".join([self.cmd, self.defaultArgs, "-o ControlMaster=yes", self.socketArgsDef, self.remoteHost, self._argFormat("sleep %d" % duration)]) )
		timeout = 0
		while not os.path.exists(self.sshLink):
			time.sleep(0.5)
			timeout += 0.5
			if timeout == 5:
				vprint("SSH socket still not available after 5 seconds...\n%s" % self.sshLink, level=1)
				vprint('Socket process: %s' % (socketProc.cmd), level=2)
			if timeout == 10:
				return False
	def _CleanSocket(self):
		if not os.path.exists(self.sshLink):
			print "No Socket %s" % self.sshLink
			return True
		print "Killing Socket %s" % self.sshLink
		#killSocket = LoggedProcess( " ".join([self.cmd, self.defaultArgs, self.socketArgsDef, "-O exit", self.remoteHost]) )
		#while killSocket.poll() == -1:
			#print "poll", killSocket.poll()
			#time.sleep(0.5)
			#timeout += 0.5
			#if timeout == 5:
				#vprint("Failed to cancel ssh Socket...\n%s" % self.sshLink, level=1)
				#return False
		#print "done", killSocket.poll()
		timeout = 0
		while os.path.exists(self.sshLink):
			print "exists %d" % timeout
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
					raise RethrowError("Could not create or access directory for SSHLink:\n	%s" % sshLinkDir)
				else:
					return False
		if sshLinkDir!=os.path.dirname(os.path.expanduser("~/.ssh/")):
			try:
				os.chmod(sshLinkDir, stat.S_IRWXU)
			except Exception:
				if self.socketEnforce:
					raise RethrowError("Could not secure directory for SSHLink:\n	%s" % sshLinkDir)
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
						raise RethrowError("Could not secure SSHLink:\n	%s" % sshLink)
					else:
						return False
			else:
				if self.socketEnforce:
					raise RuntimeError("Non-socket object already exists for SSHLink:\n	%s" % sshLink)
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
		socketProc = self._CreateSocket(maxSeconds)
		self.socketTimestamp = time.time()
		return self._secureSSHLink()

# remote Processes via GSISSH
class GSISSHProcessHandler(SSHProcessHandler):
	# commands to use - overwritten by inheriting class
	def __initcommands(self, **kwargs):
		cmd = resolveInstallPath("gsissh")
		cpy = resolveInstallPath("gsiscp") + " -r"
