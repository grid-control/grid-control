from __future__ import generators
import sys, os, random, popen2, tempfile
from grid_control import ConfigError, WMS, Job, utils

class LSF(WMS):
	__rndChars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

	_statusMap = {
		'PEND':	Job.QUEUED,
		'RUN':	Job.RUNNING,
		'aborted':	Job.ABORTED,
		'cancelled':	Job.CANCELLED,
		'EXIT':	Job.FAILED,
		'DONE':		Job.DONE,
		'cleared':	Job.SUCCESS
	}

	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, init)

		self._submitExec = utils.searchPathFind('bsub')
		self._statusExec = utils.searchPathFind('bjobs')
		self._cancelExec = utils.searchPathFind('bkill')
		self._queue = config.get('lsf', 'queue', '')

		self._tmpPath = os.path.join(self._outputPath, 'tmp')

		random.seed()


	def submitJob(self, id, job):
		try:
			if not os.path.exists(self._tmpPath):
				os.mkdir(self._tmpPath)
		except IOError:
			raise RuntimeError("Temporary path '%s' could not be created." % self._tmpPath)
		#log = tempfile.mktemp('.log')
		
		activity = utils.ActivityLog('submitting jobs')

		params = ''
		if len(self._queue):
			params = ' -q %s' % self._queue

		#tarFile = os.path.join(self.workDir, 'sandbox.tar.gz')	
		agrument = ''	
		argument = '%d LSF %s %s' % (id,self.workDir,self.module.getJobArguments(id))
		
		name = '-J GCtrl.' 
		##	map(lambda x: random.choice(self.__rndChars),
		##	    xrange(0, 8))
		job.set('name', name)

		#outPath = os.path.join(self._tmpPath, name + '.stdout.txt')
		#errPath = os.path.join(self._tmpPath, name + '.stderr.txt')

		outPath = os.path.join(self._outputPath, 'tmp/job_%d/' % id)
		
		try:
			if not os.path.exists(outPath):
				os.mkdir(outPath)

		except IOError:
			raise RunTimeError("Could not create temporary output dirs '%s'") % (self._outPath)
		

		
		executable = utils.atRoot('share', 'run.sh')

		##proc = popen2.Popen3("%s%s -N %s -o %s -e %s %s"
		##                     % (self._submitExec, params, name
		##                        utils.shellEscape(outPath),
		##                        utils.shellEscape(errPath),
		##                        utils.shellEscape(executable)),
		##                     True)

		proc = popen2.Popen3("%s%s %s -o %s/stdout.txt -e %s/stderr.txt %s %s"
		                     % (self._submitExec, params, name, outPath, \
					outPath,\
					utils.shellEscape(executable),argument),
		                     True)
		
		id = None

		lines = proc.fromchild.readlines()
		for line in lines:
		 id = line.split("<")[1].split(">")[0]

		retCode = proc.wait()

		if retCode != 0:
			print >> sys.stderr, "WARNING: bsub failed:"
		elif id == None:
			print >> sys.stderr, "WARNING: bsub did not yield job id:"

		if id == None:
			#for line in open(log, 'r'):
			#	sys.stderr.write(line)
			print >> sys.stderr, "ERROR: NO JOB ID!"
			exit

		return id


	def checkJobs(self, ids):
		if not len(ids):
			return []
		result=[]
		try:
			activity = utils.ActivityLog("checking job status")

			for tid in ids:
				#print tid
				proc = popen2.Popen4("%s %s"
						     % (self._statusExec,tid),
						     True)

				for line in proc.fromchild.readlines():
					if (line.split(' ')[0]=="No"):
						print "ERROR"
						beh = 'error'
						status=self._statusMap[beh]
						data = {}
						result.append((tid,status,data))
					elif (line.split(' ')[0]=="JOBID"):
					#print "first LINE"
						continue
					else:
						#print "REAL INFO"
						items=line.split()
						id=items[0];
						if tid!=id:
							print "BUH"
							#print >> sys.stderr, "ERROR: IDS do not MATCH! %s : %s",(tid,id)
							#raise RunTimeError()
						status=self._statusMap[items[2]]
						data={}
						result.append((tid,status,data))

				retCode = proc.wait()

				if retCode != 0:
				#FIXME
					print >> sys.stderr, "WARNING: glite-job-status failed:"
					for line in open(log, 'r'):
						sys.stderr.write(line)

		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass
			
			return result


			
		
	def getJobsOutput(self, ids):
		if not len(ids):
			return []

		try:
			outPath = os.path.join(self._outputPath, 'tmp')
			if not os.path.exists(outPath):
				raise RuntimeError("Temporary path '%s' with job output could not be found." % outPath)

			
			fd, jobs = tempfile.mkstemp('.jobids','',outPath)
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jobs = tempfile.mktemp('.jobids','',outPath)
				try:
					fd = os.open(jobs, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break
			

		log = tempfile.mktemp('.log')

		result = []
		
			
		try:
			fp = os.fdopen(fd , 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()
			# FIXME: error handling
			
			activity = utils.ActivityLog("retrieving job outputs")
						
			del activity
			
					
			for file in os.listdir(outPath):
				path = os.path.join(outPath, file)
				if os.path.isdir(path):
					result.append(path)
			#print "RESULT:",result		
					
	       	finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass
			try:
				os.rmdir(tmpPath)
			except:
				pass
			
		return result
						
	def cancel(self, ids):
		if not len(ids):
			return True

		try:
			fd, jobs = tempfile.mkstemp('.jobids')
		except AttributeError:	# Python 2.2 has no tempfile.mkstemp
			while True:
				jobs = tempfile.mktemp('.jobids')
				try:
					fd = os.open(jobs, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
				except OSError:
					continue
				break

		log = tempfile.mktemp('.log')

		result = []

		try:
			fp = os.fdopen(fd, 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()
			# FIXME: error handling

			activity = utils.ActivityLog("cancelling jobs")

			for id in ids:
				proc = popen2.Popen3("%s %s"
						     % (self._cancelExec,
							id), True)

				retCode = proc.wait()
				if retCode != 0:
				#FIXME
				      print >> sys.stderr, "WARNING: %s failed:" % os.path.basename(self._cancelExec)
				      return False

			del activity



		finally:
			try:
				os.unlink(jobs)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass

		return True
	
