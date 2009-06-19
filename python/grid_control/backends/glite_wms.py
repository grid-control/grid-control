from __future__ import generators
import sys, os, time, copy, popen2, tempfile, cStringIO, md5
from grid_control import ConfigError, Job, utils
from grid_wms import GridWMS
from glite import Glite

try:
	from email.utils import parsedate
except ImportError:
	from email.Utils import parsedate

class GliteWMS(Glite):
	def __init__(self, module):
		GridWMS.__init__(self, module)

		self._delegateExec = utils.searchPathFind('glite-wms-job-delegate-proxy')
		self._submitExec = utils.searchPathFind('glite-wms-job-submit')
		self._statusExec = utils.searchPathFind('glite-wms-job-status')
		self._outputExec = utils.searchPathFind('glite-wms-job-output')
		self._cancelExec = utils.searchPathFind('glite-wms-job-cancel')

		self._configVO = module.config.getPath('glite-wms', 'config', '')
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config file '%s' does not exist." % self._configVO)

		try:
			self._ce = module.config.get('glite-wms', 'ce')
		except:
			self._ce = None


	def bulkSubmissionBegin(self):
		log = tempfile.mktemp('.log')

		try:
			params = ''

			if self._configVO != '':
				params += ' --config %s' % utils.shellEscape(self._configVO)

			activity = utils.ActivityLog('creating delegate proxy for job submission')

			proc = popen2.Popen4("%s%s -a --noint --logfile %s"
			                     % (self._delegateExec, params,
			                        utils.shellEscape(log)), True)

			self._jobDelegationID = None
			lines = proc.fromchild.readlines()

			for line in lines:
				line = line.strip()
				try:
					(left, right) = line.split(':', 1)
					if left.endswith('identifier'):
						self._jobDelegationID = right.strip()
				except:
					pass
			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: glite-wms-job-delegate-proxy failed (%d)" % retCode
				print >> sys.stderr, lines
			elif id == None:
				print >> sys.stderr, "WARNING: glite-wms-job-delegate-proxy did not yield a proxy id:"

			if id == None and os.path.exists(log):
				sys.stderr.write(open(log, 'r').read())

			# FIXME: glite-wms-job-delegate-proxy

		finally:
			try:
				os.unlink(jdl)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass


	def submitJob(self, id, job):
		if self._jobDelegationID == None:
			return None

		fd, jdl = tempfile.mkstemp('.jdl')
		log = tempfile.mktemp('.log')

		try:
			data = cStringIO.StringIO()
			self.makeJDL(data, id)
			data = data.getvalue()

			job.set('jdl', data)

			fp = os.fdopen(fd, 'w')
			fp.write(data)
			fp.close()

			params = ''

			if self._configVO != '':
				params += ' --config %s' % utils.shellEscape(self._configVO)

			if self._ce != None:
				params += ' -r %s' % utils.shellEscape(self._ce)

			activity = utils.ActivityLog('submitting jobs')

			time.sleep(1)
			
			proc = popen2.Popen4("%s%s -d %s --nomsg --noint --logfile %s %s"
			                     % (self._submitExec, params,
			                        self._jobDelegationID,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jdl)), True)

			id = None
			lines = proc.fromchild.readlines()
			for line in map(str.strip, lines):
				if line.startswith('http'):
					id = line
			retCode = proc.wait()

			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: glite-wms-job-submit failed (%d):" % retCode
				rberr = False;
				if os.path.exists(log):
					if open(log, 'r').read().find("<ErrorCode>1228</ErrorCode>") != -1:
						rberr = True;
				else:
					print >> sys.stderr, "No logfile to display!"
					print >> sys.stderr, str.join('\n', lines)
					print >> sys.stderr, open(jdl, 'r').read()
				if rberr:
					print >> sys.stderr, "RB is overloaded!"
					return id
			elif id == None:
				print >> sys.stderr, "WARNING: glite-wms-job-submit did not yield job id"

			if id == None and os.path.exists(log):
				sys.stderr.write(open(log, 'r').read())

			# FIXME: glite-wms-job-submit
			return id

		finally:
			try:
				os.unlink(jdl)
			except:
				pass
			try:
				os.unlink(log)
			except:
				pass


	def getJobsOutput(self, ids):
		if not len(ids):
			return []

		tmpPath = os.path.join(self._outputPath, 'tmp')
		try:
			if not os.path.exists(tmpPath):
				os.mkdir(tmpPath)
		except IOError:
			raise RuntimeError("Temporary path '%s' could not be created." % tmpPath)

		fd, jobs = tempfile.mkstemp('.jobids')
		log = tempfile.mktemp('.log')

		result = []

		try:
			fp = os.fdopen(fd, 'w')
			for id in ids:
				fp.write("%s\n" % id)
			fp.close()

			if len(ids) == 1:
				wmsExtraDir = md5.md5(ids[0]).hexdigest()
				outPath = os.path.join(tmpPath, wmsExtraDir)
				if not os.path.exists(outPath):
					os.mkdir(outPath)
			else:
				outPath = tmpPath


			# FIXME: error handling

			activity = utils.ActivityLog("retrieving job outputs")

			proc = popen2.Popen4("%s --noint --logfile %s -i %s --dir %s"
			                     % (self._outputExec,
			                        utils.shellEscape(log),
			                        utils.shellEscape(jobs),
			                        utils.shellEscape(outPath)),
			                        True)
			for data in proc.fromchild.readlines():
				# FIXME: moep
				pass

			retCode = proc.wait()

			
			del activity

			if retCode != 0:
				#FIXME
				print >> sys.stderr, "WARNING: glite-wms-job-output failed (%d):" % retCode
				if os.path.exists(log):
					sys.stderr.write(open(log,'r').read().replace("-----------------------------------------\n",""))

			for file in os.listdir(tmpPath):
				path = os.path.join(tmpPath, file)
				if os.path.isdir(path):
					result.append(path)

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
