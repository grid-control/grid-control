#!/usr/bin/env python
import gcSupport, sys, os, optparse, popen2, time, random, threading
from python_compat import *
from grid_control import Job, JobDB, GCError, Config, Proxy, job_selector, job_db, storage, utils, logException

def md5sum(filename):
	m = md5()
	# use 4M blocksize:
	blocksize = 4096 * 1024
	f = open(filename, 'r')
	while True:
		s = f.read(blocksize)
		m.update(s)
		if len(s) != blocksize:
			break
	return m.hexdigest()


def main(args):
	help = \
"""
DEFAULT: The default is to download the SE file and check them with MD5 hashes.
 * In case all files are transferred sucessfully, the job is marked
   as already downloaded, so that the files are not copied again.
 * Failed transfer attempts will mark the job as failed, so that it
   can be resubmitted."""
	parser = optparse.OptionParser(usage = "%prog [options] <config file>\n" + help)

	def addBoolOpt(optList, optPostfix, dest, default, help, optShort=('', ''), optPrefix=('no', ''), helpPrefix=('do not ', '')):
		def buildLongOpt(prefix, postfix):
			if prefix and postfix:
				return '--%s-%s' % (prefix, postfix)
			elif prefix and not postfix:
				return '--' + prefix
			else:
				return '--' + postfix
		optList.add_option(optShort[True], buildLongOpt(optPrefix[True], optPostfix), dest=dest,
			default=default, action='store_true', help=helpPrefix[True] + help + ('', ' [Default]')[default])
		optList.add_option(optShort[False], buildLongOpt(optPrefix[False], optPostfix), dest=dest,
			default=default, action='store_false', help=helpPrefix[False] + help + (' [Default]', '')[default])

	addBoolOpt(parser, 'verify-md5', dest='verify',       default=True,  optShort=('', '-v'),
		help='MD5 verification of SE files', helpPrefix=('disable ', 'enable '))
	addBoolOpt(parser, 'loop',       dest='loop',         default=False, optShort=('', '-l'),
		help='loop over jobs until all files are successfully processed')
	addBoolOpt(parser, 'infinite',   dest='infinite',     default=False, optShort=('', '-L'),
		help='process jobs in an infinite loop')
	addBoolOpt(parser, 'shuffle',    dest='shuffle',      default=False,
		help='shuffle download order')
	addBoolOpt(parser, '',           dest='skipExisting', default=False, optPrefix=('overwrite', 'skip-existing'),
		help='files which are already on local disk', helpPrefix=('overwrite ', 'skip '))

	ogFlags = optparse.OptionGroup(parser, 'Job state / flag handling', '')
	addBoolOpt(ogFlags, 'mark-dl',   dest='markDL',       default=True,
		help='mark sucessfully downloaded jobs as such')
	addBoolOpt(ogFlags, 'mark-dl',   dest='markIgnoreDL', default=False, optPrefix=('use', 'ignore'),
		help='mark about sucessfully downloaded jobs', helpPrefix=('use ', 'ignore '))
	addBoolOpt(ogFlags, 'mark-fail', dest='markFailed',   default=True,
		help='mark jobs failing verification as such')
	addBoolOpt(ogFlags, 'mark-empty-fail', dest='markEmptyFailed', default=False,
		help='mark jobs without any files as failed')
	parser.add_option_group(ogFlags)

	ogFiles = optparse.OptionGroup(parser, 'Local / SE file handling', '')
	for (optPostfix, dest, help, default) in [
			('local-ok',   'rmLocalOK',   'files of successful jobs in local directory', False),
			('local-fail', 'rmLocalFail', 'files of failed jobs in local directory', False),
			('se-ok',      'rmSEOK',      'files of successful jobs on SE', False),
			('se-fail',    'rmSEFail',    'files of failed jobs on the SE', False),
		]:
		addBoolOpt(ogFiles, optPostfix, dest=dest, default=default, optPrefix=('keep', 'rm'),
			help=help, helpPrefix=('keep ', 'remove '))
	parser.add_option_group(ogFiles)

	parser.add_option('-o', '--output',   dest='output', default=None,
		help='specify the local output directory')
	parser.add_option('-P', '--proxy',    dest='proxy',  default='VomsProxy',
		help='specify the proxy type used to determine ability to download - VomsProxy or TrivialProxy')
	parser.add_option('-S', '--selectSE', dest='selectSE',  default=None, action='append',
		help='specify the SE paths to process')
	parser.add_option('-r', '--retry',    dest='retry',  default=0,
		help='how often should a transfer be attempted [Default: 0]')
	parser.add_option('-t', '--threads',  dest='threads',  default=0, type=int,
		help='how many parallel download threads should be used to download files [Default: no multithreading]')
	parser.add_option('', '--slowdown',   dest='slowdown', default=2,
		help='specify time between downloads [Default: 2 sec]')
	parser.add_option('', '--show-host',  dest='showHost', default=False, action='store_true',
		help='show SE hostname during download')

	# Shortcut options
	def withoutDefaults(opts):
		def isDefault(opt):
			return (parser.get_option(opt).default and parser.get_option(opt).action == 'store_true') or \
				(not parser.get_option(opt).default and parser.get_option(opt).action == 'store_false')
		return str.join(' ', filter(lambda x: not isDefault(x), opts.split()))

	ogShort = optparse.OptionGroup(parser, 'Shortcuts', '')
	optMove = '--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail --rm-se-fail --rm-local-fail --rm-se-ok --keep-local-ok'
	ogShort.add_option('-m', '--move', dest='shMove', default=None, action='store_const', const=optMove,
		help = 'Move files from SE - shorthand for:'.ljust(100) + withoutDefaults(optMove))

	optCopy = '--verify-md5 --overwrite --mark-dl --use-mark-dl --mark-fail --rm-se-fail --rm-local-fail --keep-se-ok --keep-local-ok'
	ogShort.add_option('-c', '--copy', dest='shCopy', default=None, action='store_const', const=optCopy,
		help = 'Copy files from SE - shorthand for:'.ljust(100) + withoutDefaults(optCopy))

	optJCopy = '--verify-md5 --skip-existing --no-mark-dl --ignore-mark-dl --no-mark-fail --keep-se-fail --keep-local-fail --keep-se-ok --keep-local-ok'
	ogShort.add_option('-j', '--just-copy', dest='shJCopy', default=None, action='store_const', const=optJCopy,
		help = 'Just copy files from SE - shorthand for:'.ljust(100) + withoutDefaults(optJCopy))

	optSCopy = '--verify-md5 --mark-dl --mark-fail --rm-se-fail --rm-local-fail --keep-se-ok --keep-local-ok'
	ogShort.add_option('-s', '--smart-copy', dest='shSCopy', default=None, action='store_const', const=optSCopy,
		help = 'Copy correct files from SE, but remember already downloaded files and delete corrupt files - shorthand for: '.ljust(100) + withoutDefaults(optSCopy))

	optJVerify = '--verify-md5 --no-mark-dl --keep-se-fail --rm-local-fail --keep-se-ok --rm-local-ok --ignore-mark-dl'
	ogShort.add_option('-V', '--just-verify', dest='shJVerify', default=None, action='store_const', const=optJVerify,
		help = 'Just verify files on SE - shorthand for:'.ljust(100) + withoutDefaults(optJVerify))

	optJDelete = '--skip-existing --rm-se-fail --rm-se-ok --rm-local-fail --keep-local-ok --no-mark-dl --ignore-mark-dl'
	ogShort.add_option('-D', '--just-delete', dest='shJDelete', default=None, action='store_const', const=optJDelete,
		help = 'Just delete all finished files on SE - shorthand for:'.ljust(100) + withoutDefaults(optJDelete))
	parser.add_option_group(ogShort)

	(opts, args) = parser.parse_args()
	def processShorthand(optSet):
		if optSet:
			parser.parse_args(args = optSet.split() + sys.argv[1:], values = opts)
	processShorthand(opts.shMove)
	processShorthand(opts.shCopy)
	processShorthand(opts.shJCopy)
	processShorthand(opts.shSCopy)
	processShorthand(opts.shJVerify)
	processShorthand(opts.shJDelete)

	# Disable loop mode if it is pointless
	if (opts.loop and not opts.skipExisting) and (opts.markIgnoreDL or not opts.markDL):
		sys.stderr.write('Loop mode was disabled to avoid continuously downloading the same files\n')
		(opts.loop, opts.infinite) = (False, False)

	# we need exactly one positional argument (config file)
	if len(args) != 1:
		sys.stderr.write('usage: %s [options] <config file>\n\n' % os.path.basename(sys.argv[0]))
		sys.stderr.write('Config file not specified!\n')
		sys.stderr.write('Use --help to get a list of options!\n')
		sys.exit(0)

	while True:
		try:
			if (realmain(opts, args) or not opts.loop) and not opts.infinite:
				break
			time.sleep(60)
		except KeyboardInterrupt:
			raise
			print '\n\nDownload aborted!\n'
			sys.exit(1)


def dlfs_rm(path, msg):
	procRM = storage.se_rm(path)
	if procRM.wait() != 0:
		print '\t\tUnable to remove %s!' % msg
		utils.eprint('%s\n\n' % procRM.getMessage())


def realmain(opts, args):
	try:
		proxy = Proxy.open(opts.proxy, Config(configDict={'proxy': {'ignore warnings': 'True'}}))
	except:
		sys.stderr.write(logException())
		sys.exit(1)

	(workDir, config, jobDB) = gcSupport.initGC(args)
	jobList = jobDB.getJobs(job_selector.ClassSelector(job_db.JobClass.SUCCESS))

	# Create SE output dir
	if not opts.output:
		opts.output = os.path.join(workDir, 'se_output')
	if '://' not in opts.output:
		opts.output = 'file:///%s' % os.path.abspath(opts.output)

	infos = {}
	def incInfo(x):
		infos[x] = infos.get(x, 0) + 1

	def processSingleJob(jobNum, output):
		output.init(jobNum)
		job = jobDB.get(jobNum)
		# Only run over finished and not yet downloaded jobs
		if job.state != Job.SUCCESS:
			output.error('Job has not yet finished successfully!')
			return incInfo('Processing')
		if job.get('download') == 'True' and not opts.markIgnoreDL:
			output.error('All files already downloaded!')
			return incInfo('Downloaded')
		retry = int(job.get('download attempt', 0))
		failJob = False

		if not proxy.canSubmit(20*60, True):
			sys.stderr.write('Please renew grid proxy!')
			sys.exit(1)

		# Read the file hash entries from job info file
		files = gcSupport.getFileInfo(workDir, jobNum, lambda retCode: retCode == 0)
		output.files(files)
		if not files:
			if opts.markEmptyFailed:
				failJob = True
			else:
				return incInfo('No files for job ' + str(jobNum))

		for (fileIdx, fileInfo) in enumerate(files):
			(hash, name_local, name_dest, pathSE) = fileInfo
			output.file(fileIdx)

			# Copy files to local folder
			outFilePath = os.path.join(opts.output, name_dest)
			if opts.selectSE:
				if not (True in map(lambda s: s in pathSE, opts.selectSE)):
					output.error('skip file because it is not located on selected SE!')
					return
			if opts.skipExisting and (storage.se_exists(outFilePath) == 0):
				output.error('skip file as it already exists!')
				return
			if storage.se_exists(os.path.dirname(outFilePath)).wait() != 0:
				storage.se_mkdir(os.path.dirname(outFilePath)).wait()

			checkPath = 'file:///tmp/dlfs.%s' % name_dest
			if 'file://' in outFilePath:
				checkPath = outFilePath

			def monitorFile(path, lock, abort):
				path = path.replace('file://', '')
				(csize, osize, stime, otime, lttime) = (0, 0, time.time(), time.time(), time.time())
				while not lock.acquire(False): # Loop until monitor lock is available
					if csize != osize:
						lttime = time.time()
					if time.time() - lttime > 5*60: # No size change in the last 5min!
						output.error('Transfer timeout!')
						abort.acquire()
						break
					if os.path.exists(path):
						csize = os.path.getsize(path)
						output.file(fileIdx, csize, osize, stime, otime)
						(osize, otime) = (csize, time.time())
					else:
						stime = time.time()
					time.sleep(0.1)
				lock.release()

			copyAbortLock = threading.Lock()
			monitorLock = threading.Lock()
			monitorLock.acquire()
			monitor = utils.gcStartThread('Download monitor %s' % jobNum,
				monitorFile, checkPath, monitorLock, copyAbortLock)
			result = -1
			procCP = storage.se_copy(os.path.join(pathSE, name_dest), outFilePath, tmp = checkPath)
			while True:
				if not copyAbortLock.acquire(False):
					monitor.join()
					break
				copyAbortLock.release()
				result = procCP.poll()
				if result != -1:
					monitorLock.release()
					monitor.join()
					break
				time.sleep(0.02)

			if result != 0:
				output.error('Unable to copy file from SE!')
				output.error(procCP.getMessage())
				failJob = True
				break

			# Verify => compute md5hash
			if opts.verify:
				try:
					hashLocal = md5sum(checkPath.replace('file://', ''))
					if not ('file://' in outFilePath):
						dlfs_rm('file://%s' % checkPath, 'SE file')
				except KeyboardInterrupt:
					raise
				except:
					hashLocal = None
				output.hash(fileIdx, hashLocal)
				if hash != hashLocal:
					failJob = True
			else:
				output.hash(fileIdx)

		# Ignore the first opts.retry number of failed jobs
		if failJob and opts.retry and (retry < opts.retry):
			output.error('Download attempt #%d failed!' % (retry + 1))
			job.set('download attempt', str(retry + 1))
			jobDB.commit(jobNum, job)
			return incInfo('Download attempts')

		for (fileIdx, fileInfo) in enumerate(files):
			(hash, name_local, name_dest, pathSE) = fileInfo
			# Remove downloaded files in case of failure
			if (failJob and opts.rmLocalFail) or (not failJob and opts.rmLocalOK):
				output.status(fileIdx, 'Deleting file %s from local...' % name_dest)
				outFilePath = os.path.join(opts.output, name_dest)
				if storage.se_exists(outFilePath).wait() == 0:
					dlfs_rm(outFilePath, 'local file')
			# Remove SE files in case of failure
			if (failJob and opts.rmSEFail)    or (not failJob and opts.rmSEOK):
				output.status(fileIdx, 'Deleting file %s...' % name_dest)
				dlfs_rm(os.path.join(pathSE, name_dest), 'SE file')
			output.status(fileIdx, None)

		if failJob:
			incInfo('Failed downloads')
			if opts.markFailed:
				# Mark job as failed to trigger resubmission
				job.state = Job.FAILED
		else:
			incInfo('Successful download')
			if opts.markDL:
				# Mark as downloaded
				job.set('download', 'True')

		# Save new job status infos
		jobDB.commit(jobNum, job)
		output.finish()
		time.sleep(float(opts.slowdown))

	if opts.shuffle:
		random.shuffle(jobList)
	else:
		jobList.sort()

	if opts.threads:
		from grid_control_gui import ansi_console
		errorOutput = []
		class ThreadDisplay:
			def __init__(self):
				self.output = []
			def init(self, jobNum):
				self.jobNum = jobNum
				self.output = ['Job %5d' % jobNum, '']
			def infoline(self, fileIdx, msg = ''):
				return 'Job %5d [%i/%i] %s %s' % (self.jobNum, fileIdx + 1, len(self.files), self.files[fileIdx][2], msg)
			def files(self, files):
				(self.files, self.output, self.tr) = (files, self.output[1:], ['']*len(files))
				for x in range(len(files)):
					self.output.insert(2*x, self.infoline(x))
					self.output.insert(2*x+1, '')
			def file(self, idx, csize = None, osize = None, stime = None, otime = None):
				(hash, name_local, name_dest, pathSE) = self.files[idx]
				if otime:
					trfun = lambda sref, tref: gcSupport.prettySize(((csize - sref) / max(1, time.time() - tref)))
					self.tr[idx] = '%7s avg. - %7s/s inst.' % (gcSupport.prettySize(csize), trfun(0, stime))
					self.output[2*idx] = self.infoline(idx, '(%s - %7s/s)' % (self.tr[idx], trfun(osize, otime)))
			def hash(self, idx, hashLocal = None):
				(hash, name_local, name_dest, pathSE) = self.files[idx]
				if hashLocal:
					if hash == hashLocal:
						result = ansi_console.Console.fmt('MATCH', [ansi_console.Console.COLOR_GREEN])
					else:
						result = ansi_console.Console.fmt('FAIL', [ansi_console.Console.COLOR_RED])
					msg = '(R:%s L:%s) => %s' % (hash, hashLocal, result)
				else:
					msg = ''
				self.output[2*idx] = self.infoline(idx, '(%s)' % self.tr[idx])
				self.output[2*idx+1] = msg
				print self, repr(msg)
			def error(self, msg):
				errorOutput.append(msg)
			def write(self, msg):
				self.output.append(msg)
			def status(self, idx, msg):
				if msg:
					self.output[2*idx] = self.infoline(idx, '(%s)' % self.tr[idx]) + ' ' + msg
				else:
					self.output[2*idx] = self.infoline(idx, '(%s)' % self.tr[idx])
			def finish(self):
#				self.output.append(str(self.jobNum) + 'FINISHED')
				pass

		(active, todo) = ([], list(jobList))
		todo.reverse()
		screen = ansi_console.Console()
		screen.move(0, 0)
		screen.savePos()
		while True:
			screen.erase()
			screen.loadPos()
			active = filter(lambda (t, d): t.isAlive(), active)
			while len(active) < opts.threads and len(todo):
				display = ThreadDisplay()
				active.append((utils.gcStartThread('Download %s' % todo[-1],
					processSingleJob, todo.pop(), display), display))
			for (t, d) in active:
				sys.stdout.write(str.join('\n', d.output))
			sys.stdout.write(str.join('\n', ['=' * 50] + errorOutput))
			sys.stdout.flush()
			if len(active) == 0:
				break
			time.sleep(0.01)
	else:
		class DefaultDisplay:
			def init(self, jobNum):
				sys.stdout.write('Job %d: ' % jobNum)
			def files(self, files):
				self.files = files
				sys.stdout.write('The job wrote %d file%s to the SE\n' % (len(files), ('s', '')[len(files) == 1]))
			def file(self, idx, csize = None, osize = None, stime = None, otime = None):
				(hash, name_local, name_dest, pathSE) = self.files[idx]
				if otime:
					tr = lambda sref, tref: gcSupport.prettySize(((csize - sref) / max(1, time.time() - tref)))
					tmp = name_dest
					if opts.showHost:
						tmp += ' [%s]' % pathSE.split('//')[-1].split('/')[0].split(':')[0]
					self.write('\r\t%s (%7s - %7s/s avg. - %7s/s inst.)' % (tmp,
						gcSupport.prettySize(csize), tr(0, stime), tr(osize, otime)))
					sys.stdout.flush()
				else:
					self.write('\t%s' % name_dest)
					sys.stdout.flush()
			def hash(self, idx, hashLocal = None):
				(hash, name_local, name_dest, pathSE) = self.files[idx]
				self.write(' => %s\n' % ('\33[0;91mFAIL\33[0m', '\33[0;92mMATCH\33[0m')[hash == hashLocal])
				self.write('\t\tRemote site: %s\n' % hash)
				self.write('\t\t Local site: %s\n' % hashLocal)
			def error(self, msg):
				sys.stdout.write('\nJob %d: %s' % (jobNum, msg.strip()))
			def status(self, idx, msg):
				if msg:
					self.write('\t' + msg + '\r')
				else:
					self.write(' ' * len('\tDeleting file %s from SE...\r' % self.files[idx][2]) + '\r')
			def write(self, msg):
				sys.stdout.write(msg)
			def finish(self):
				sys.stdout.write('\n')

		for jobNum in jobList:
			processSingleJob(jobNum, DefaultDisplay())

	# Print overview
	if infos:
		print '\nStatus overview:'
		for (state, num) in infos.items():
			if num > 0:
				print '%20s: [%d/%d]' % (state, num, len(jobList))
		print

	if ('Downloaded' in infos) and (infos['Downloaded'] == len(jobDB)):
		return True
	return False

if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
