import os
from grid_control import ConfigError, WMS, Glite, utils

class LCG(Glite):

	def __init__(self, config, module, init):
		WMS.__init__(self, config, module, init)

		self._submitExec = utils.searchPathFind('edg-job-submit')
		self._statusExec = utils.searchPathFind('edg-job-status')
		self._outputExec = utils.searchPathFind('edg-job-get-output')
		self._cancelExec = utils.searchPathFind('edg-job-cancel')

		self._configVO = config.getPath('lcg', 'config-vo', '')
		if self._configVO != '' and not os.path.exists(self._configVO):
			raise ConfigError("--config-vo file '%s' does not exist." % self._configVO)

		try:
			self._ce = config.get('lcg', 'ce')
		except:
			self._ce = None


        def cancel(self, ids):
                if not len(ids):
                        return True

                try:
                        fd, jobs = tempfile.mkstemp('.jobids')
                except AttributeError:  # Python 2.2 has no tempfile.mkstemp
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

                        proc = popen2.Popen3("%s --noint --logfile %s -i %s"
                                             % (self._cancelExec,
                                                utils.shellEscape(log),
                                                utils.shellEscape(jobs)), True)

                        retCode = proc.wait()

                        del activity

                        if retCode != 0:
                                #FIXME
                                print >> sys.stderr, "WARNING: edg-job-cancel failed:"
                                for line in open(log, 'r'):
                                        sys.stderr.write(line)

                                return False

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

