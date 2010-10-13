import utils

# All functions use url_* functions from gc-run.lib (just like the job did...)

def se_runcmd(cmd, *urls):
	runLib = utils.pathGC('share', 'gc-run.lib')
	urlargs = str.join(' ', map(lambda x: '"%s"' % x.replace('dir://', 'file://'), urls))
	return 'source %s || exit 1; print_and_eval "%s" %s' % (runLib, cmd, urlargs)

se_rm = lambda target: utils.LoggedProcess(se_runcmd("url_rm", target))
se_mkdir = lambda target: utils.LoggedProcess(se_runcmd("url_mkdir", target))
se_exists = lambda target: utils.LoggedProcess(se_runcmd("url_exists", target))

def se_copy(src, dst, force = True):
	cmd = 'url_copy_single%s' % (('', '_force')[force])
	return utils.LoggedProcess(se_runcmd(cmd, src, dst))
