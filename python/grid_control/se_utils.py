import utils

# All functions use url_* functions from gc-run.lib (just like the job did...)

def se_runcmd(cmd, urls):
	runLib = utils.pathGC('share', 'gc-run.lib')
	urlargs = str.join(' ', map(lambda x: '"%s"' % x.replace('dir://', 'file://'), urls))
	return 'source %s || exit 1; print_and_eval "%s" %s' % (runLib, cmd, urlargs)

lambda se_rm(target) = utils.LoggedProcess(se_runcmd("url_rm", se_url(target)))

def se_copy(src, dst, force = True):
	cmd = 'url_copy_single%s' % (('', '_force')[force])
	return utils.LoggedProcess(se_runcmd(cmd, se_url(src), se_url(dst)))
