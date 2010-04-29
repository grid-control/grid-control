import utils

# All functions use url_* functions from run.lib (just like the job did...)

def getRunLibCmd(*cmds):
	runLib = utils.pathGC('share', 'run.lib')
	cmd = 'print_and_%seval %s' % (('', 'q')[utils.verbosity() == 0], str.join(' ', cmds))
	return 'source %s || exit 1; %s' % (runLib, cmd)


def se_rm(target):
	target = target.replace('dir://', 'file://')
	return utils.LoggedProcess(getRunLibCmd('"url_rm" "%s"' % target))


def se_copy(src, dst, force = True):
	src = src.replace('dir://', 'file://')
	dst = dst.replace('dir://', 'file://')
	cmd = "url_copy_single%s" % (('', '_force')[force])
	return utils.LoggedProcess(getRunLibCmd('"%s" "%s" "%s"' % (cmd, src, dst)))
