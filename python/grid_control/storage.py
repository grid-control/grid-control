import utils

# All functions use url_* functions from gc-run.lib (just like the job did...)

def se_runcmd(cmd, varDict = {}, *urls):
	runLib = utils.pathGC('share', 'gc-run.lib')
	urlargs = str.join(' ', map(lambda x: '"%s"' % x.replace('dir://', 'file://'), urls))
	varString = str.join(' ', map(lambda x: 'export %s="%s";' % (x, varDict[x]), varDict))
	return 'source %s || exit 1; %s print_and_eval "%s" %s' % (runLib, varString, cmd, urlargs)

se_rm = lambda target: utils.LoggedProcess(se_runcmd("url_rm", {}, target))
se_mkdir = lambda target: utils.LoggedProcess(se_runcmd("url_mkdir", {}, target))
se_exists = lambda target: utils.LoggedProcess(se_runcmd("url_exists", {}, target))

def se_copy(src, dst, force = True, tmp = ''):
	cmd = 'url_copy_single%s' % (('', '_force')[force])
	return utils.LoggedProcess(se_runcmd(cmd, {'GC_KEEPTMP': tmp}, src, dst))
