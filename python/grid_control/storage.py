import os, utils

# All functions use url_* functions from gc-run.lib (just like the job did...)

ensurePrefix = lambda fn: utils.QM('://' in fn, fn, 'file://%s' % os.path.abspath(fn).lstrip('/'))

def se_runcmd(cmd, varDict = {}, *urls):
	runLib = utils.pathGC('share', 'gc-run.lib')
	args = str.join(' ', map(lambda x: '"%s"' % ensurePrefix(x).replace('dir://', 'file://'), urls))
	varString = str.join(' ', map(lambda x: 'export %s="%s";' % (x, varDict[x]), varDict))
	return utils.LoggedProcess('source %s || exit 1; %s %s %s' % (runLib, varString, cmd, args))

se_ls = lambda target: se_runcmd('url_ls', {}, target)
se_rm = lambda target: se_runcmd('print_and_eval "url_rm"', {}, target)
se_mkdir = lambda target: se_runcmd('print_and_eval "url_mkdir"', {}, target)
se_exists = lambda target: se_runcmd('print_and_eval "url_exists"', {}, target)

def se_copy(src, dst, force = True, tmp = ''):
	cmd = 'print_and_eval "url_copy_single%s"' % utils.QM(force, '_force', '')
	return se_runcmd(cmd, {'GC_KEEPTMP': tmp}, src, dst)
