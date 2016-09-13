import os, sys

def matchFile(fn, showTypes = ['py', 'sh', 'lib', 'conf', 'txt', 'json', 'cfg', 'rst'],
		showExternal = None, showAux = None, showScript = None, showTestsuite = None):
	# Filetype check
	ft = None
	if fn.split('/')[-1].startswith('gc-') and ('.' not in fn):
		ft = 'sh'
	if True in map(fn.endswith, showTypes):
		for ftcand in showTypes:
			if fn.endswith(ftcand):
				ft = ftcand
	if not ft:
		return False
	# External check
	isExternal = False
	if True in map(lambda pat: pat in fn, [
		'/xmpp/', '/requests/', '/DashboardAPI/', 'python/logging/',
		'/DLSAPI/', '/DLSAPI_v1/', '/DLSAPI_v2/',
		'/DBSAPI/', '/DBSAPI_v1/', '/DBSAPI_v2/',]):
		isExternal = True
	elif fn in ['docs/ExampleC1_production.py', 'docs/examples/ExampleC1_production.py',
			'python/textwrap.py', 'python/optparse.py',
			'packages/grid_control_cms/Lexicon.py', 'packages/Lexicon.py',
			'packages/python_compat_json.py', 'packages/json.py',
			'packages/python_compat_popen2.py', 'packages/popen2.py', 'python/popen2.py',
			'packages/python_compat_tarfile.py', 'packages/tarfile.py', 'python/tarfile.py', 'packages/pc_tarfile.py',
			'packages/python_compat_urllib2.py',
			'packages/python_compat.py',
		]:
		isExternal = True
	if isExternal and (showExternal == False):
		return False
	# Aux check
	if ('/share/' in fn) and (showAux == False):
		return False
	# Scripts check
	if fn.startswith('scripts/') and (showScript == False):
		return False
	# Testsuite check
	if fn.startswith('testsuite/') and (showTestsuite == False):
		return False
	for blacklisted in ['source_check', '/setup.py']:
		if blacklisted in fn:
			return False
	return True

def getFiles(gcBase = '../..', **kwargs):
	clearPath = lambda x: os.path.abspath(os.path.normpath(os.path.expanduser(x)))
	gcBase = clearPath(gcBase)
	sys.path.append(clearPath(os.path.join(gcBase, 'packages')))
	from python_compat import relpath

	def iterAllFiles():
		for entry in ['scripts', 'packages', 'testsuite']:
			if os.path.exists(os.path.join(gcBase, entry)):
				for (root, dirs, files) in os.walk(os.path.join(gcBase, entry)):
					for fn in files:
						yield os.path.join(root, fn)
		yield os.path.join(gcBase, 'go.py')
		yield os.path.join(gcBase, 'GC')

	for fn in map(lambda fn: relpath(clearPath(fn), gcBase), iterAllFiles()):
		if matchFile(fn, **kwargs):
			yield (os.path.join(gcBase, fn), fn)

if __name__ == '__main__':
	for fn in getFiles('~/grid-control.git'):
		print(fn)
