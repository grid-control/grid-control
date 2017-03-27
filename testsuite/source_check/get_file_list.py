import os, sys
from grid_control.utils import clean_path
from python_compat import any, imap, relpath


def get_file_list(gc_base_path='../..', **kwargs):
	gc_base_path = clean_path(gc_base_path)
	sys.path.append(clean_path(os.path.join(gc_base_path, 'packages')))

	def _iter_all_files():
		for entry in ['scripts', 'packages', 'testsuite']:
			if os.path.exists(os.path.join(gc_base_path, entry)):
				for (root, _, files) in os.walk(os.path.join(gc_base_path, entry)):
					for fn in files:
						yield os.path.join(root, fn)
		yield os.path.join(gc_base_path, 'go.py')
		yield os.path.join(gc_base_path, 'GC')

	for fn in imap(lambda fn: relpath(clean_path(fn), gc_base_path), _iter_all_files()):
		if match_file(fn, **kwargs):
			yield (os.path.join(gc_base_path, fn), fn)


def main():
	import logging
	for fn in get_file_list('~/grid-control.git'):
		logging.info(fn)


def match_external(fn):
	external_pat_list = [
		'/xmpp/', '/requests/', '/DashboardAPI/', 'python/logging/',
		'/DLSAPI/', '/DLSAPI_v1/', '/DLSAPI_v2/',
		'/DBSAPI/', '/DBSAPI_v1/', '/DBSAPI_v2/',
	]
	external_fn_list = [
		'docs/ExampleC1_production.py', 'docs/examples/ExampleC1_production.py',
		'python/textwrap.py', 'python/optparse.py',
		'packages/grid_control_cms/Lexicon.py', 'packages/Lexicon.py',
		'packages/python_compat_json.py', 'packages/json.py',
		'packages/python_compat_popen2.py', 'packages/popen2.py', 'python/popen2.py',
		'packages/python_compat_tarfile.py', 'packages/tarfile.py',
		'python/tarfile.py', 'packages/pc_tarfile.py',
		'packages/python_compat_urllib2.py',
		'packages/python_compat.py',
	]
	if any(imap(lambda pat: pat in fn, external_pat_list)):
		return True
	return fn in external_fn_list


def match_file(fn, show_type_list=None, show_external=None, show_aux=None, show_script=None,
		show_testsuite=None, show_source_check=False, no_links=True):
	if not match_file_type(fn, show_type_list, no_links):
		return False
	# External check
	if match_external(fn) and (show_external is False):
		return False
	# Aux check
	if ('/share/' in fn) and (show_aux is False):
		return False
	# Scripts check
	if fn.startswith('scripts/') and (show_script is False):
		return False
	# Testsuite check
	if not match_special(fn, show_testsuite, show_source_check):
		return False
	return True


def match_file_type(fn, show_type_list, no_links):
	link_blacklist = ['downloadFromSE', 'lumiInfo', 'gcTool', 'gcSettings']
	if no_links and any(imap(lambda pat: pat in fn, link_blacklist)):
		return False
	show_type_list = show_type_list or ['py', 'sh', 'lib', 'conf', 'txt', 'json', 'cfg', 'rst']
	# Filetype check
	if fn.split('/')[-1].startswith('gc-') and ('.' not in fn):
		return 'sh'
	if any(imap(fn.endswith, show_type_list)):
		for ftcand in show_type_list:
			if fn.endswith(ftcand):
				return ftcand


def match_special(fn, show_testsuite, show_source_check):
	if fn.startswith('testsuite/') and (show_testsuite is False):
		return False
	if fn.startswith('testsuite/source_check') and (show_source_check is False):
		return False
	for blacklisted in ['/setup.py']:
		if blacklisted in fn:
			return False
	return True


if __name__ == '__main__':
	main()
