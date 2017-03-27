import os, logging
from grid_control.utils.file_objects import SafeFile


def main():
	os.system('git rev-parse --short HEAD > .git_version')
	os.system('git log | grep git-svn > .svn_version')
	for line in SafeFile('.svn_version').iter_close():
		svn_version = int(line.split('@')[1].split()[0])
		break
	git_version = SafeFile('.git_version').read_close().strip()
	svn_version += 1
	logging.critical('%s %s', svn_version, git_version)
	os.unlink('.svn_version')
	os.unlink('.git_version')
	fn = '../../packages/grid_control/__init__.py'
	line_list = SafeFile(fn).read().splitlines()
	fp = SafeFile(fn, 'w')
	for line in line_list:
		if line.startswith('__version__'):
			version_tuple = (svn_version / 1000, (svn_version / 100) % 10, svn_version % 100, git_version)
			line = "__version__ = '%d.%d.%d (%s)'" % version_tuple
		fp.write(line + '\n')
	fp.close()


if __name__ == '__main__':
	main()
