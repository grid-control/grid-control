#!/usr/bin/env python
import sys, os, getopt

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(sys.argv[0])))
sys.path.append(os.path.join(_root, 'python'))
# and include grid_control python module
from grid_control import *


def syntax(out):
	out.write("Syntax: %s [OPTIONS] <config file>\n\n"
	          "    Options:\n"
	          "\t-h, --help               Show this helpful message\n"
	          "\n" % sys.argv[0])


def main(args):
	longOptions = ['help']
	shortOptions = 'h'

	# let getopt dig through the options
	try:
		opts, args = getopt.getopt(args, shortOptions, longOptions)
	except getopt.GetoptError:
		# fail if an invalid option or missing argument was found
		syntax(sys.stderr)
		return 1

	# process options
	for opt, arg in opts:
		if opt in ('-h', '--help'):
			syntax(sys.stdout)
			return 0

	# we need exactly one config file argument
	if len(args) != 1:
		syntax(sys.stderr)
		return 1
	configFile = args[0]

	# big try... except block to catch exceptions and print error message
	try:
		# try to open config file
		try:
			f = open(configFile, 'r')
		except IOError, e:
			raise GridError("Configuration file `%s' not found" % configFile)

		config = Config(f)
		f.close()

		print config.getPath('global', 'workdir')

		# Test grid proxy
		proxy = config.get('grid', 'proxy')
		proxy = Proxy.open(proxy)
		print 'Your proxy has %d seconds left!' % proxy.timeleft()

	except GridError, e:
		e.showMessage()
		return 1

	# everything seems to be in order
	return 0


# if go.py is executed from the command line, call main() with the arguments
if __name__ == '__main__':
	sys.exit(main(sys.argv[1:]))
