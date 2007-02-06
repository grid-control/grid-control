#!/usr/bin/env python
import sys, os

# add python subdirectory from where go.py was started to search path
_root = os.path.dirname(os.path.abspath(os.path.normpath(__file__)))
sys.path.append(os.path.join(_root, 'python'))

# and include grid_control python module
from grid_control import *

###
### main program
###
if __name__ == '__main__':
	proxy = Proxy()
	print proxy
