#!/usr/bin/env python

import sys, os

root = os.path.realpath(os.path.dirname(sys.argv[0]))
sys.path.append(os.path.join(root, 'python'))

from grid_control import *

voms = Voms()
print voms
