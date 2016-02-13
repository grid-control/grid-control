#!/usr/bin/python
        
##
## MAIN PROGRAM
##

import os, sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from DashboardAPI import report
sys.path.pop()

if __name__ == '__main__' :
    args = sys.argv[1:]
    report(args)
    sys.exit(0)
