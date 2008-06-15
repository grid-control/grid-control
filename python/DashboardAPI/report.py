#!/usr/bin/python
        
##
## MAIN PROGRAM
##

import sys
from DashboardAPI import report

if __name__ == '__main__' :
    args = sys.argv[1:]
    report(args)
    sys.exit(0)
