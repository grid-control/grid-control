#!/usr/bin/env python
        
##
## MAIN PROGRAM
##

import getopt,sys
from DashboardAPI import report

if __name__ == '__main__' :
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:")
    except getopt.GetoptError:
        # print help information and exit:
        print "Unknown option"
        sys.exit(1)
    if len( opts)==1 :
        copt=opts[0]
        filename=copt[1]
        try:
            rfile=open(filename)
        except IOError, ex:
            print "Can not open input file"
            sys.exit(1)
        lines=rfile.readlines()
        for line in lines :
           args.append(line.strip())
#        print args
#        print "********************"
    report(args)
#    print "***"
#    print opts
#    print "###"
#    print args
    sys.exit(0)

