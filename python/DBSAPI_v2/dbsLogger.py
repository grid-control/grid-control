#!/usr/bin/env python
# This class controls DBS's logging behaviour
#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"

import logging
from dbsApiException import *

# DBS Defined Log Levels
DBSDEBUG=1
DBSINFO=2
DBSWARNING=3

class DbsLogger:

  # Default LOG Level is ERROR
  def __init__(self, userloglevel="ERROR", logout="STDOUT"):
    #
    # Setup Proper logging
    #
    # DBS Supported Log Levels
    #  Standard
    #CRITICAL   50
    #ERROR      40
    #NOTSET     0
    #  DBS Defined
    #DBSWARNING 3
    #DBSINFO    2
    #DBSDEBUG   1
    logging.addLevelName(DBSDEBUG, 'DBSDEBUG')
    logging.addLevelName(DBSINFO, 'DBSINFO')
    logging.addLevelName(DBSWARNING, 'DBSWARNING')

    if userloglevel in ("", None, "DBSDEBUG"):
       loglevel=DBSDEBUG
    elif userloglevel == 'DBSINFO':
       loglevel=DBSINFO
    elif userloglevel == 'DBSWARNING':
       loglevel=DBSWARNING
    elif userloglevel == 'ERROR':
       loglevel=logging.ERROR
    elif userloglevel == 'CRITICAL':
       loglevel=logging.CRITICAL
    elif userloglevel == 'NOTSET':
       loglevel=logging.NOTSET
    # If something else is specified, raise error
    else :
       raise DbsApiException(args="Unknow Log 'LEVEL'=%s, please choose a valid level" % str(userloglevel) )

    # User want to write in a file ?
    if logout not in ("", None, "STDOUT"):
            print "Writing log to ", logout
            logging.basicConfig(level=loglevel,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logout,
                    filemode='w+')
    # By default write it on stdout
    else:
            logging.basicConfig(level=loglevel,
                    format='%(asctime)s %(levelname)s %(message)s')

