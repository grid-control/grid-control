#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2006 Cornell University, Ithaca, NY 14853.
#
# Author:  Valentin Kuznetsov, 2006
#

# system modules
import os, sys, string, stat, re
from optparse import OptionParser

# DBS specific modules
from dbsException    import DbsException
from dbsApiException import *

class DbsOptionParser:
  """
     OptionParser is main class to parse options.
  """
  def __init__(self):
      self.parser = OptionParser()
      self.parser.add_option("--quiet",action="store_true", default=False, dest="quiet",
           help="be quiet during deployment procedure")
      self.parser.add_option("--dbType",action="store", type="string", dest="dbType",
           help="specify DB type, e.g. ORACLE, MySQL")
      self.parser.add_option("--url",action="store", type="string", dest="url",
           help="specify URL, e.g. http://cmssrv17.fnal.gov:8989/DBS/servlet/DBSServlet")
      self.parser.add_option("--host",action="store", type="string", dest="host",
           help="specify host, e.g. http://localhost")
      self.parser.add_option("--port",action="store", type="string", dest="port",
           help="specify port, e.g. 8080")
      self.parser.add_option("--servlet",action="store", type="string", dest="servlet",
           help="specify servlet, e.g. /DBS/servlet/DBSServlet")
      self.parser.add_option("--dbsInstance",action="store", type="string", dest="instance",
           help="specify DB instances, e.g. MCLocal_1/Writer")
      self.parser.add_option("--username",action="store", type="string", dest="user",
           help="specify user name for DB access (if any)")
      self.parser.add_option("--password",action="store", type="string", dest="password",
           help="specify user password for DB access (if any)")
      self.parser.add_option("-v","--verbose", action="store", type="int", default=0, dest="verbose",
           help="specify verbose level, e.g. --verbose=1, or higher --verbose=2")
  def getOpt(self):
      """
          Returns parse list of options
          @type  self: class object
          @param self: none
          @rtypei : none
          @return : list of options.
      """
      return self.parser.parse_args()
#
# main
#
if __name__ == "__main__":
    optManager  = DbsOptionParser()
    (opts,args) = optManager.getOpt()
    print "options:  ",opts
    print "arguments:",args

