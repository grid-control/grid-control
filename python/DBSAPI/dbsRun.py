#
# Revision: 0.0 $"
# Id: dbsRun.py,v 0.0 2006/1/1 18:26:04 afaq Exp $"
#
""" This file is generated on Wed Nov  8 13:38:46 2006 """ 

"""SERIOUS WARNING:

         This file is a generated file,
         in case you have made manual changes to  
         any of generated files, make sure you DO NOT
         end up over-writting them by re-running the
         generator and copying them here.

         Either make changes to generator, or carefully
         preserve the manual changes. 
"""
import dbsException
from dbsBaseObject import *

class  DbsRun(DbsBase):
   """ 
   Class for Run

   Following input parameters:

              RunNumber, User may not need to set this variable always
              NumberOfEvents, User may not need to set this variable always
              NumberOfLumiSections, User may not need to set this variable always
              TotalLuminosity, User may not need to set this variable always
              StoreNumber, User may not need to set this variable always
              StartOfRun, User may not need to set this variable always
              EndOfRun, User may not need to set this variable always
              Dataset, Probably a required variable
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.setdefault('Dataset', [])
      #
      self.validate()


