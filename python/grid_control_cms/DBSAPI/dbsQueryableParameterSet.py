#
# Revision: 0.0 $"
# Id: dbsQueryableParameterSet.py,v 0.0 2006/1/1 18:26:04 afaq Exp $"
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

class  DbsQueryableParameterSet(DbsBase):
   """ 
   Class for QueryableParameterSet

   Following input parameters:

              Hash, Probably a required variable
              Name, User may not need to set this variable always
              Version, User may not need to set this variable always
              Type, User may not need to set this variable always
              Annotation, User may not need to set this variable always
              Content, User may not need to set this variable always
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


