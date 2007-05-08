# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsProcessingPath(DbsBase):
   """ 
   Class for ProcessingPath

   Following input parameters:
              pathId, User may not need to set this variable always
              parent, User may not need to set this variable always
              application, Probably a required variable
              dataTier, Probably a required variable
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


