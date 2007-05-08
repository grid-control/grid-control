# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsParent(DbsBase):
   """ 
   Class for Parent

   Following input parameters:
              parent, Probably a required variable
              type, Probably a required variable
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


