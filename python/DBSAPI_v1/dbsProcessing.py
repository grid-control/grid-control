# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsProcessing(DbsBase):
   """ 
   Class for Processing

   Following input parameters:
              objectId, User may not need to set this variable always
              parent, User may not need to set this variable always
              primaryDataset, Probably a required variable
              processingName, Probably a required variable
              applicationConfig, Probably a required variable
              isOpen, Probably a required variable
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


