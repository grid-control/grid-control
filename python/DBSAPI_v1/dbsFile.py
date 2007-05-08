# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsFile(DbsBase):
   """ 
   Class for File

   Following input parameters:
              objectId, User may not need to set this variable always
              logicalFileName, Probably a required variable
              guid, User may not need to set this variable always
              checkSum, User may not need to set this variable always
              fileType, Probably a required variable
              fileStatus, Probably a required variable
              fileSize, User may not need to set this variable always
              block, Probably a required variable
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


