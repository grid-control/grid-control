# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsFileBlock(DbsBase):
   """ 
   Class for FileBlock

   Following input parameters:
              objectId, User may not need to set this variable always
              blockName, User may not need to set this variable always
              processing, User may not need to set this variable always
              blockStatusName, Probably a required variable
              numberOfBytes, Probably a required variable
              numberOfFiles, Probably a required variable
              fileList, User may not need to set this variable always
              eventCollectionList, User may not need to set this variable always
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      self.setdefault('fileList', [])
      self.setdefault('eventCollectionList', [])
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


