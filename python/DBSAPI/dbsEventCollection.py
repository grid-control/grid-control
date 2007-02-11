# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsEventCollection(DbsBase):
   """ 
   Class for EventCollection

   Following input parameters:
              objectId, User may not need to set this variable always
              collectionName, Probably a required variable
              numberOfEvents, User may not need to set this variable always
              collectionStatus, User may not need to set this variable always
              datasetPathName, Probably a required variable
              parentageList, User may not need to set this variable always
              fileList, User may not need to set this variable always
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values

      self.setdefault('fileList', [])
      self.setdefault('parentageList', [])
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


