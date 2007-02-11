# This file is generated on date XXXX

import dbsException
from dbsBaseObject import *

class  DbsProcessedDataset(DbsBase):
   """ 
   Class for ProcessedDataset

   Following input parameters:
              objectId, User may not need to set this variable always
              primaryDataset, Probably a required variable
              processing, Probably a required variable
              datasetName, Probably a required variable
              dataTier, Probably a required variable
              datasetPathName, User may not need to set this variable always
              isDatasetOpen, Probably a required variable
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()
      assert (self.get('datasetPathName', None)
          or (self.get('primaryDataset', None)
          and self.get('datasetName', None)
          and self.get('dataTier', None)))


