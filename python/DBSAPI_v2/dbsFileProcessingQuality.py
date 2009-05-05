#
# Revision: $"
# Id: $"
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

class  DbsFileProcessingQuality(DbsBase):
   """ 
   Class for FileProcessingQuality

   Following input parameters:

            ParentFile : File for which processing quality is being recorded, LFN of the file that failed to produce a child file
            FailedEventList : Which events were failed, optional
            Description : Upto 1000 chars of what possibly went wrong
            ChildDataset : The child dataset path, whoes file was suppose to be produced by this file
            ProcessingStatus : Status string representing what went wrong
            FailedEventCount : Number of events that failed, Optional

   """

   def __init__(self, **args):
      DbsBase.__init__(self)
      self.setdefault('FailedEventList', [])
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()

