#
# Revision: 0.0 $"
# Id: dbsFileBlock.py,v 0.0 2006/1/1 18:26:04 afaq Exp $"
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

class  DbsFileBlock(DbsBase):
   """ 
   Class for FileBlock

   Following input parameters:

              Name, User may not need to set this variable always
              Status, User may not need to set this variable always
              BlockSize, User may not need to set this variable always
              NumberOfFiles, User may not need to set this variable always
              OpenForWriting, User may not need to set this variable always
              Dataset, User may not need to set this variable always
              fileList, User may not need to set this variable always
              StorageElementList, User may not need to set this variable always
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # List type object fileList needs to be initialized
      # to avoid return "None" instead of empty list []
      self.setdefault('FileList', [])
      self.setdefault('StorageElementList', [])
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


