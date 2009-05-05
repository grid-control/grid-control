#
# Revision: 0.0 $"
# Id: dbsPrimaryDataset.py,v 0.0 2006/1/1 18:26:04 afaq Exp $"
#
import dbsException
from dbsBaseObject import *

class  DbsCompositeAnalysisDataset(DbsBase):
   """ 
   Class for PrimaryDataset

   Following input parameters:

              Annotation, User may not need to set this variable always
              Name, Probably a required variable
              StartDate, User may not need to set this variable always
              EndDate, User may not need to set this variable always
              Type, User may not need to set this variable always
              Description, User may not need to set this variable always
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      self.setdefault('ADSList', [])
      # Read in all User provided values
      self.update(args)
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()


