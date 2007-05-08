#
# Revision: 0.0 $"
# Id: dbsAnalysisDataset.py,v 0.0 2006/1/1 18:26:04 afaq Exp $"
#
import dbsException
from dbsBaseObject import *

class  DbsAnalysisDatasetDefinition(DbsBase):
   """ 
   Class for AnalysisDatasetDefinition

   Following input parameters:

              Annotation, User may not need to set this variable always
              Name, Probably a required variable
              Type, User may not need to set this variable always
              Status, User may not need to set this variable always
              PhysicsGroup, User may not need to set this variable always
   """
   def __init__(self, **args):
      DbsBase.__init__(self)
      # Read in all User provided values
      self.update(args)
      self.setdefault('FileList', [])
      self.setdefault('AlgoList', [])
      self.setdefault('TierList', [])
      self.setdefault('LumiList', [])
      self.setdefault('RunsList', [])
      self.setdefault('AnalysisDSList', [])
      self.setdefault('LumiRangeList', [])
      self.setdefault('RunRangeList', [])
      
      # Verifying that data types of user provide parameters is correct
      # Validating the data using ValidationTable(.py)
      self.validate()

