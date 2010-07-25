
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertMergedFile(self, parents, outputFile):
    """
    Inserts a LFN into DBS, while getting all details of input LFNs 
    as PHYSICS and merge "parents" of THIS outputLFN.

    params:
        outputFile: is the output file object with all detals, such a Check sum, Number of events etc.
        Also specifying ParentFiles (The files that were merged into THIS file)
   
    """

    #parents = outputFile['ParentList']
    #Reset parents as REAL parents will be those that are 
    #from the Parent of Unmerged files
    outputFile['ParentList'] = []
    for anInputLFN in parents:
       
        #fileDetails = self.listFiles(patternLFN=get_name(anInputLFN), details=True) 
        fileDetails = self.listFiles(patternLFN=get_name(anInputLFN), retriveList=["all"]) 
        if len(fileDetails) < 1:
		raise DbsApiException(args="Unmerged file %s not found in DBS" %get_name(anInputLFN), code="1999")
        fileDetail = fileDetails[0] 
        for atier in fileDetail['TierList']:
		if atier not in outputFile['TierList']:
			outputFile['TierList'].append(atier)

        for alumi in fileDetail['LumiList']:
                if alumi not in outputFile['LumiList']:
                        outputFile['LumiList'].append(alumi)

        for algo in fileDetail['AlgoList']:
                if algo not in outputFile['AlgoList']:
                        outputFile['AlgoList'].append(algo)
   
        for achild in fileDetail['ChildList']:
                if achild not in outputFile['ChildList']:
                        outputFile['ChildList'].append(achild)

        for aparent in fileDetail['ParentList']:
                if aparent not in outputFile['ParentList']:
                        outputFile['ParentList'].append(aparent)

        # Branches must be same, I hope !!!!!!!!!!
        for abranch in fileDetail['BranchList']:
                if abranch not in outputFile['BranchList']:
                        outputFile['BranchList'].append(abranch)

	for trig in fileDetail['FileTriggerMap']:
		if trig not in outputFile['FileTriggerMap']:
			outputFile['FileTriggerMap'].append(trig) 

    self.insertFiles(outputFile['Dataset'], [outputFile], outputFile['Block'])

  # ------------------------------------------------------------
