
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def dbsApiImplInsertMergedDataset(self, dataset, merege_ds_name, merge_algo):
    """
    Clones a dataset and add another Algo to it.
    This is done for merged datasets.
    params:
         dataset: the dataset needs to be cloned 
         merge_algo:  merge application that needs to added to cloned dataset.
                      Assuming for now that merge_algo is just ONE Algo object
    """

    path = get_path(dataset) 
    token = path.split("/")
    #print token

    orig_ds = self.listProcessedDatasets(token[1], token[3], token[2])

    if len(orig_ds) < 1:
	raise DbsApiException(args="Dataset %s Not found in DBS" %path, code="1008")
    
    proc = orig_ds[0]

    proc['Name'] = merege_ds_name
    if merge_algo not in (None, ''):
	#raise DbsApiException(args="You must provide an Algorithm object for the merged dataset")
	#return
        self.insertAlgorithm(merge_algo)  
        proc['AlgoList'].append(merge_algo) 

    #Grab the parents as well.
    proc['ParentList'] = self.listDatasetParents(path)

    #Create the dataset
    self.insertProcessedDataset (proc)

    #Lets grab the Runs as well
    ds_runs = self.listRuns(path)
    #And add the to newly created dataset
    for aRun in ds_runs:
        self.insertRunInPD(proc, aRun['RunNumber'])

    return proc

  # ------------------------------------------------------------
