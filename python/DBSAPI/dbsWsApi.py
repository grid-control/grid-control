from Service_services import *
import dbsException

import dbsApi

import ZSI

class DbsWsApi(dbsApi.DbsApi):

   def __init__(self):
       self.loc = ServiceLocator()
       self.portType = self.loc.getServicePortType()

   def createPrimaryDataset(self, primaryDataset):

     try:
       request = createPrimaryDatasetRequestWrapper()
       request._primaryDataset = primaryDataset
       response = self.portType.createPrimaryDataset(request)
       return response._primaryDatasetId
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def createProcessedDataset(self, processedDataset):

     try:
       request = createProcessedDatasetRequestWrapper()
       request._processedDataset = processedDataset
       response = self.portType.createProcessedDataset(request)
       return response._processedDatasetId
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def createFileBlock(self, datasetPathName, block):

     try:
       request = createFileBlockRequestWrapper()
       request._datasetPathName = datasetPathName
       request._block = block
       response = self.portType.createFileBlock(request)
       return response._fileBlockId
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def insertEventCollections(self, eventCollectionList):

     try:
       request = insertEventCollectionsRequestWrapper()
       request._eventCollectionList = eventCollectionList
       response = self.portType.insertEventCollections(request)
       return response._result
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def mergeEventCollections(self, inputEventCollectionList, outputEventCollection):

     try:
       request = mergeEventCollectionsRequestWrapper()
       request._inputEventCollectionList = inputEventCollectionList
       request._outputEventCollection = outputEventCollection
       response = self.portType.mergeEventCollections(request)
       return response._result
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def getDatasetContents(self, datasetPathName, listFiles):

     try:
       request = getDatasetContentsRequestWrapper()
       request._datasetPathName = datasetPathName
       request._listFiles = listFiles
       response = self.portType.getDatasetContents(request)
       return response._blockList
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def getDatasetFileBlocks(self, datasetPathName):

     try:
       request = getDatasetFileBlocksRequestWrapper()
       request._datasetPathName = datasetPathName
       response = self.portType.getDatasetFileBlocks(request)
       return response._blockList
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)

   def listDataset(self, datasetPathName):

     try:
       request = listDatasetRequestWrapper()
       request._datasetPathName = datasetPathName
       response = self.portType.listDataset(request)
       return response._datasetList
     except ZSI.FaultException, ex:
       raise dbsApi.DbsApiException(exception=ex)


