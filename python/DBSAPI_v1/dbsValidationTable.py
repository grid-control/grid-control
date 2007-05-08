# This file is generated on date XXXX


from dbsValidateTools import *
ValidationTable = {

"DbsPrimaryDataset" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "datasetName" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
          },
"DbsApplication" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "executable" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "version" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "family" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
          },
"DbsParameterSet" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "hash" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "content" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
          },
"DbsApplicationConfig" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "application" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
         "parameterSet" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
          },
"DbsProcessing" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "parent" : { "Comment" : "User may not need to set this variable always", "Validator" : isDictType },
         "primaryDataset" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
         "processingName" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "applicationConfig" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
         "isOpen" : { "Comment" : "Probably a required variable", "Validator" : isIntType },
          },
"DbsProcessedDataset" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "primaryDataset" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
         "processing" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
         "datasetName" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "dataTier" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "datasetPathName" : { "Comment" : "User may not need to set this variable always", "Validator" : verifyDatasetPathName },
         "isDatasetOpen" : { "Comment" : "Probably a required variable", "Validator" : isIntType },
          },
"DbsParent" : {
         "parent" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
         "type" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
          },
"DbsFileBlock" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "blockName" : { "Comment" : "User may not need to set this variable always", "Validator" : isStringType },
         "processing" : { "Comment" : "User may not need to set this variable always", "Validator" : isDictType },
         "blockStatusName" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "numberOfBytes" : { "Comment" : "Probably a required variable", "Validator" : isLongType },
         "numberOfFiles" : { "Comment" : "Probably a required variable", "Validator" : isLongType },
         "fileList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "eventCollectionList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
          },
"DbsFile" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "logicalFileName" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "guid" : { "Comment" : "User may not need to set this variable always", "Validator" : isStringType },
         "checkSum" : { "Comment" : "User may not need to set this variable always", "Validator" : isStringType },
         "fileType" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "fileStatus" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "fileSize" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "block" : { "Comment" : "Probably a required variable", "Validator" : isDictType },
          },
"DbsEventCollection" : {
         "objectId" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "collectionName" : { "Comment" : "Probably a required variable", "Validator" : isStringType },
         "numberOfEvents" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "collectionStatus" : { "Comment" : "User may not need to set this variable always", "Validator" : isStringType },
         "datasetPathName" : { "Comment" : "Probably a required variable", "Validator" : verifyDatasetPathName },
         "parentageList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "fileList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
          },
}


