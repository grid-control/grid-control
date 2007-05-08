#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#

import types, string, re

ALLOWED_NAME_CHARS_ = "[a-zA-Z0-9_\a\.-]"
ALLOWED_DATASET_REGEX_ = [
  re.compile("/%s+/%s+" % (ALLOWED_NAME_CHARS_, ALLOWED_NAME_CHARS_)),
  re.compile("/%s+/%s+/%s+" % (ALLOWED_NAME_CHARS_, ALLOWED_NAME_CHARS_,
                    ALLOWED_NAME_CHARS_))
  ]

def verifyDatasetPathName(datasetPathName):
  """ Verify the validity of the given name. """
  for regex in ALLOWED_DATASET_REGEX_:
    if regex.match(datasetPathName): return True
  return False
  #raise InvalidDatasetPathName(args="Invalid dataset path name '%s'" % datasetPathName)

def isIntType(inObj) :
  """ Type Checking for Int Type """
  if type(inObj) ==  type(int(1)):
     return True
  return False

def isStringType(inObj) :
  """ Type Checking for String Type """
  if type(inObj) ==  type(""):
     return True
  return False

def isListType(inObj) :
  """ Type Checking for LIST Type """
  if type(inObj) ==  type([]):
     return True
  return False

def isDictType(inObj) :
  """ Type Checking for LIST Type """
  if isinstance(inObj,dict) :
     return True
  return False

def isLongType(inObj) :
  """ Type Checking for LIST Type """
  if type(inObj) ==  type(long(1)):
     return True
  else:
     if type(inObj) ==  type(int(1)):
        return True
  return False

