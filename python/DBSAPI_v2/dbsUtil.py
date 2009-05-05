import os, string
from dbsException import DbsException
from dbsApiException import *
#from dbsApi import DbsApi


"""
def makeAPI(url):
                #args = {}
                #args['url'] = url
                args = {}
                if url.startswith('http'):
                        args['url'] = url
                        args['mode'] = 'POST'

                return DbsApi(args)

"""

def get_name (obj):
    """
    A utility function, that gets "Name" from an Object.
    Not a very cool function !
    """

    if obj == None:
            return "";
    if type(obj) == type(''):
       return obj
    name = obj.get('Name')
    if name ==  None:
            return ""
    return name


def file_name (obj):
    """
    A utility function, that gets "Name" from an Object.
    Not a very cool function !
    """

    if obj == None:
            return "";
    if type(obj) == type(''):
       return obj
    name = obj.get('LogicalFileName')
    if name ==  None:
            return ""
    return name

def get_run (obj):
    """
    A utility function, that gets "Name" from an Object.
    Not a very cool function !
    """

    if obj == None:
            return "";

    if type(obj) == type(int(1)):
       return obj

    if type(obj) == type(long(1)):
       return obj

    if type(obj) == type (""):
       return obj

    num = obj.get('RunNumber')
    if num ==  None:
            return ""

    return num

def getInt(value = None):
        if (value == None ) :
                return 0
        if (len(value) < 1 ) :
                return 0
        return int(value)

def getLong(value = None):
        if (value == None ) :
                return 0
        if (len(value) < 1 ) :
                return 0
        return long(value)

def get_path (dataset):
    """
    Determine the dataset path of a dataset.  If the argument is a
    string, it's assumed to be the path and is returned.  If the 
    argument is an object, its assumed to be a processed datatset 
    and this function make a path (string) out of its  primary dataset, 
    tier and processed dataset name.

    Note: takes the FIRST Tier from the list of tiers
          A dataset can have multiple tiers, and however you contruct the path
          using any tier, still leads to same processed dataset, so picking 
          first tier doen't havm the operations. 
    """

    if dataset == None:
            return ""
    if type(dataset) == type(''):
       return dataset

    if dataset.get('Path') not in ('', None):
        return dataset.get('Path')

    # Worst case fabricate a Path !
    if dataset.get('Name') not in ('', None):
         primary = dataset.get('PrimaryDataset')
         if primary != None:
            tier= dataset.get('TierList', [])

            if tier in (None, []):
               raise InvalidDatasetPathName(Message="The dataset/path provided is incorrect")
            #return "/" + primary.get('Name') \
                        #+ "/" + dataset.get('Name') + "/" + tier[0]    
            return "/" + primary.get('Name') \
                        + "/" + dataset.get('Name') + "/" + string.join(tier, "-")
               #return "/" + primary.get('Name') \
               #      + "/" + tier[0] + "/" + dataset.get('Name')

    # Anything missing (name, primary or tier) thats an error 
    raise InvalidDatasetPathName(Message="The dataset/path provided is incorrect")

