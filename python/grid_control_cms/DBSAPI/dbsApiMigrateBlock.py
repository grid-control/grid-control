
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsMigrateApi import DbsMigrateApi

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *
from dbsApi import DbsApi

def makeAPI(url, version = None):
                #args = {}
                #args['url'] = url
                args = {}
                if url.startswith('http'):
                        args['url']     = url
                        args['mode']    = 'POST'
                        if version:
                            args['version'] = version

                return DbsApi(args)

def dbsApiImplMigrateBlock(self, srcURL, dstURL, block_name="", srcVersion = None, dstVersion = None, pruneBranches = False):
    """
    Migrate a SINGLE Block of a dataset from srcURL to dstURL
    
    Parameters

        srcURL : is the URL of the deployed DBS instance from where the dataset will be transferred.

        dstURL : is the URL of the deployed DBS instance into which the dataset will be transferred.

        path : is mandatory field. Its the dataset path for which API is being invoked (can be provided as dataset object).
                 This represents that dataset that needs to be migrated.

        block_name : Name of the Block of whose  files wile be migrated, This is an optional parameter. If not provided
                             then all the blocks within the dataset will be migrated.

    raise: DbsApiException.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    pruneBranches = False
    force = False
    
    try:
       # Invoke Server.
       if block_name not in [None, ""] :
	    path = get_path(block_name.split('#')[0])
	    apiSrc = makeAPI(srcURL, srcVersion)
	    apiDst = makeAPI(dstURL, dstVersion)
	    transfer = DbsMigrateApi(apiSrc, apiDst, force, pruneBranches)
	    transfer.migrateForNiceBoys(path, block_name) 

    except Exception, ex:
      raise DbsBadResponse(exception=ex)


