
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsMigrateApi import DbsMigrateApi

from dbsException import DbsException
from dbsApiException import *

import logging
import inspect

from dbsLogger import *

from dbsUtil import *
from dbsApi import DbsApi

def makeAPI(url):
                #args = {}
                #args['url'] = url
                args = {}
                if url.startswith('http'):
                        args['url'] = url
                        args['mode'] = 'POST'

                return DbsApi(args)



def dbsApiImplMigrateDatasetContents(self, srcURL, dstURL, path, block_name="", noParentsReadOnly = False, pruneBranches = False):
    """
    
Migrate API Instructions

    * This Api Call does the 3rd party transfer of a dataset from a source DBS instance to
       destination DBS instance.
    * This API provides an option to migrate recursively the dataset and all its parents also. By default all
       the parents will be  transferred along with the child dataset. If a dataset is being transferred and
       its parents are not in the destination DBS instance, then this API will transfer the parents first and
       thier parents and so on and so fortth until a parent is found in the destination DBS.
    * User have a choice to just migare a single  dataset and ignore all its parents in the destination DBS.
 
   

Parameters

        srcURL : is the URL of the deployed DBS instance from where the dataset will be transferred.

        dstURL : is the URL of the deployed DBS instance into which the dataset will be transferred.

        path : is mandatory field. Its the dataset path for which API is being invoked (can be provided as dataset object).
                 This represents that dataset that needs to be migrated.

        block_name : Name of the Block of whose  files wile be migrated, This is an optional parameter. If not provided
                             then all the blocks within the dataset will be migrated.

       noParentsReadOnly : this is an optional flag which allows the users to either migrate all the parent datasets along with
                     the specified dataset (path), or just  the specified dataset (path) without all the parents. 
                     By default this flag is set to False which implies that all the parents will be migrated
                     as well. NOTE that this will be a time expensive operation since the datasets parents will be
                     migrated recursively before the specified  dataset gets migrated. This is by far the ideal way 
                     of transferring the dataset. If the users want to just transfer the specified dataset and ignore 
                     the parentage , then they should set this flag to False explicitly. When this  flag will be set to False
                     then only the specified dataset will be transferred to the destination DBS. This will be a 
                     much faster operation but there  will be implication of such transfer

                     For example when only a child dataset is transferred to a destination DBS without the 
                     parents, then its parentage information is completely lost in the destination DBS. 
                     To cope with this situation, the dataset that was transferred with deceased parents 
                     are marked as READ ONLY dataset in  destination DBS. This means that any alteration or 
                     addition to this dataset READ ONLY dataset is not permitted anymore. Further once the dataset
                     is marked READ ONLY then , it cannot be migrated anywhere else. If the users really want to 
                     migrate this dataset to another instance they will have  to migrate the same dataset from 
                     the source DBS to another instance either again with parents or without parents (READ ONLY)

                     NOTE that migrating a single level dataset (without parents) will mark the dataset as READ ONLY 
                     only in the Local instances. When user will migrate  a dataset without parents to Global instance , 
                     it will NOT be marked as READ ONLY there, but if this dataset have parents (already in Global instance),
                     then that information will be recorded properly. If the Global dataset does not have 
                     the parents already then it will throw an exception.

Scenarios

                    

    Here are a couple of scenarios with example of how to use this API properly

     [ DBS Gobal ]                                                                        [ DBS Local ]
     DS1
       +----DS2
               +----DS3
   
    Consider that there are two DBS instance Global and Local. Global Instance has 3 datasets DS1 is parent of DS2 
    is parent of DS3.  Global URL is GU an Local instance URL is LU.

    A user to do do a local processing of DS3 and create another dataset DS4 which is a child of DS3. He may migrate 
    recursively DS3 along with parents
    [ DBS Gobal ]                                                                        [ DBS Local ]
     DS1                                                                                        DS1                                                             
       +----DS2               migrateDatasetContents(GU, LU, DS3)                                +-----DS2
               +----DS3    --------------------------------------------------------->                  +-----DS3
     

    Or the user may migrate just DS3 to Local lnstance
    [ DBS Gobal ]                                                                        [ DBS Local ]
     DS1             
       +----DS2             migrateDatasetContents(GU, LU, DS3,parents=False)            
                 +----DS3     --------------------------------------------------------->          DS3(Marked as READ ONLY)


  In the former case the user can migrate DS3 from his local instance back to Global or any other instance. 
  In the latter case the dataset is  marked as READ ONLY and CANNOT be migrated or changed
  Now lets say the user generated DS4 from DS3 and now he want to migrate DS4 back to global. 
  In the former case he can either  do a one level migration or recursive migration and both will work. 
  When he does one level (without parent) migration or DS4

    [ DBS Gobal ]                                                                              [ DBS Local ]
     DS1                                                                                               DS1                                                             
       +----DS2                                                                                            +-----DS2
               +----DS3     migrateDatasetContents(LU, GU, DS3,parents=False)                                   +-----DS3
                       +----DS4   <--------------------------------------------------------------------                 +---DS4


    Note that one level migration of DS4 to Global did not mark the dataset DS4 as READ ONLY. 
    Further it also maintained the   parentage relationship (DS3 parent of DS4) in Global DBS. If for some reason
    DS3 did not existed in Global DBS and the user  did one level migration of DS4 to Global, then it would have 
    thrown an exception that the parent of DS4 does not exist in Global.  In that case the user can do the 
    recursive migration of DS4 to Global

    [ DBS Gobal ]                                                                                [ DBS Local ]
     DS1                                                                                                      DS1                                                             
       +----DS2                                                                                                +-----DS2
               +----DS3             migrateDatasetContents(LU, GU, DS3)                                              +-----DS3
                       +----DS4   <--------------------------------------------------------------------                      +---DS4

  The recursive migration of DS4 to Global will make sure that if DS3 does not exist in Global, it gets transferred first and 
  same is true for DS3 and DS1. In this case since they are already present in Global the migration of DS3, DS2 and DS1 
   will be ignored even if the user tried the recursive migration. 
  flag to be True, in which case the DS3,DS2 and DS1 will be forcefully migrated even if they are already present in Global DBS. 
   The implication is None.


  Now consider the second case then DS3 is marked READ ONLY in Local DBS. In this case the user can migrate DBS4 only as 
  one level migration or as recursive migration if Force is set to False.

    [ DBS Gobal ]                                                                                        [ DBS Local ]
     DS1             
       +----DS2   
               +----DS3               migrateDatasetContents(LU, GU, DS3,parents=False)                    DS3(Marked as READ ONLY)
                       +----DS4   <--------------------------------------------------------------------     +----DS4


   Again the parentage information of DS4 is maintained in Global DBS as DS3 already exist there. If DS3 does not exist in 
   Global already, it will raise an exception. NOTE that the dataset DS4 in Global DBS is not marked as READ ONLY even 
   though it was transferred via single level (without parents) migration . This implies that the READ ONLY dataset cannot
   be created in Gloaball DBS. they can just resides in LOCAL instances.

  Second option is to transfer recursively DS4. 

    [ DBS Gobal ]                                                                                        [ DBS Local ]
     DS1             
       +----DS2   
               +----DS3                migrateDatasetContents(LU, GU, DS3)                                      DS3(Marked as READ ONLY)
                       +----DS4   <--------------------------------------------------------------------            +----DS4


   The reason the above will succeed is that the recursive migration will first check for parent of DS4 which is DS3 and check 
   its existence in Global DBS. Since it already exists there  DS3 (along with DS2 and DS1) will be ignored and will NOT get 
   transferred. 
  

Examples

        Migrates the dataset and the specified block with parents
        api.migrateDatasetContents("http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
                                        "https://cmssrv17.fnal.gov:8443/DBS/servlet/DBSServlet"
                                        "/ZmumuJets_EtGamma_450_550/CMSSW_1_3_4-Spring07-1689/GEN-SIM-DIGI-RECO",
                                        "/ZmumuJets_EtGamma_450_550/CMSSW_1_3_4-Spring07-1689/GEN-SIM-DIGI-RECO#a03cf5c0-bed4-40d3-9f0e-39e6b91ccf58")

        Migrates all the blocks in the dataset with parents
        api.migrateDatasetContents("http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
                                        "https://cmssrv17.fnal.gov:8443/DBS/servlet/DBSServlet"
                                        "/ZmumuJets_EtGamma_450_550/CMSSW_1_3_4-Spring07-1689/GEN-SIM-DIGI-RECO",)

        Migrates the dataset and the specified block without parents
        api.migrateDatasetContents("http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
                                        "https://cmssrv17.fnal.gov:8443/DBS/servlet/DBSServlet"
                                        "/ZmumuJets_EtGamma_450_550/CMSSW_1_3_4-Spring07-1689/GEN-SIM-DIGI-RECO",
                                        "/ZmumuJets_EtGamma_450_550/CMSSW_1_3_4-Spring07-1689/GEN-SIM-DIGI-RECO#a03cf5c0-bed4-40d3-9f0e-39e6b91ccf58",
                                         noParentsReadOnly=True)


        Migrates all the blocks in the dataset without the parents
        api.migrateDatasetContents("http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
                                        "https://cmssrv17.fnal.gov:8443/DBS/servlet/DBSServlet"
                                        "/ZmumuJets_EtGamma_450_550/CMSSW_1_3_4-Spring07-1689/GEN-SIM-DIGI-RECO",
                                         noParentsReadOnly=True)



    raise: DbsApiException.

    """

    funcInfo = inspect.getframeinfo(inspect.currentframe())
    #logging.log(DBSDEBUG, "Api call invoked %s" % str(funcInfo[2]))

    try:
       # Invoke Server.
       path = get_path(path)
       apiSrc = makeAPI(srcURL)
       apiDst = makeAPI(dstURL)

       #transfer = DbsMigrateApi(apiSrc, apiDst, False, pruneBranches)
       transfer = DbsMigrateApi(apiSrc, apiDst, True, pruneBranches)
       if block_name not in [None, ""] :
	       if noParentsReadOnly:
		       transfer.migrateBlockRO(path, block_name)
               else:
		       #print "calling transfer.migrateBlock"
		       transfer.migrateBlock(path, block_name)
	       
       else :
	       if noParentsReadOnly:
		       transfer.migratePathRO(path)
	       else:
		       transfer.migratePath(path)
		       

    except Exception, ex:
      raise DbsBadResponse(exception=ex)

 # ------------------------------------------------------------

