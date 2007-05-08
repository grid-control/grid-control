import os, re, string, xml.sax, xml.sax.handler
from xml.sax.saxutils import escape

myxml = "<?xml version='1.0' standalone='yes'?><!-- DBS Version 1 --><dbs><analysis_dataset id='5' analysis_dataset_name='AnalysisDS_b97b1762-a97f-4348-be9b-d10a9445e7ae' annotation='aaaab97b1762-a97f-4348-be9b-d10a9445e7ae' type='1' status='1' creation_date='1066729598000' last_modification_date='2007-01-25 16:41:41.0' created_by='Let_me_try_this' last_modified_by='VIJAY_SEKHRI_DN'><analysis_dataset_definition id='22' analysis_dataset_definition_name='AnalysisDS_Defination_b97b1762-a97f-4348-be9b-d10a9445e7ae' lumi_sections='9997,9996' lumi_section_ranges='1 AND 10,9995 AND 9997' runs='9999, 9998,9998' runs_ranges='1 AND 10,12 AND 18' algorithms='MyVersion1_b97b1762-a97f-4348-be9b-d10a9445e7ae_MyFamily1_b97b1762-a97f-4348-be9b-d10a9445e7ae_MyExe1_b97b1762-a97f-4348-be9b-d10a9445e7ae_DUMMY_HASH_b97b1762-a97f-4348-be9b-d10a9445e7ae,MyVersion2_b97b1762-a97f-4348-be9b-d10a9445e7ae_MyFamily2_b97b1762-a97f-4348-be9b-d10a9445e7ae_MyExe2_b97b1762-a97f-4348-be9b-d10a9445e7ae_DUaMMY_HASH_b97b1762-a97f-4348-be9b-d10a9445e7ae' lfns='TEST_LFN_1_b97b1762-a97f-4348-be9b-d10a9445e7ae,TEST_LFN_2_b97b1762-a97f-4348-be9b-d10a9445e7ae' path='/This_is_a_test_primary_b97b1762-a97f-4348-be9b-d10a9445e7ae/This_is_a_test_tier_SIM_b97b1762-a97f-4348-be9b-d10a9445e7ae/CHILD_This_is_a_test_processed_b97b1762-a97f-4348-be9b-d10a9445e7ae' tiers='This_is_a_test_tier_HIT_b97b1762-a97f-4348-be9b-d10a9445e7ae,This_is_a_test_tier_SIM_b97b1762-a97f-4348-be9b-d10a9445e7ae' analysis_dataset_names='AnalysisDS_b97b1762-a97f-4348-be9b-d10a9445e7ae' user_cut='RunNumber = 2' creation_date='1066729598000' last_modification_date='2007-01-25 16:41:40.0' created_by='Let_me_try_this' last_modified_by='VIJAY_SEKHRI_DN' /></analysis_dataset><SUCCESS/></dbs>"

class Handler (xml.sax.handler.ContentHandler):
           def startElement(self, name, attrs):
		print name

xml.sax.parseString (myxml, Handler ())

