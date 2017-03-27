#!/usr/bin/env python
__import__('sys').path.append(__import__('os').path.join(__import__('os').path.dirname(__file__), '..'))
__import__('testfwk').setup(__file__)
# - prolog marker
import os
from testfwk import run_test
from grid_control_cms.sitedb import SiteDB
from web_replay import replay_start
from python_compat import sorted


os.environ['X509_USER_PROXY'] = 'web_replay.py'
replay_start('cms_web_responses')

class Test_SiteDB:
	"""
	>>> site_db = SiteDB()
	>>> site_db.dn_to_username(dn='/C=DE/O=GermanGrid/OU=KIT/CN=Guenter Quast')
	'quastg'
	>>> site_db.username_to_dn(username='quastg')
	'/C=DE/O=GermanGrid/OU=KIT/CN=Guenter Quast'
	>>> site_db.cms_name_to_se(cms_name='T*_DE_KIT')
	['cmssrm-kit.gridka.de']
	>>> sorted(site_db.cms_name_to_se(cms_name='T*_US_FNAL'))
	['cmsdcadisk01.fnal.gov', 'cmseos.fnal.gov']
	>>> site_db.se_to_cms_name(se_name='cmssrm-kit.gridka.de')
	['T1_DE_KIT']
	>>> sorted(site_db.se_to_cms_name(se_name='cmsdcadisk01.fnal.gov'))
	['T1_US_FNAL', 'T1_US_FNAL_Disk', 'T1_US_FNAL_Disk', 'T3_US_FNALLPC']
	>>> sorted(site_db.se_to_cms_name(se_name='cmseos.fnal.gov'))
	['T3_US_FNALLPC']
	"""

run_test()
