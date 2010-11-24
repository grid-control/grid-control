
import sys
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

import inspect

from dbsUtil import *

def isIn(aparent, parentList):
	for i in parentList:
		#print "checking ---> %s " %i['LogicalFileName']
		if i['LogicalFileName'] == aparent['LogicalFileName']:
			return True
	return False
	

def dbsApiImplRemap(self, merged1, merged2, blockName):

	merged2FileList = self.listFiles(path = merged2, blockName = blockName, retriveList=['retrive_parent'])
	merged1FileList = self.listFiles(path = merged1, retriveList=['retrive_parent'])
	for afile in merged2FileList:
		aFileLFN = afile['LogicalFileName']
		print "Checking File %s in Merged dataset2" %aFileLFN
		parentListM2 = afile['ParentList']
		for aparent in parentListM2:
			aparentLFN = aparent['LogicalFileName']
			print "Getting  parent of %s ( is grandparent of original file)" %aparentLFN
			tmpParentList = self.listFiles(patternLFN=aparentLFN, retriveList=['retrive_parent'])
			for atmpParent in tmpParentList:
				grandParentList = atmpParent['ParentList']
				for agrandParent in grandParentList:
					print "Going to check the grand parent %s" %agrandParent['LogicalFileName']
					for aFileInM1 in merged1FileList:
						parentListM1 = aFileInM1['ParentList']
						#print "checking the grandparent in %s" %parentListM1
						if isIn(agrandParent, parentListM1):
							fileM1LFN = aFileInM1['LogicalFileName']
							print '__________________________________________________________'
							print 'INSERTING the real parent %s in Merged dataset2' %fileM1LFN
							print '__________________________________________________________\n\n'
							self.insertFileParent(aFileLFN, fileM1LFN)
						else:
							print "Grand parent  and parent did not match"


			if not isIn(aparent, merged1FileList):
				print '****************************************************************************'
				print 'DELETING the parent %s from Merged dataset2' %aparentLFN
				print '****************************************************************************\n\n'
				self.deleteFileParent(aFileLFN, aparentLFN)
	
	"""
	# Delete all the parents of merged1 dataset
	for afile in merged1FileList:
		print "Cheking File %s in Merged dataset1" %afile['LogicalFileName']
		parentList = afile['ParentList']
		for aparent in parentList:
			print '****************************************************************************'
			print 'DELETING the parent %s from Merged dataset1' %aparent['LogicalFileName']
			print '****************************************************************************\n\n'
			self.deleteFileParent(afile['LogicalFileName'], aparent['LogicalFileName'])
	"""


