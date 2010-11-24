# Revision: $"
# Id: $"

import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape, unescape
from cStringIO import StringIO

from dbsException import DbsException
from dbsApiException import *

from xml.sax import SAXParseException
import inspect

from dbsUtil import *

def dbsApiImplGetHelp(self, entity = ""):
	funcInfo = inspect.getframeinfo(inspect.currentframe())
	data = self._server._call ({ 'api' : 'getHelp',
			'entity' : entity }, 'GET')
	#print data
	try:
		result = []
		class Handler (xml.sax.handler.ContentHandler):
			self.exampleList = []
			self.attrList = []
			self.entityObj = {}
			self.exampleObj = {}
			self.key = ''
			self.oldkey = ''
			self.tmpS = ''
			def startElement(self, name, attrs):
				#print "in start element"
				#print name
				#import pdb
				#pdb.set_trace()
				#print dir(attrs)
				self.key = str(name)
				#print 'line 1'
				if name == 'key':
					self.exampleList = []
					self.attrList = []
					#print 'line 2'
					self.entityObj = {}
					self.tmpS = ''
					#print 'line 3'
				
				if name == 'dbs':
					self.oldkey = ''
					
				if name == 'desc':
					self.exampleObj = {}
				#print 'line 4'
					#result.append(DbsPrimaryDataset (
			def endElement(self, name):
				#print 'in end element'
				if name == 'key':
					result.append(self.entityObj)
					self.entityObj['examples'] = self.exampleList
					self.entityObj['attrs'] = self.attrList
				if name == 'query':
					self.exampleList.append(self.exampleObj)

			def characters(self, s):
				#print 'line 5'
				#s = str(unescape(s)).strip()
				#print 'line 6'
				s = s.strip()
				#print "s is %s" %s
				if self.oldkey != self.key:
					self.tmpS = s
				#print 'line 7'
				#print 'self.key is %s ' %self.key
				#print 'len of s is %d' %len(s)
				if s not in [None, '', '\n', '\0', '\t', '\t\t']:
						#if(len(s) > 1):
						if ((self.key == 'query') & (self.oldkey == 'query')) | ((self.key == 'desc') & (self.oldkey == 'desc')):
							#print 'before in query tmpS is %s' %self.tmpS
							self.tmpS += ' ' + s
							#print 'after in query tmpS is %s' %self.tmpS
							s = self.tmpS

						if (self.key == 'desc') | (self.key == 'query') :
							self.exampleObj[str(self.key)] = s
						elif (self.key == 'attr'):
							self.attrList.append(s)
						else:
							self.entityObj[str(self.key)] = s
						#print 'line 8'
				self.oldkey = self.key

	
		#data.replace('gt;', 'GREATER_THAN')
		#data.replace('lt;', 'LESS_THAN')
		#print data
		xml.sax.parseString (data, Handler ())
		#print result
		return result
	except SAXParseException, ex:
		msg = "Unable to parse XML response from DBS Server"
		msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
		raise DbsBadXMLData(args=msg, code="5999")


