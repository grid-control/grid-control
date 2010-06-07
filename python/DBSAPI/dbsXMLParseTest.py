
import os, re, string, socket, xml.sax, xml.sax.handler
import base64
from xml.sax.saxutils import escape
from cStringIO import StringIO
from xml.sax import SAXParseException

if __name__ == "__main__":

    # Parse the resulting xml output.
    try:
      result = []

      class Handler (xml.sax.handler.ContentHandler):

        def startElement(self, name, attrs):
          if name == 'file_proc_quality':
		print "TESTED"
		print attrs['failed_event_list']

        def endElement(self, name):
            if name == 'TEST':
		print "TEST DONE"

      data="""<?xml version='1.0' standalone='yes'?>
<dbs>
<file_proc_quality lfn='TEST_LFN'
 child_dataset='ABC_DATASET'
 failed_event_count='123'
 description='just kidding'
 processing_status='no_status'
 failed_event_list='1,2,3,4'
/>
</dbs>"""

      xml.sax.parseString (data, Handler ())

    except SAXParseException, ex:
      msg = "Unable to parse XML response from DBS Server"
      msg += "\n  Server has not responded as desired, try setting level=DBSDEBUG"
      raise DbsBadXMLData(args=msg, code="5999")


