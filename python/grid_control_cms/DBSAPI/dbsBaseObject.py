#
# Revision: 1.3 $"
# Id: DBSXMLParser.java,v 1.3 2006/10/26 18:26:04 afaq Exp $"
#
import dbsException
from dbsValidationTable import ValidationTable
import types, re, string
from dbsValidateTools import *

class DbsBase(dict):
	""" Base Class for all DBS Client Data Types """
	def __init__(self, **args):
		dict.__init__(self)

        def validate(self):
       	    """
	    Validate a DBS Data Object dictionary
	    """
            objName = self.__class__.__name__
	    return
            # Gets the dictionay from the validationTable
            valTable = ValidationTable[objName]
            for key in self.keys():
                if key in valTable.keys():
		     validator = valTable.get(key).get('Validator', None)
                     if validator != None:
                         if not validator(self[key]) :
			    raise dbsException.DbsException(args=
                               "Validation Error: Check the required data type for %s.%s " \
					%(objName, key) )
		     else:
		         raise dbsException.DbsException(args=
				"Validation Error: The type of %s.%s not specified in ValidationTable" \
					%(objName, key) )




