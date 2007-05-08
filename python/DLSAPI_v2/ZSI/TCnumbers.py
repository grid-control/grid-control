#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/TCnumbers.py,v 1.2 2002/02/04 17:39:26 rsalz Exp $
'''Typecodes for numbers.
'''

from ZSI import _copyright, _inttypes, _floattypes, _seqtypes, \
        EvaluateException
from ZSI.TC import TypeCode, Integer, Decimal

class IunsignedByte(Integer):
    '''Unsigned 8bit value.
    '''
    tag = "unsignedByte"

class IunsignedShort(Integer):
    '''Unsigned 16bit value.
    '''
    tag = "unsignedShort"

class IunsignedInt(Integer):
    '''Unsigned 32bit value.
    '''
    tag = "unsignedInt"

class IunsignedLong(Integer):
    '''Unsigned 64bit value.
    '''
    tag = "unsignedLong"

class Ibyte(Integer):
    '''Signed 8bit value.
    '''
    tag = "byte"

class Ishort(Integer):
    '''Signed 16bit value.
    '''
    tag = "short"

class Iint(Integer):
    '''Signed 32bit value.
    '''
    tag = "int"

class Ilong(Integer):
    '''Signed 64bit value.
    '''
    tag = "long"

class InegativeInteger(Integer):
    '''Value less than zero.
    '''
    tag = "negativeInteger"

class InonPositiveInteger(Integer):
    '''Value less than or equal to zero.
    '''
    tag = "nonPositiveInteger"

class InonNegativeInteger(Integer):
    '''Value greater than or equal to zero.
    '''
    tag = "nonNegativeInteger"

class IpositiveInteger(Integer):
    '''Value greater than zero.
    '''
    tag = "positiveInteger"

class Iinteger(Integer):
    '''Integer value.
    '''
    tag = "integer"

class IEnumeration(Integer):
    '''Integer value, limited to a specified set of values.
    '''

    def __init__(self, choices, pname=None, **kw):
        Integer.__init__(self, pname, **kw)
        self.choices = choices
        t = type(choices)
        if t in _seqtypes:
            self.choices = tuple(choices)
        elif TypeCode.typechecks:
            raise TypeError(
                'Enumeration choices must be list or sequence, not ' + str(t))
        if TypeCode.typechecks:
            for c in self.choices:
                if type(c) not in _inttypes:
                    raise TypeError('Enumeration choice "' +
                            str(c) + '" is not an integer')

    def parse(self, elt, ps):
        val = Integer.parse(self, elt, ps)
        if val not in self.choices:
            raise EvaluateException('Value "' + str(val) + \
                        '" not in enumeration list',
                    ps.Backtrace(elt))
        return val

class FPfloat(Decimal):
    '''IEEE 32bit floating point value.
    '''
    tag = "float"

class FPdouble(Decimal):
    '''IEEE 64bit floating point value.
    '''
    tag = "double"

class FPEnumeration(FPfloat):
    '''Floating point value, limited to a specified set of values.
    '''

    def __init__(self, choices, pname=None, **kw):
        FPfloat.__init__(self, pname, **kw)
        self.choices = choices
        t = type(choices)
        if t in _seqtypes:
            self.choices = tuple(choices)
        elif TypeCode.typechecks:
            raise TypeError(
                'Enumeration choices must be list or sequence, not ' + str(t))
        if TypeCode.typechecks:
            for c in self.choices:
                if type(c) not in _floattypes:
                    raise TypeError('Enumeration choice "' +
                            str(c) + '" is not floating point number')

    def parse(self, elt, ps):
        val = Decimal.parse(self, elt, ps)
        if val not in self.choices:
            raise EvaluateException('Value "' + str(val) + \
                        '" not in enumeration list',
                    ps.Backtrace(elt))
        return val

if __name__ == '__main__': print _copyright
