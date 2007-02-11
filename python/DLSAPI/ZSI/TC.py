#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/TC.py,v 1.38 2005/02/15 17:44:48 rsalz Exp $
'''General typecodes.
'''

from ZSI import _copyright, _children, _child_elements, \
        _floattypes, _stringtypes, _seqtypes, _find_arraytype, _find_href, \
        _find_encstyle, _textprotect, \
        _find_xsi_attr, _find_type, _Node, \
        EvaluateException, _valid_encoding

from ZSI.wstools.Namespaces import SCHEMA, SOAP
try:
    from xml.dom.ext import Canonicalize
except:
    from ZSI.compat import Canonicalize

import re, types, time

from base64 import decodestring as b64decode, encodestring as b64encode
from urllib import unquote as urldecode, quote as urlencode
from binascii import unhexlify as hexdecode, hexlify as hexencode


_is_xsd_or_soap_ns = lambda ns: ns in [
                        SCHEMA.XSD3, SOAP.ENC, SCHEMA.XSD1, SCHEMA.XSD2, ]
_find_nil = lambda E: _find_xsi_attr(E, "null") or _find_xsi_attr(E, "nil")


def _get_object_id(pyobj):
    x = id(pyobj)
    # Python 2.3.x will generate a FutureWarning for negative IDs, so
    # we use a different prefix character to ensure uniqueness, and
    # call abs() to avoid the warning.
    if x < 0:
        return 'x%x' % abs(x)
    else:
        return 'o%x' % x


class TypeCode:
    '''The parent class for all parseable SOAP types.
    Class data:
        typechecks -- do init-time type checking if non-zero
    Class data subclasses may define:
        parselist -- list of valid SOAP types for this class, as
            (uri,name) tuples, where a uri of None means "all the XML
            Schema namespaces"
        errorlist -- parselist in a human-readable form; will be
            generated if/when needed
        seriallist -- list of Python types or user-defined classes
            that this typecode can serialize.
    '''

    typechecks = 1

    def __init__(self, pname=None, oname=None, aname=None, optional=0,
    typed=1, repeatable=0, unique=0, ons=None, **kw):
        '''Baseclass initialization.
        Instance data (and usually keyword arg)
            pname -- the parameter name (localname).
            nspname -- the namespace for the parameter;
                None to ignore the namespace
            oname -- output name
            ons -- the namespace prefix of the oname
            typed -- output xsi:type attribute
            repeatable -- element can appear more than once
            optional -- the item is optional
            default -- default value
            unique -- data item is not aliased (no href/id needed)
        '''
        if type(pname) in _seqtypes:
            self.nspname, self.pname = pname
        else:
            self.nspname, self.pname = None, pname
        # Set oname before splitting pname, and aname after.
        self.oname = oname or self.pname
        self.ons = ons
        if self.pname:
            i = str(self.pname).find(':')
            if i > -1: self.pname = self.pname[i + 1:]
        self.aname = aname or self.pname

        self.optional = optional
        self.typed = typed
        self.repeatable = repeatable
        self.unique = unique
        if kw.has_key('default'): self.default = kw['default']
        if kw.has_key('rpc'): self.rpc = kw['rpc']

    def parse(self, elt, ps):
        '''elt -- the DOM element being parsed
        ps -- the ParsedSoap object.
        '''
        raise EvaluateException("Unimplemented evaluation", ps.Backtrace(elt))

    def SimpleHREF(self, elt, ps, tag):
        '''Simple HREF for non-string and non-struct and non-array.
        '''
        if len(_children(elt)): return elt
        href = _find_href(elt)
        if not href:
            if self.optional: return None
            raise EvaluateException('Non-optional ' + tag + ' missing',
                    ps.Backtrace(elt))
        return ps.FindLocalHREF(href, elt, 0)

    def get_parse_and_errorlist(self):
        """Get the parselist and human-readable version, errorlist is returned,
        because it is used in error messages.
        """
        d = self.__class__.__dict__
        parselist = d.get('parselist')
        errorlist = d.get('errorlist')
        if parselist and not errorlist:
            errorlist = []
            for t in parselist:
                if t[1] not in errorlist: errorlist.append(t[1])
            errorlist = ' or '.join(errorlist)
            d['errorlist'] = errorlist
        return (parselist, errorlist)

    def checkname(self, elt, ps):
        '''See if the name and type of the "elt" element is what we're
        looking for.   Return the element's type.
        '''

        parselist,errorlist = self.get_parse_and_errorlist()
        ns, name = elt.namespaceURI, elt.localName

        if ns == SOAP.ENC:
            # Element is in SOAP namespace, so the name is a type.
            if parselist and \
            (None, name) not in parselist and (ns, name) not in parselist:
                raise EvaluateException(
                'Type mismatch (got %s wanted %s) (SOAP encoding namespace)' % \
                        (name, errorlist), ps.Backtrace(elt))
            return (ns, name)

        # Not a type, check name matches.
        if self.nspname and ns != self.nspname:
            raise EvaluateException('Type NS mismatch (got %s wanted %s)' % \
                (ns, self.nspname), ps.Backtrace(elt))

        if self.pname and name != self.pname:
            raise EvaluateException('Name mismatch (got %s wanted %s)' % \
                (name, self.pname), ps.Backtrace(elt))
        return self.checktype(elt, ps)

    def checktype(self, elt, ps):
        '''See if the type of the "elt" element is what we're looking for.
        Return the element's type.
        '''
        type = _find_type(elt)
        if type is None or type is "":
            return (None,None)

        # Parse the QNAME.
        list = type.split(':')
        if len(list) == 1:
            list = [None,list[0]]
        elif len(list) != 2:
            raise EvaluateException('Malformed type attribute (not two colons)',
                    ps.Backtrace(elt))
        uri = ps.GetElementNSdict(elt).get(list[0])
        if uri is None:
            raise EvaluateException('Malformed type attribute (bad NS)',
                    ps.Backtrace(elt))
        type = list[1]
        parselist,errorlist = self.get_parse_and_errorlist()
        if not parselist or \
        (uri,type) in parselist or \
        (_is_xsd_or_soap_ns(uri) and (None,type) in parselist):
            return (uri,type)
        raise EvaluateException(
                'Type mismatch (%s namespace) (got %s wanted %s)' % \
                (uri, type, errorlist), ps.Backtrace(elt))

    def name_match(self, elt):
        '''Simple boolean test to see if we match the element name.
        '''
        return self.pname == elt.localName and \
                    self.nspname in [None, elt.namespaceURI]

    def nilled(self, elt, ps):
        '''Is the element NIL, and is that okay?
        '''
        if _find_nil(elt) not in [ "true",  "1"]: return 0
        if not self.optional:
            raise EvaluateException('Required element is NIL',
                    ps.Backtrace(elt))
        return 1

    def simple_value(self, elt, ps):
        '''Get the value of the simple content of this element.
        '''
        if not _valid_encoding(elt):
            raise EvaluateException('Invalid encoding', ps.Backtrace(elt))
        c = _children(elt)
        if len(c) == 0:
            raise EvaluateException('Value missing', ps.Backtrace(elt))
        for c_elt in c:
            if c_elt.nodeType == _Node.ELEMENT_NODE:
                raise EvaluateException('Sub-elements in value',
                    ps.Backtrace(c_elt))

        # It *seems* to be consensus that ignoring comments and
        # concatenating the text nodes is the right thing to do.
        return ''.join([E.nodeValue for E in c
                if E.nodeType 
                in [ _Node.TEXT_NODE, _Node.CDATA_SECTION_NODE ]])


class Any(TypeCode):
    '''When the type isn't defined in the schema, but must be specified
    in the incoming operation.
        parsemap -- a type to class mapping (updated by descendants), for
                parsing
        serialmap -- same, for (outgoing) serialization
    '''
    parsemap, serialmap = {}, {}

    def __init__(self, pname=None, aslist=0, **kw):
        TypeCode.__init__(self, pname, **kw)
        self.aslist = aslist
        # If not derived, and optional isn't set, make us optional
        # so that None can be parsed.
        if self.__class__ == Any and not kw.has_key('optional'):
            self.optional = 1

    def listify(self, v):
        if self.aslist: return [ v[k] for k in v.keys() ]
        return v

    def parse_into_dict_or_list(self, elt, ps):
        c = _child_elements(elt)
        count = len(c)
        v = {}
        if count == 0:
            href = _find_href(elt)
            if not href: return v
            elt = ps.FindLocalHREF(href, elt)
            self.checktype(elt, ps)
            c = _child_elements(elt)
            count = len(c)
            if count == 0: return self.listify(v)
        if self.nilled(elt, ps): return None
        for c_elt in c:
            v[str(c_elt.nodeName)] = self.parse(c_elt, ps)
        return self.listify(v)

    def parse(self, elt, ps):
        (ns,type) = self.checkname(elt, ps)
        if not type and self.nilled(elt, ps): return None
        if len(_children(elt)) == 0:
            href = _find_href(elt)
            if not href:
                if self.optional: return None
                raise EvaluateException('Non-optional Any missing',
                        ps.Backtrace(elt))
            elt = ps.FindLocalHREF(href, elt)
            (ns,type) = self.checktype(elt, ps)
        if not type and elt.namespaceURI == SOAP.ENC:
            ns,type = SOAP.ENC, elt.localName
        if not type or (ns,type) == (SOAP.ENC,'Array'):
            if self.aslist or _find_arraytype(elt):
                return [ Any(aslist=self.aslist).parse(e, ps)
                            for e in _child_elements(elt) ]
            if len(_child_elements(elt)) == 0:
                raise EvaluateException("Any cannot parse untyped element",
                        ps.Backtrace(elt))
            return self.parse_into_dict_or_list(elt, ps)
        parser = Any.parsemap.get((ns,type))
        if not parser and _is_xsd_or_soap_ns(ns):
            parser = Any.parsemap.get((None,type))
        if not parser:
            raise EvaluateException('''Any can't parse element''',
                    ps.Backtrace(elt))
        return parser.parse(elt, ps)

    def serialize(self, sw, pyobj, name=None, attrtext='', rpc=None, **kw):
        # What is attrtext for? It isn't used in the function.
        if hasattr(pyobj, 'typecode'):
            if isinstance(pyobj.typecode, Any):
                raise EvaluateException, 'Any can\'t serialize Any'
            pyobj.typecode.serialize(sw, pyobj, **kw)
            return

        n = name or self.oname or rpc or 'E%s' % _get_object_id(pyobj)
        if self.ons:
            n = '%s:%s' % (self.ons, n)
        kw['name'] = n
        tc = type(pyobj)
        if tc == types.DictType or self.aslist:
            if rpc is not None:
                print >>sw, '<%s>' % rpc
            else:
                if type(pyobj) != types.InstanceType:
                    print >>sw, '<%s>' % n

            if self.aslist:
                for val in pyobj:
                    Any().serialize(sw, val)
            else:
                for key,val in pyobj.items():
                    Any(pname=key).serialize(sw, val)

            if rpc is not None:
                print >>sw, '</%s>' % rpc
            else:
                if type(pyobj) != types.InstanceType:
                    print >>sw, '</%s>' % n
            return
        if tc in _seqtypes:
            if kw.get('typed', self.typed):
                tstr = ' SOAP-ENC:arrayType="xsd:anyType[%d]"' % len(pyobj)
            else:
                tstr = ''
            print >>sw, '<%s%s>' % (n, tstr)
            a = Any()
            for e in pyobj:
                a.serialize(sw, e, name='element')
            print >>sw, '</%s>' % n
            return
        if tc == types.InstanceType:
            tc = pyobj.__class__
            if hasattr(pyobj, 'typecode'):
                serializer = pyobj.typecode.serialmap.get(tc)
            else:
                serializer = Any.serialmap.get(tc)
            if not serializer:
                tc = (types.ClassType, pyobj.__class__.__name__)
                serializer = Any.serialmap.get(tc)
        else:
            serializer = Any.serialmap.get(tc)
            if not serializer and isinstance(pyobj, time.struct_time):
                from ZSI.TCtimes import gDateTime
                serializer = gDateTime()

        if not serializer:
            # Last-chance; serialize instances as dictionary
            if type(pyobj) != types.InstanceType:
                raise EvaluateException('''Any can't serialize ''' + \
                        repr(pyobj))
            self.serialize(sw, pyobj.__dict__, **kw)
        else:
            # Try to make the element name self-describing
            if not name and not self.oname:
                tag = getattr(serializer, 'tag', None)
                if tag:
                    if tag.find(':') == -1: tag = 'SOAP-ENC:' + tag
                    kw['name'] = kw['oname'] = tag
                    kw['typed'] = 0
            else:
                # If element name is specified, must provide xsi:type
                kw['typed'] = 1
            serializer.serialize(sw, pyobj, **kw)


def RegisterType(C, clobber=0, *args, **keywords):
    instance = apply(C, args, keywords)
    for t in C.__dict__.get('parselist', []):
        prev = Any.parsemap.get(t)
        if prev:
            if prev.__class__ == C: continue
            if not clobber:
                raise TypeError(
                    str(C) + ' duplicating parse registration for ' + str(t))
        Any.parsemap[t] = instance
    for t in C.__dict__.get('seriallist', []):
        ti = type(t)
        if ti in [ types.TypeType, types.ClassType]:
            key = t
        elif ti in _stringtypes:
            key = (types.ClassType, t)
        else:
            raise TypeError(str(t) + ' is not a class name')
        prev = Any.serialmap.get(key)
        if prev:
            if prev.__class__ == C: continue
            if not clobber:
                raise TypeError(
                    str(C) + ' duplicating serial registration for ' + str(t))
        Any.serialmap[key] = instance


class Void(TypeCode):
    '''A null type.
    '''
    parselist = [ (None,'nil') ]
    seriallist = [ types.NoneType ]

    def parse(self, elt, ps):
        self.checkname(elt, ps)
        if len(_children(elt)):
            raise EvaluateException('Void got a value', ps.Backtrace(elt))
        return None

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        n = name or self.oname or ('E%s' % _get_object_id(pyobj))
        print >>sw, '''<%s%s xsi:nil="1"/>''' % (n, attrtext)

class String(TypeCode):
    '''A string type.
    '''
    parselist = [ (None,'string') ]
    seriallist = [ types.StringType, types.UnicodeType ]
    tag = 'string'

    def __init__(self, pname=None, strip=1, textprotect=1, **kw):
        TypeCode.__init__(self, pname, **kw)
        if kw.has_key('resolver'): self.resolver = kw['resolver']
        self.strip = strip
        self.textprotect = textprotect

    def parse(self, elt, ps):
        self.checkname(elt, ps)
        if len(_children(elt)) == 0:
            href = _find_href(elt)
            if not href:
                if _find_nil(elt) not in [ "true",  "1"]:
                    # No content, no HREF, not NIL:  empty string
                    return ""
                # No content, no HREF, and is NIL...
                if self.optional: return None
                raise EvaluateException('Non-optional string missing',
                        ps.Backtrace(elt))
            if href[0] != '#':
                return ps.ResolveHREF(href, self)
            elt = ps.FindLocalHREF(href, elt)
            self.checktype(elt, ps)
        if self.nilled(elt, ps): return None
        if len(_children(elt)) == 0: return ''
        v = self.simple_value(elt, ps)
        if self.strip: v = v.strip()
        return v

    def serialize(self, sw, pyobj, name=None, attrtext='', orig=None, **kw):
        objid = _get_object_id(pyobj)
        n = name or self.oname or ('E' + objid)
        if type(pyobj) in _seqtypes:
            print >>sw, '<%s%s href="%s"/>' % (n, attrtext, pyobj[0])
            return
        if not self.unique and sw.Known(orig or pyobj):
            print >>sw, '<%s%s href="#%s"/>' % (n, attrtext, objid)
            return
        if type(pyobj) == types.UnicodeType: pyobj = pyobj.encode('utf-8')
        if kw.get('typed', self.typed):
            if self.tag and self.tag.find(':') != -1:
                tstr = ' xsi:type="%s"' % self.tag
            else:
                tstr = ' xsi:type="xsd:%s"' % (self.tag or 'string')
        else:
            tstr = ''
        if self.unique:
            idstr = ''
        else:
            idstr = ' id="%s"' % objid
        if self.textprotect: pyobj = _textprotect(pyobj)

        # ignore the xmlns if it was explicitly stated
        i = n.find('xmlns')
        if i > 0:
            ctag = '</%s>' % n[:i - 1]
        else:
            ctag = '</%s>' % n

        print >>sw, \
            '<%s%s%s%s>%s%s' % (n, attrtext, idstr, tstr, pyobj, ctag)


class URI(String):
    '''A URI.
    '''
    parselist = [ (None,'anyURI') ]
    tag = 'anyURI'

    def parse(self, elt, ps):
        val = String.parse(self, elt, ps)
        return urldecode(val)

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        String.serialize(self, sw, urlencode(pyobj), orig=pyobj, **kw)

class QName(String):
    '''A QName type
    '''
    parselist = [ (None,'QName') ]
    tag = 'QName'

class Token(String):
    '''an xsd:token type
    '''
    parselist = [ (None, 'token') ]
    tag = 'token'

class Base64String(String):
    '''A Base64 encoded string.
    '''
    parselist = [ (None,'base64Binary'), (SOAP.ENC, 'base64') ]
    tag = 'SOAP-ENC:base64'

    def parse(self, elt, ps):
        val = String.parse(self, elt, ps)
        return b64decode(val.replace(' ', '').replace('\n','').replace('\r',''))

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        String.serialize(self, sw, '\n' + b64encode(pyobj), orig=pyobj, **kw)

class HexBinaryString(String):
    '''Hex-encoded binary (yuk).
    '''
    parselist = [ (None,'hexBinary') ]
    tag = 'hexBinary'

    def parse(self, elt, ps):
        val = String.parse(self, elt, ps)
        return hexdecode(val)

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        String.serialize(self, sw, hexencode(pyobj).upper(), orig=pyobj, **kw)

class XMLString(String):
    '''A string that represents an XML document
    '''

    def __init__(self, pname=None, readerclass=None, **kw):
        String.__init__(self, pname, **kw)
        self.readerclass = readerclass

    def parse(self, elt, ps):
        if not self.readerclass:
            from xml.dom.ext.reader import PyExpat
            self.readerclass = PyExpat.Reader
        v = String.parse(self, elt, ps)
        return self.readerclass().fromString(v)

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        String.serialize(self, sw, Canonicalize(pyobj), name, attrtext, orig=pyobj, **kw)

class Enumeration(String):
    '''A string type, limited to a set of choices.
    '''

    def __init__(self, choices, pname=None, **kw):
        String.__init__(self, pname, **kw)
        t = type(choices)
        if t in _seqtypes:
            self.choices = tuple(choices)
        elif TypeCode.typechecks:
            raise TypeError(
                'Enumeration choices must be list or sequence, not ' + str(t))
        if TypeCode.typechecks:
            for c in self.choices:
                if type(c) not in _stringtypes:
                    raise TypeError(
                        'Enumeration choice ' + str(c) + ' is not a string')

    def parse(self, elt, ps):
        val = String.parse(self, elt, ps)
        if val not in self.choices:
            raise EvaluateException('Value not in enumeration list',
                    ps.Backtrace(elt))
        return val


# This is outside the Integer class purely for code esthetics.
_ignored = []

class Integer(TypeCode):
    '''Common handling for all integers.
    '''

    ranges = {
        'unsignedByte':         (0, 255),
        'unsignedShort':        (0, 65535),
        'unsignedInt':          (0, 4294967295L),
        'unsignedLong':         (0, 18446744073709551615L),

        'byte':                 (-128, 127),
        'short':                (-32768, 32767),
        'int':                  (-2147483648L, 2147483647),
        'long':                 (-9223372036854775808L, 9223372036854775807L),

        'negativeInteger':      (_ignored, -1),
        'nonPositiveInteger':   (_ignored, 0),
        'nonNegativeInteger':   (0, _ignored),
        'positiveInteger':      (1, _ignored),

        'integer':              (_ignored, _ignored)
    }
    parselist = [ (None,k) for k in ranges.keys() ]
    seriallist = [ types.IntType, types.LongType ]
    tag = None

    def __init__(self, pname=None, format='%d', **kw):
        TypeCode.__init__(self, pname, **kw)
        self.format = format

    def parse(self, elt, ps):
        (ns,type) = self.checkname(elt, ps)
        elt = self.SimpleHREF(elt, ps, 'integer')
        if not elt: return None
        tag = getattr(self.__class__, 'tag')
        if tag:
            if type is None:
                type = tag
            elif tag != type:
                raise EvaluateException('Integer type mismatch; ' \
                        'got %s wanted %s' % (type,tag), ps.Backtrace(elt))
        
        if self.nilled(elt, ps): return None
        v = self.simple_value(elt, ps)
        try:
            v = int(v)
        except:
            try:
                v = long(v)
            except:
                raise EvaluateException('Unparseable integer',
                    ps.Backtrace(elt))
        (rmin, rmax) = Integer.ranges.get(type, (_ignored, _ignored))
        if rmin != _ignored and v < rmin:
            raise EvaluateException('Underflow, less than ' + repr(rmin),
                    ps.Backtrace(elt))
        if rmax != _ignored and v > rmax:
            raise EvaluateException('Overflow, greater than ' + repr(rmax),
                    ps.Backtrace(elt))
        return v

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        n = name or self.oname or ('E%s' % _get_object_id(pyobj))
        if kw.get('typed', self.typed):
            tstr = ' xsi:type="xsd:%s"' % (self.tag or 'integer')
        else:
            tstr = ''
        print >>sw, ('<%s%s%s>' + self.format + '</%s>') % \
                (n, attrtext, tstr, pyobj, n)

# See credits, below.
def _make_inf():
    x = 2.0
    x2 = x * x
    i = 0
    while i < 100 and x != x2:
        x = x2
        x2 = x * x
        i = i + 1
    if x != x2:
        raise ValueError("This machine's floats go on forever!")
    return x

# This is outside the Decimal class purely for code esthetics.
_magicnums = { }
try:
    _magicnums['INF'] = float('INF')
    _magicnums['-INF'] = float('-INF')
except:
    _magicnums['INF'] = _make_inf()
    _magicnums['-INF'] = -_magicnums['INF']

# The following comment and code was written by Tim Peters in
# article <001401be92d2$09dcb800$5fa02299@tim> in comp.lang.python,
# also available at the following URL:
# http://groups.google.com/groups?selm=001401be92d2%2409dcb800%245fa02299%40tim
# Thanks, Tim!

# NaN-testing.
#
# The usual method (x != x) doesn't work.
# Python forces all comparisons thru a 3-outcome cmp protocol; unordered
# isn't a possible outcome.  The float cmp outcome is essentially defined
# by this C expression (combining some cross-module implementation
# details, and where px and py are pointers to C double):
#   px == py ? 0 : *px < *py ? -1 : *px > *py ? 1 : 0
# Comparing x to itself thus always yields 0 by the first clause, and so
# x != x is never true.
# If px and py point to distinct NaN objects, a strange thing happens:
# 1. On scrupulous 754 implementations, *px < *py returns false, and so
#    does *px > *py.  Python therefore returns 0, i.e. "equal"!
# 2. On Pentium HW, an unordered outcome sets an otherwise-impossible
#    combination of condition codes, including both the "less than" and
#    "equal to" flags.  Microsoft C generates naive code that accepts
#    the "less than" flag at face value, and so the *px < *py clause
#    returns true, and Python returns -1, i.e. "not equal".
# So with a proper C 754 implementation Python returns the wrong result,
# and under MS's improper 754 implementation Python yields the right
# result -- both by accident.  It's unclear who should be shot <wink>.
#
# Anyway, the point of all that was to convince you it's tricky getting
# the right answer in a portable way!
def isnan(x):
    """x -> true iff x is a NaN."""
    # multiply by 1.0 to create a distinct object (x < x *always*
    # false in Python, due to object identity forcing equality)
    if x * 1.0 < x:
        # it's a NaN and this is MS C on a Pentium
        return 1
    # Else it's non-NaN, or NaN on a non-MS+Pentium combo.
    # If it's non-NaN, then x == 1.0 and x == 2.0 can't both be true,
    # so we return false.  If it is NaN, then assuming a good 754 C
    # implementation Python maps both unordered outcomes to true.
    return 1.0 == x and x == 2.0

class Decimal(TypeCode):
    '''Parent class for floating-point numbers.
    '''

    parselist = [ (None,'decimal'), (None,'float'), (None,'double') ]
    seriallist = _floattypes
    tag = None
    ranges =  {
        'float': ( 7.0064923216240861E-46,
                        -3.4028234663852886E+38, 3.4028234663852886E+38 ),
        'double': ( 2.4703282292062327E-324,
                        -1.7976931348623158E+308, 1.7976931348623157E+308),
    }
    zeropat = re.compile('[1-9]')

    def __init__(self, pname=None, format='%f', **kw):
        TypeCode.__init__(self, pname, **kw)
        self.format = format

    def parse(self, elt, ps):
        (ns,type) = self.checkname(elt, ps)
        elt = self.SimpleHREF(elt, ps, 'floating-point')
        if not elt: return None
        tag = getattr(self.__class__, 'tag')
        if tag:
            if type is None:
                type = tag
            elif tag != type:
                raise EvaluateException('Floating point type mismatch; ' \
                        'got %s wanted %s' % (type,tag), ps.Backtrace(elt))
        # Special value?
        if self.nilled(elt, ps): return None
        v = self.simple_value(elt, ps)
        m = _magicnums.get(v)
        if m: return m

        try:
            fp = float(v)
        except:
            raise EvaluateException('Unparseable floating point number',
                    ps.Backtrace(elt))
        if str(fp).lower() in [ 'inf', '-inf', 'nan', '-nan' ]:
            raise EvaluateException('Floating point number parsed as "' + \
                    str(fp) + '"', ps.Backtrace(elt))
        if fp == 0 and Decimal.zeropat.search(v):
            raise EvaluateException('Floating point number parsed as zero',
                    ps.Backtrace(elt))
        (rtiny, rneg, rpos) = Decimal.ranges.get(type, (None, None, None))
        if rneg and fp < 0 and fp < rneg:
            raise EvaluateException('Negative underflow', ps.Backtrace(elt))
        if rtiny and fp > 0 and fp < rtiny:
            raise EvaluateException('Positive underflow', ps.Backtrace(elt))
        if rpos and fp > 0 and fp > rpos:
            raise EvaluateException('Overflow', ps.Backtrace(elt))
        return fp

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        n = name or self.oname or ('E%s' % _get_object_id(pyobj))
        if kw.get('typed', self.typed):
            tstr = ' xsi:type="xsd:%s"' % (self.tag or 'decimal')
        else:
            tstr = ''
        if pyobj == _magicnums['INF']:
            print >>sw, ('<%s%s%s>INF</%s>') % (n, attrtext, tstr, n)
        elif pyobj == _magicnums['-INF']:
            print >>sw, ('<%s%s%s>-INF</%s>') % (n, attrtext, tstr, n)
        elif isnan(pyobj):
            print >>sw, ('<%s%s%s>NaN</%s>') % (n, attrtext, tstr, n)
        else:
            print >>sw, ('<%s%s%s>' + self.format + '</%s>') % \
                (n, attrtext, tstr, pyobj, n)

class Boolean(TypeCode):
    '''A boolean.
    '''

    parselist = [ (None,'boolean') ]

    def parse(self, elt, ps):
        self.checkname(elt, ps)
        elt = self.SimpleHREF(elt, ps, 'boolean')
        if not elt: return None
        if self.nilled(elt, ps): return None
        v = self.simple_value(elt, ps).lower()
        if v == 'false': return 0
        if v == 'true': return 1
        try:
            v = int(v)
        except:
            try:
                v = long(v)
            except:
                raise EvaluateException('Unparseable boolean',
                    ps.Backtrace(elt))
        if v: return 1
        return 0

    def serialize(self, sw, pyobj, name=None, attrtext='', **kw):
        n = name or self.oname or ('E%s' % _get_object_id(pyobj))
        if kw.get('typed', self.typed):
            tstr = ' xsi:type="xsd:boolean"'
        else:
            tstr = ''
        pyobj = (pyobj and 1) or 0
        print >>sw, '<%s%s%s>%d</%s>' % (n, attrtext, tstr, pyobj, n)


class XML(TypeCode):
    '''Opaque XML which shouldn't be parsed.
        comments -- preserve comments
        inline -- don't href/id when serializing
        resolver -- object to resolve href's
        wrapped -- put a wrapper element around it
    '''

    # Clone returned data?
    copyit = 0

    def __init__(self, pname=None, comments=0, inline=0, wrapped=1, **kw):
        TypeCode.__init__(self, pname, **kw)
        self.comments = comments
        self.inline = inline
        if kw.has_key('resolver'): self.resolver = kw['resolver']
        self.wrapped = wrapped
        self.copyit = kw.get('copyit', XML.copyit)

    def parse(self, elt, ps):
        if not self.wrapped:
            return elt
        c = _child_elements(elt)
        if not c:
            href = _find_href(elt)
            if not href:
                if self.optional: return None
                raise EvaluateException('Embedded XML document missing',
                        ps.Backtrace(elt))
            if href[0] != '#':
                return ps.ResolveHREF(href, self)
            elt = ps.FindLocalHREF(href, elt)
            c = _child_elements(elt)
        if _find_encstyle(elt) != "":
            #raise EvaluateException('Embedded XML has unknown encodingStyle',
            #       ps.Backtrace(elt)
            pass
        if len(c) != 1:
            raise EvaluateException('Embedded XML has more than one child',
                    ps.Backtrace(elt))
        if self.copyit: return c[0].cloneNode(1)
        return c[0]

    def serialize(self, sw, pyobj, name=None, attrtext='',
    unsuppressedPrefixes=[], **kw):
        if not self.wrapped:
            Canonicalize(pyobj, sw, unsuppressedPrefixes=unsuppressedPrefixes,
                comments=self.comments)
            return
        objid = _get_object_id(pyobj)
        n = name or self.oname or ('E' + objid)
        if type(pyobj) in _stringtypes:
            print >>sw, '<%s%s href="%s"/>' % (n, attrtext, pyobj)
        elif kw.get('inline', self.inline):
            self.cb(sw, pyobj, unsuppressedPrefixes)
        else:
            print >>sw, '<%s%s href="#%s"/>' % (n, attrtext, objid)
            sw.AddCallback(self.cb, pyobj, unsuppressedPrefixes)

    def cb(self, sw, pyobj, unsuppressedPrefixes=[]):
        if sw.Known(pyobj): return
        objid = _get_object_id(pyobj)
        n = self.pname or ('E' + objid)
        print >>sw, '<%s SOAP-ENC:encodingStyle="" id="%s">' % (n, objid)
        Canonicalize(pyobj, sw, unsuppressedPrefixes=unsuppressedPrefixes,
            comments=self.comments)
        print >>sw, '</%s>' % n

from TCnumbers import *
from TCtimes import *
from TCcompound import *
from TCapache import *

if __name__ == '__main__': print _copyright
