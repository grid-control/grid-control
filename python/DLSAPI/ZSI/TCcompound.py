#! /usr/bin/env python
# $Header: /cvsroot/pywebsvcs/zsi/ZSI/TCcompound.py,v 1.34 2005/02/04 05:16:10 fdrake Exp $
'''Compound typecodes.
'''

from ZSI import _copyright, _children, _child_elements, \
        _inttypes, _stringtypes, _seqtypes, _find_arraytype, _find_href, \
        _find_type, \
        EvaluateException
from ZSI.TC import TypeCode, Any, _get_object_id
from ZSI.wstools.Namespaces import SCHEMA, SOAP
import re, types

_find_arrayoffset = lambda E: E.getAttributeNS(SOAP.ENC, "offset")
_find_arrayposition = lambda E: E.getAttributeNS(SOAP.ENC, "position")

_offset_pat = re.compile(r'\[[0-9]+\]')
_position_pat = _offset_pat


def _check_typecode_list(ofwhat, tcname):
    '''Check a list of typecodes for compliance with Struct or Choice
    requirements.'''
    for o in ofwhat:
        if not isinstance(o, TypeCode):
            raise TypeError(
                tcname + ' ofwhat outside the TypeCode hierarchy, ' +
                str(o.__class__))
        if o.pname is None:
            raise TypeError(tcname + ' element ' + str(o) + ' has no name')

class Struct(TypeCode):
    '''A structure.
    '''

    def __init__(self, pyclass, ofwhat, pname=None, inorder=0, inline=0,
    mutable=1, hasextras=0, **kw):
        '''pyclass -- the Python class to hold the fields
        ofwhat -- a list of fields to be in the struct
        hasextras -- ignore extra input fields
        inorder -- fields must be in exact order or not
        inline -- don't href/id when serializing
        mutable -- object could change between multiple serializations
        type -- the (URI,localname) of the datatype
        '''
        TypeCode.__init__(self, pname, **kw)
        self.pyclass = pyclass
        self.inorder = inorder
        self.inline = inline
        self.mutable = mutable
        if self.mutable: self.inline = 1
        self.hasextras = hasextras
        self.type = kw.get('type')
        t = type(ofwhat)
        if t not in _seqtypes:
            raise TypeError(
                'Struct ofwhat must be list or sequence, not ' + str(t))
        self.ofwhat = tuple(ofwhat)
        if pname is None and kw.has_key('typed'):
            Any.parsemap[self.type] = self
            t = kw['typed']
            for w in self.ofwhat: w.typed = t
        if TypeCode.typechecks:
            if self.pyclass is not None and type(self.pyclass) != types.ClassType:
                raise TypeError('pyclass must be None or a class, not ' +
                        str(type(self.pyclass)))
            _check_typecode_list(self.ofwhat, 'Struct')

    def parse(self, elt, ps):
        #if elt.localName != self.pname:
        #    elt = elt.getElementsByTagName(self.pname)[0]
        self.checkname(elt, ps)
        if self.type and \
        self.checktype(elt, ps) not in [ self.type, (None,None) ]:
            raise EvaluateException('Struct has wrong type', ps.Backtrace(elt))
        href = _find_href(elt)
        if href:
            if _children(elt):
                raise EvaluateException('Struct has content and HREF',
                        ps.Backtrace(elt))
            elt = ps.FindLocalHREF(href, elt)
        c = _child_elements(elt)
        count = len(c)
        if self.nilled(elt, ps): return None
        repeatable_args = False
        for tc in self.ofwhat:
            if tc.repeatable:
                repeatable_args = True
                break

        if not repeatable_args:
            if count > len(self.ofwhat) and not self.hasextras:
                raise EvaluateException('Too many sub-elements', ps.Backtrace(elt))

        # Create the object.
        v = {}

        # Clone list of kids (we null it out as we process)
        c, crange = c[:], range(len(c))
        # Loop over all items we're expecting
        for i,what in [ (i, self.ofwhat[i]) for i in range(len(self.ofwhat)) ]:
            # Loop over all available kids
            for j,c_elt in [ (j, c[j]) for j in crange if c[j] ]:
                if what.name_match(c_elt):
                    # Parse value, and mark this one done.
                    try:
                        value = what.parse(c_elt, ps)
                    except EvaluateException, e:
                        e.str = '%s.%s: %s' % \
                                (self.pname or '?', what.aname or '?', e.str)
                        raise e
                    if what.repeatable:
                        if v.has_key(what.aname):
                            v[what.aname].append(value)
                        else:
                            v[what.aname] = [value]
                        c[j] = None
                        continue
                    else:
                        v[what.aname] = value
                    c[j] = None
                    break
                # No match; if it was supposed to be here, that's an error.
                if self.inorder and i == j:
                    raise EvaluateException('Out of order struct',
                            ps.Backtrace(c_elt))
            else:
                if not what.optional and not v.has_key(what.aname):
                    raise EvaluateException('Element "' + what.aname + \
                        '" missing from struct', ps.Backtrace(elt))
                if hasattr(what, 'default'):
                    v[what.aname] = what.default

        if not self.hasextras:
            extras = [c_elt for c_elt in c if c_elt]
            if len(extras):
                raise EvaluateException('Extra sub-elements (' + \
                        ','.join([c.nodeName for c in extras]) + ') found',
                        ps.Backtrace(elt))

        if not self.pyclass: return v

        try:
            pyobj = self.pyclass(self.aname)
        except Exception, e:
            raise TypeError("Constructing %s(%s): %s" % (self.pname, self.pyclass.__name__, str(e)))
        for key in v.keys():
            setattr(pyobj, key, v[key])
        return pyobj

    def serialize(self, sw, pyobj, inline=None, name=None, attrtext='', **kw):
        if inline or self.inline:
            self.cb(sw, pyobj, name=name, **kw)
        else:
            objid = _get_object_id(pyobj)
            n = name or self.oname or ('E' + objid)
            print >>sw, '<%s%s href="#%s"/>' % (n, attrtext, objid)
            sw.AddCallback(self.cb, pyobj)

    def cb(self, sw, pyobj, name=None, **kw):
        if not self.mutable and sw.Known(pyobj): return
        objid = _get_object_id(pyobj)
        n = name or self.oname or ('E' + objid)
        if self.inline:
            print >>sw, '<%s>' % n
        else:
            if kw.get('typed', self.typed):
                attrtext = ' xmlns="%s" xsi:type="%s" ' % (self.type[0], self.type[1])
            else:
                attrtext = ''
            print >>sw, '<%s %sid="%s">' % (n, attrtext, objid)
        if self.pyclass:
            d = pyobj.__dict__
        else:
            d = pyobj
            if TypeCode.typechecks and type(d) != types.DictType:
                raise TypeError("Classless struct didn't get dictionary")
        for what in self.ofwhat:
            v = d.get(what.aname)
            if v is None:
                v = d.get(what.aname.lower())
            if what.optional and v is None: continue
            try:
                if what.repeatable and type(v) in _seqtypes:
                    for v2 in v: what.serialize(sw, v2)
                else:
                    what.serialize(sw, v)
            except Exception, e:
                raise Exception('Serializing %s.%s, %s %s' %
                        (n, what.aname or '?', e.__class__.__name__, str(e)))

        # ignore the xmlns if it was explicitly stated
        i = n.find('xmlns')
        if i > 0:
            print >>sw, '</%s>' % n[:i - 1]
        else:
            print >>sw, '</%s>' % n


class Choice(TypeCode):
    '''A union, also known as an XSD choice; pick one.  (Get it?)
    '''

    def __init__(self, ofwhat, pname=None, **kw):
        '''choices -- list of typecodes; exactly one must be match
        '''
        TypeCode.__init__(self, pname, **kw)
        t = type(ofwhat)
        if t not in _seqtypes:
            raise TypeError(
                'Struct ofwhat must be list or sequence, not ' + str(t))
        self.ofwhat = tuple(ofwhat)
        if len(self.ofwhat) == 0: raise TypeError('Empty choice')
        _check_typecode_list(self.ofwhat, 'Choice')

    def parse(self, elt, ps):
        for o in self.ofwhat:
            if o.name_match(elt): return (o.pname, o.parse(elt, ps))
        raise EvaluateException('Element not found in choice list',
                ps.Backtrace(elt))

    def serialize(self, sw, pyobj, **kw):
        name, value = pyobj
        for o in self.ofwhat:
            if name == o.pname:
                o.serialize(sw, value, **kw)
                return
        raise TypeError('Name "' + name + '" not found in choice.')

    def name_match(self, elt):
        for o in self.ofwhat:
            if o.name_match(elt): return 1
        return 0

class Array(TypeCode):
    '''An array.
        mutable -- object could change between multiple serializations
        undeclared -- do not serialize/parse arrayType attribute.
    '''

    def __init__(self, atype, ofwhat, pname=None, dimensions=1, fill=None,
    sparse=0, mutable=0, size=None, nooffset=0, undeclared=0,
    childnames=None, **kw):
        TypeCode.__init__(self, pname, **kw)
        self.dimensions = dimensions
        self.atype = atype
        if not undeclared and self.atype[-1] != ']': self.atype = self.atype + '[]'
        # Support multiple dimensions
        if self.dimensions != 1:
            raise TypeError("Only single-dimensioned arrays supported")
        self.fill = fill
        self.sparse = sparse
        if self.sparse: ofwhat.optional = 1
        self.mutable = mutable
        self.size = size
        self.nooffset = nooffset
        self.undeclared = undeclared
        self.childnames = childnames
        if self.size:
            t = type(self.size)
            if t in _inttypes:
                self.size = (self.size,)
            elif t in _seqtypes:
                self.size = tuple(self.size)
            elif TypeCode.typechecks:
                raise TypeError('Size must be integer or list, not ' + str(t))

        if TypeCode.typechecks:
            if not self.undeclared and type(atype) not in _stringtypes:
                raise TypeError("Array type must be a string or None.")
            t = type(ofwhat)
            if t != types.InstanceType:
                raise TypeError(
                    'Array ofwhat must be an instance, not ' + str(t))
            if not isinstance(ofwhat, TypeCode):
                raise TypeError(
                    'Array ofwhat outside the TypeCode hierarchy, ' +
                    str(ofwhat.__class__))
            if self.size:
                if len(self.size) != self.dimensions:
                    raise TypeError('Array dimension/size mismatch')
                for s in self.size:
                    if type(s) not in _inttypes:
                        raise TypeError('Array size "' + str(s) +
                                '" is not an integer.')
        self.ofwhat = ofwhat

    def parse_offset(self, elt, ps):
        o = _find_arrayoffset(elt)
        if not o: return 0
        if not _offset_pat.match(o):
            raise EvaluateException('Bad offset "' + o + '"',
                        ps.Backtrace(elt))
        return int(o[1:-1])

    def parse_position(self, elt, ps):
        o = _find_arrayposition(elt)
        if not o: return None
        if o.find(','):
            raise EvaluateException('Sorry, no multi-dimensional arrays',
                    ps.Backtrace(elt))
        if not _position_pat.match(o):
            raise EvaluateException('Bad array position "' + o + '"',
                    ps.Backtrace(elt))
        return int(o[-1:1])

    def parse(self, elt, ps):
        href = _find_href(elt)
        if href:
            if _children(elt):
                raise EvaluateException('Array has content and HREF',
                        ps.Backtrace(elt))
            elt = ps.FindLocalHREF(href, elt)
        if self.nilled(elt, ps): return None
        if not _find_arraytype(elt) and not self.undeclared:
            raise EvaluateException('Array expected', ps.Backtrace(elt))
        t = _find_type(elt)
        if t:
            pass # XXX should check the type, but parsing that is hairy.
        offset = self.parse_offset(elt, ps)
        v, vlen = [], 0
        if offset and not self.sparse:
            while vlen < offset:
                vlen += 1
                v.append(self.fill)
        for c in _child_elements(elt):
            item = self.ofwhat.parse(c, ps)
            position = self.parse_position(c, ps) or offset
            if self.sparse:
                v.append((position, item))
            else:
                while offset < position:
                    offset += 1
                    v.append(self.fill)
                v.append(item)
            offset += 1
        return v

    def serialize(self, sw, pyobj, name=None, attrtext='', childnames=None,
    **kw):
        if not self.mutable and sw.Known(pyobj): return
        objid = _get_object_id(pyobj)
        n = name or self.oname or ('E' + objid)
        offsettext = ''
        if not self.sparse and not self.nooffset:
            offset, end = 0, len(pyobj)
            while offset < end and pyobj[offset] == self.fill:
                offset += 1
            if offset: offsettext = ' SOAP-ENC:offset="[%d]"' % offset
        if self.unique:
            idtext = ''
        else:
            idtext = ' id="%s"' % objid
        if self.undeclared:
            print >>sw, '<%s%s%s%s>' % \
                    (n, attrtext, offsettext, idtext)
        else:
            print >>sw, '<%s%s%s%s SOAP-ENC:arrayType="%s">' % \
                    (n, attrtext, offsettext, idtext, self.atype)
        d = {}
        kn = childnames or self.childnames
        if kn:
            d['name'] = kn
        elif not self.ofwhat.aname:
            d['name'] = 'element'
        if not self.sparse:
            for e in pyobj[offset:]: self.ofwhat.serialize(sw, e, **d)
        else:
            position = 0
            for pos, v in pyobj:
                if pos != position:
                    d['attrtext'] = ' SOAP-ENC:position="[%d]"' % pos
                    position = pos
                else:
                    d['attrtext'] = ''
                self.ofwhat.serialize(sw, v, **d)
                position += 1

        i = n.find('xmlns')
        if i > 0:
            print >>sw, '</%s>' % n[:i - 1]
        else:
            print >>sw, '</%s>' % n


if __name__ == '__main__': print _copyright
