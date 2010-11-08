#!/usr/bin/env python

import os
import sys
import struct

sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

from memcache import binary, constants

def _requireKey(f):
    # Helper for validating keys in dict storage.
    def g(self, req, data):
        if req.key in self.d:
            return f(self, req, data)
        else:
            raise binary.MemcachedNotFound()
    return g

class DictStorage(object):

    def __init__(self):
        self.d = {}

    @_requireKey
    def doGet(self, req, data):
        exp, flags, cas, val = self.d[req.key]
        res = binary.GetResponse(req, flags, cas, data=val)
        # If the magic 'slow' is requested, slow down.
        if req.key == 'slow':
            rv = defer.Deferred()
            reactor.callLater(5, rv.callback, res)
            return rv
        else:
            return res

    def doGetQ(self, req, data):
        try:
            return self.doGet(req, data)
        except binary.MemcachedNotFound:
            return binary.EmptyResponse()

    def doSet(self, req, data):
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        self.d[req.key] = (exp, flags, 0, data)

    def doAdd(self, req, data):
        if req.key in self.d:
            raise binary.MemcachedExists()
        else:
            flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
            self.d[req.key] = (exp, flags, 0, data)

    @_requireKey
    def doAppend(self, req, newdata):
        exp, flags, cas, olddata = self.d[req.key]
        self.d[req.key] = (exp, flags, 0, olddata + newdata)

    @_requireKey
    def doPrepend(self, req, newdata):
        exp, flags, cas, olddata = self.d[req.key]
        self.d[req.key] = (exp, flags, 0, newdata + olddata)

    @_requireKey
    def doDelete(self, req, data):
        del self.d[req.key]

    def doFlush(self, req, data):
        self.d = {}

    def doStats(self, req, data):
        r = binary.MultiResponse()
        r.add(binary.Response(req, key='version', data='blah'))
        r.add(binary.Response(req, key='', data=''))
        return r

storage = DictStorage()

def ex(*a):
    print "Shutting down a client."
    raise binary.MemcachedDisconnect()
    # this also works, but apparently confuses people.
    # sys.exit(0)

def doNoop(req, data):
    return binary.Response(req)

class ExampleBinaryServer(binary.BinaryServerProtocol):

    handlers = {
        constants.CMD_GET: storage.doGet,
        constants.CMD_GETQ: storage.doGetQ,
        constants.CMD_SET: storage.doSet,
        constants.CMD_ADD: storage.doAdd,
        constants.CMD_APPEND: storage.doAppend,
        constants.CMD_PREPEND: storage.doPrepend,
        constants.CMD_DELETE: storage.doDelete,
        constants.CMD_STAT: storage.doStats,
        constants.CMD_FLUSH: storage.doFlush,
        constants.CMD_NOOP: doNoop,
        constants.CMD_QUIT: ex
        }

factory = protocol.Factory()
factory.protocol = ExampleBinaryServer

reactor.listenTCP(11212, factory)
reactor.run()
