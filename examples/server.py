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

    def doSet(self, req, data):
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        self.d[req.key] = (exp, flags, 0, data)

    @_requireKey
    def doDelete(self, req, data):
        del self.d[req.key]

storage = DictStorage()

def ex(*a):
    print "Shutting down a client."
    raise binary.MemcachedDisconnect()
    # this also works, but apparently confuses people.
    # sys.exit(0)

class ExampleBinaryServer(binary.BinaryServerProtocol):

    handlers = {
        constants.CMD_GET: storage.doGet,
        constants.CMD_SET: storage.doSet,
        constants.CMD_DELETE: storage.doDelete,
        constants.CMD_QUIT: ex
        }

factory = protocol.Factory()
factory.protocol = ExampleBinaryServer

reactor.listenTCP(11212, factory)
reactor.run()
