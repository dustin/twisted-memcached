#!/usr/bin/env python

import os
import sys
import struct

sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

from memcache import binary, constants

class DictStorage(object):

    def __init__(self):
        self.d = {}

    def doGet(self, req, data):
        try:
            exp, flags, cas, val = self.d[req.key]
            return binary.GetResponse(req, flags, cas, data=val)
        except KeyError:
            raise binary.MemcachedNotFound()

    def doSet(self, req, data):
        flags, exp = struct.unpack(constants.SET_PKT_FMT, req.extra)
        self.d[req.key] = (exp, flags, 0, data)
        return binary.Response(req)

storage = DictStorage()

class ExampleBinaryServer(binary.BinaryServerProtocol):

    handlers = {
        constants.CMD_GET: storage.doGet,
        constants.CMD_SET: storage.doSet
        }

factory = protocol.Factory()
factory.protocol = ExampleBinaryServer

reactor.listenTCP(11212, factory)
reactor.run()
