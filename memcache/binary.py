"""Memcached binary protocol implementation."""

import sys
import struct

from collections import deque

from twisted.internet import defer
from twisted.protocols import stateful
from twisted.python import log

from constants import *

__all__ = ['BinaryServerProtocol',
           'MemcachedUnknownCommand',
           'MemcachedNotFound',
           'MemcachedExists']

class Request(object):

    def __init__(self, hdr):
        (self.magic, self.opcode, self.keylen, self.extralen, self.datatype,
         self.bodylen, self.opaque, self.cas) = struct.unpack(REQ_PKT_FMT, hdr)
        self.extra = ''
        self.key = ''

class Response(object):

    def __init__(self, req, cas=0, status=0, key='', extra='', data=''):
        self.req = req
        self.cas = cas
        self.key = key
        self.extra= extra
        self.status = status
        self.data = data

    def toSequence(self):
        """Convert the response to a sequence for writing."""
        res = self
        req = self.req
        hdr = struct.pack(RES_PKT_FMT, RES_MAGIC_BYTE, req.opcode,
                          len(res.key), len(res.extra), 0, res.status,
                          len(res.data) + len(res.extra), req.opaque,
                          res.cas)
        return [hdr, self.extra, self.data]

class GetResponse(Response):

    def __init__(self, req, flags, cas=0, status=0, key='', data=''):
        extra = struct.pack(GET_RES_FMT, flags)
        super(GetResponse, self).__init__(req, cas, status, key, extra, data)

class MemcachedDisconnect(Exception):
    """Raise this exception from your handler to drop the client connection."""

class MemcachedError(Exception):
    pass

class MemcachedUnknownCommand(MemcachedError):
    code = ERR_UNKNOWN_CMD
    msg = "Unknown command"

class MemcachedNotFound(MemcachedError):
    code = ERR_NOT_FOUND
    msg = "Not Found"

class MemcachedExists(MemcachedError):
    code = ERR_EXISTS
    msg = "Exists"

class MemcachedNotStored(MemcachedError):
    code = ERR_NOT_STORED
    msg = "Not stored"

class BinaryServerProtocol(stateful.StatefulProtocol):
    """Protocol handling for acting like a memcached server."""

    handlers = {}

    def makeConnection(self, trans):
        stateful.StatefulProtocol.makeConnection(self, trans)
        self.__todo = deque()
        self.__responses = {}

    def getInitialState(self):
        self.currentReq = None
        return self._headerReceived, MIN_RECV_PACKET

    def _headerReceived(self, header):
        if ord(header[0]) != REQ_MAGIC_BYTE:
            self.transport.loseConnection()
            return

        r = self.currentReq = Request(header)

        if r.extralen:
            return self._got_extra, r.extralen
        else:
            return self._got_key, r.keylen

    def _got_extra(self, data):
        self.currentReq.extra = data

        return self._got_key, self.currentReq.keylen

    def _got_key(self, data):
        self.currentReq.key = data

        return self._completed, (self.currentReq.bodylen
                                 - self.currentReq.extralen
                                 - self.currentReq.keylen)

    def _sendResponses(self):
        while self.__todo and self.__responses.has_key(self.__todo[0]):
            d = self.__todo.popleft()
            res = self.__responses[d]
            del self.__responses[d]
            self._respond(res)
        assert len(self.__responses) <= len(self.__todo)

    def _completed(self, data):
        request = self.currentReq
        d = defer.maybeDeferred(self.handlers.get(request.opcode,
                                                  self.unknownCommand),
                                request, data)

        self.__todo.append(d)

        def _c(res, req):
            if not res:
                res = Response(req)
            self.__responses[d] = res
            self._sendResponses()

        def _e(e, req):
            e.trap(MemcachedError)
            self._respond(Response(req,
                                   status=e.value.code, data=e.value.msg))

        def _exit(e):
            e.trap(SystemExit, MemcachedDisconnect)
            self.transport.loseConnection()

        d.addCallback(_c, self.currentReq)
        d.addErrback(_e, self.currentReq)
        d.addErrback(_exit)
        d.addErrback(log.err)

        return self.getInitialState()

    def _respond(self, res):
        self.transport.writeSequence(res.toSequence())

    def unknownCommand(self, request, data):
        log.msg("Got an unknown request for %s" % hex(request.opcode))
        raise MemcachedUnknownCommand()
