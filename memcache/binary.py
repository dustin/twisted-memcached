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

class BinaryServerProtocol(stateful.StatefulProtocol):
    """Protocol handling for acting like a memcached server."""

    handlers = {}

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

    def _completed(self, data):
        try:
            request = self.currentReq
            self.handlers.get(request.opcode,
                              self.unknownCommand)(request, data, self)
        except MemcachedError, e:
            self._respondError(self.currentReq, e.code, e.msg)
        except:
            log.err()

        return self.getInitialState()

    def _respondError(self, req, errval, msg=''):
        header = struct.pack(RES_PKT_FMT, RES_MAGIC_BYTE, req.opcode,
                             0, 0, 0, errval, len(msg),
                             req.opaque, 0)
        self.transport.writeSequence([header, msg])

    def sendAck(self, req, cas):
        header = struct.pack(RES_PKT_FMT, RES_MAGIC_BYTE, req.opcode,
                             0, 0, 0, 0, 0,
                             req.opaque, cas)
        self.transport.write(header)

    def sendValue(self, req, flags, cas, value):
        header = struct.pack(RES_PKT_FMT, RES_MAGIC_BYTE, req.opcode,
                             GET_RES_SIZE, 0, 0, 0, len(value) + GET_RES_SIZE,
                             req.opaque, cas)
        extra = struct.pack(GET_RES_FMT, flags)
        self.transport.writeSequence([header, extra, value])

    def unknownCommand(self, request, data, prot):
        raise MemcachedUnknownCommand()
