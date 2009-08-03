import sys
import struct

from zope.interface import implements

from twisted.trial import unittest
from twisted.internet import interfaces, defer

from memcache import binary, constants

class RequestTest(unittest.TestCase):

    def test_construction(self):
        keylen = 113
        extralen = 24
        bodylen = 81859
        opaque = 885932158
        cas = 81849958
        pkt = struct.pack(binary.REQ_PKT_FMT, binary.REQ_MAGIC_BYTE,
                          binary.CMD_STAT, keylen, extralen, 0,
                          bodylen, opaque, cas)

        req = binary.Request(pkt)

        self.assertEquals(binary.REQ_MAGIC_BYTE, req.magic)
        self.assertEquals(binary.CMD_STAT, req.opcode)
        self.assertEquals(keylen, req.keylen)
        self.assertEquals(extralen, req.extralen)
        self.assertEquals(0, req.datatype)
        self.assertEquals(bodylen, req.bodylen)
        self.assertEquals(opaque, req.opaque)
        self.assertEquals(cas, req.cas)

class ResponseTest(unittest.TestCase):

    def test_toSequence(self):
        opaque = 885932158
        cas = 82385929
        pkt = struct.pack(binary.REQ_PKT_FMT, binary.REQ_MAGIC_BYTE,
                          binary.CMD_STAT, 0, 0, 0,
                          0, opaque, 0)

        req = binary.Request(pkt)

        key = "some_key"
        data = "some data"
        res = binary.Response(req, cas, key=key, data=data)

        seq = res.toSequence()
        expected = [struct.pack(binary.RES_PKT_FMT, binary.RES_MAGIC_BYTE,
                                res.req.opcode, len(key), 0, 0, 0,
                                len(data), opaque, cas),
                    '',
                    data]

        self.assertEquals(expected, seq)

class GetResponseTest(unittest.TestCase):

    def test_toSequence(self):
        opaque = 885932158
        cas = 82385929
        flags = 82859
        pkt = struct.pack(binary.REQ_PKT_FMT, binary.REQ_MAGIC_BYTE,
                          binary.CMD_STAT, 0, 0, 0,
                          0, opaque, 0)

        req = binary.Request(pkt)

        key = "some_key"
        data = "some data"
        res = binary.GetResponse(req, flags, cas, key=key, data=data)

        seq = res.toSequence()
        esize = struct.calcsize(binary.GET_RES_FMT)
        expected = [struct.pack(binary.RES_PKT_FMT, binary.RES_MAGIC_BYTE,
                                res.req.opcode, len(key), esize, 0, 0,
                                len(data) + esize,
                                opaque, cas),
                    struct.pack(binary.GET_RES_FMT, flags),
                    data]

        self.assertEquals(expected, seq)

class TestTransport(object):

    implements(interfaces.ITransport)

    disconnecting = False

    def __init__(self):
        self.received = []
        self.disconnected = 0

    def write(self, data):
        self.received.append(data)

    def writeSequence(self, data):
        self.received.extend(data)

    def loseConnection(self):
        self.disconnected += 1

    def getPeer(self):
        return None

    def getHost(self):
        return None

class TestServer(object):

    def __init__(self):
        self.responses = []

    def noop(self, req, data):
        for d, r in reversed(self.responses):
            d.callback(r)

    def get(self, req, data):
        return binary.GetResponse(req, 9282, data='response')

    def set(self, req, data):
        raise binary.MemcachedNotStored()

    def quit(self, req, data):
        sys.exit(0)

    def getq(self, req, data):
        """quiet gets just queue up and replay backwards on a noop"""
        d = defer.Deferred()
        data = 'response %d' % len(self.responses)
        self.responses.append((d, binary.GetResponse(req, 8259, key=req.key,
                                                     data=data)))
        return d

testServer = TestServer()

class TestServerProtocol(binary.BinaryServerProtocol):

    handlers = {
        constants.CMD_NOOP: testServer.noop,
        constants.CMD_GET:  testServer.get,
        constants.CMD_GETQ: testServer.getq,
        constants.CMD_SET:  testServer.set,
        constants.CMD_QUIT: testServer.quit
        }

    def __init__(self):
        self.responses = []

    def _respond(self, res):
        self.responses.append(res)
        binary.BinaryServerProtocol._respond(self, res)

class BinaryServerProtocolTest(unittest.TestCase):

    def setUp(self):
        self.prot = TestServerProtocol()
        self.trans = TestTransport()
        self.prot.makeConnection(self.trans)

    def assertResponses(self, responses):
        self.assertEquals(len(self.prot.responses), len(responses))
        for g,e in zip(self.prot.responses, responses):
            for k in e:
                gotval = reduce(getattr, k.split('.'), g)
                self.assertEquals(e[k], gotval, "For opaque=%d" % g.req.opaque)

    def mkReq(self, op, key='', extra='', data='', opaque=0, cas=0):
        keylen = len(key)
        extralen = len(extra)
        bodylen = len(data)
        pkt = struct.pack(binary.REQ_PKT_FMT, binary.REQ_MAGIC_BYTE,
                          op, keylen, extralen, 0,
                          bodylen + extralen, opaque, cas)

        return pkt + extra + key + data

    def test_noop(self):
        self.prot.dataReceived(self.mkReq(constants.CMD_NOOP))
        self.assertResponses([{'req.opcode': constants.CMD_NOOP, 'status': 0}])

    def test_quit(self):
        self.prot.dataReceived(self.mkReq(binary.CMD_QUIT))
        self.assertEquals(1, self.trans.disconnected)

    def test_unhandled(self):
        self.prot.dataReceived(self.mkReq(binary.CMD_STAT))
        self.assertResponses([{'status': constants.ERR_UNKNOWN_CMD}])

    def test_get(self):
        self.prot.dataReceived(self.mkReq(constants.CMD_GET, key='x'))
        self.assertResponses([{'data': 'response'}])

    def test_ordering(self):
        self.prot.dataReceived(self.mkReq(constants.CMD_GETQ, key='x', opaque=1))
        self.prot.dataReceived(self.mkReq(constants.CMD_GETQ, key='y', opaque=2))
        self.prot.dataReceived(self.mkReq(constants.CMD_NOOP))
        self.assertResponses([{'req.opaque': 1, 'req.opcode': constants.CMD_GETQ, 'key': 'x'},
                              {'req.opaque': 2, 'req.opcode': constants.CMD_GETQ, 'key': 'y'},
                              {'req.opcode': constants.CMD_NOOP}])

    def test_set(self):
        extra = struct.pack(binary.SET_PKT_FMT, 8184, 1984)
        self.prot.dataReceived(self.mkReq(binary.CMD_SET, key='x',
                                          extra=extra, data='y'))
        self.assertResponses([{'status': constants.ERR_NOT_STORED}])

    def test_bad_req(self):
        self.prot.dataReceived("x" * constants.MIN_RECV_PACKET)
        self.assertEquals(1, self.trans.disconnected)
