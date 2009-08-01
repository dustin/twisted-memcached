import struct

from twisted.trial import unittest

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
