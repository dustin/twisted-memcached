#!/usr/bin/env python
"""

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import struct

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7
CMD_FLUSH = 8
CMD_GETQ = 9
CMD_NOOP = 10
CMD_VERSION = 11
CMD_GETK = 0xc
CMD_GETKQ = 0xd
CMD_APPEND = 0x0e
CMD_PREPEND = 0x0f
CMD_STAT = 0x10
CMD_SETQ = 0x11
CMD_ADDQ = 0x12
CMD_REPLACEQ = 0x13
CMD_DELETEQ = 0x14
CMD_INCRQ = 0x15
CMD_DECRQ = 0x16
CMD_QUITQ = 0x17
CMD_FLUSHQ = 0x18
CMD_APPENDQ = 0x19
CMD_PREPENDQ = 0x1a

# SASL stuff
CMD_SASL_LIST_MECHS = 0x20
CMD_SASL_AUTH = 0x21
CMD_SASL_STEP = 0x22

# Flags, expiration
SET_PKT_FMT=">II"

# flags
GET_RES_FMT=">I"
GET_RES_SIZE = struct.calcsize(GET_RES_FMT)

# How long until the deletion takes effect.
DEL_PKT_FMT=""

# amount, initial value, expiration
INCRDECR_PKT_FMT=">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL=0xffffffff
INCRDECR_RES_FMT=">Q"

# Time bomb
FLUSH_PKT_FMT=">I"

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = 0x80
RES_MAGIC_BYTE = 0x81

# magic, opcode, keylen, extralen, datatype, [reserved], bodylen, opaque, cas
REQ_PKT_FMT=">BBHBBxxIIQ"
# magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
RES_PKT_FMT=">BBHBBHIIQ"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
# The header sizes don't deviate
assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

EXTRA_HDR_FMTS={
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT,
    CMD_INCR: INCRDECR_PKT_FMT,
    CMD_DECR: INCRDECR_PKT_FMT,
    CMD_DELETE: DEL_PKT_FMT,
    CMD_FLUSH: FLUSH_PKT_FMT,
}

EXTRA_HDR_SIZES=dict(
    [(k, struct.calcsize(v)) for (k,v) in EXTRA_HDR_FMTS.items()])

ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
ERR_TOOBIG = 0x3
ERR_INVAL = 0x4
ERR_NOT_STORED = 0x5
ERR_UNKNOWN_CMD = 0x81
