#!/usr/bin/env python3
"""
AkkaraDB TCP binary protocol test client.

Wire format (all integers little-endian, packed):
  Request:  [ApiRequestHeader: 16B] [key: key_len B] [val: val_len B] [crc32c: 4B]
  Response: [ApiResponseHeader: 13B] [val: val_len B] [crc32c: 4B]

ApiRequestHeader (16B):
  magic[4]      = b'AKRQ'
  version[1]    = 1
  opcode[1]     = 0x01=Get 0x02=Put 0x03=Remove 0x04=GetAt
  request_id[4]
  key_len[2]
  val_len[4]

ApiResponseHeader (13B):
  magic[4]      = b'AKRS'
  status[1]     = 0x00=Ok 0x01=NotFound 0xFF=Error
  request_id[4]
  val_len[4]

Usage:
  python tcp_client_test.py [--host HOST] [--port PORT]
"""

import socket
import struct
import sys

# ── CRC32C (Castagnoli, polynomial 0x82F63B78) ────────────────────────────────
# Self-contained, no external dependencies.

_CRC32C_TABLE: list[int] = []


def _build_table() -> None:
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if (crc & 1) else crc >> 1
        _CRC32C_TABLE.append(crc)


_build_table()


def crc32c(data: bytes, seed: int = 0) -> int:
    crc = seed ^ 0xFFFFFFFF
    for b in data:
        crc = (crc >> 8) ^ _CRC32C_TABLE[(crc ^ b) & 0xFF]
    return crc ^ 0xFFFFFFFF


# ── Protocol constants ────────────────────────────────────────────────────────

REQ_MAGIC = b'AKRQ'
RESP_MAGIC = b'AKRS'
VERSION = 1

OP_GET = 0x01
OP_PUT = 0x02
OP_REMOVE = 0x03
OP_GET_AT = 0x04

STATUS_OK = 0x00
STATUS_NOT_FOUND = 0x01
STATUS_ERROR = 0xFF

REQ_HDR_FMT = '<4sBBIHI'  # magic(4) version(B) opcode(B) req_id(I) key_len(H) val_len(I)
RESP_HDR_FMT = '<4sBI I'  # magic(4) status(B) req_id(I) val_len(I)  — note: struct needs 1 byte gap
# ApiResponseHeader is 13B packed (no padding):
#   magic[4] status[1] request_id[4] val_len[4]
RESP_HDR_SIZE = 13


# ── Low-level I/O ─────────────────────────────────────────────────────────────

def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('Server closed connection')
        buf += chunk
    return buf


# ── Frame builder / parser ────────────────────────────────────────────────────

_req_counter = 0


def send_request(sock: socket.socket, opcode: int,
                 key: bytes, val: bytes = b'') -> int:
    global _req_counter
    _req_counter += 1
    req_id = _req_counter

    # Header: magic(4) version(1) opcode(1) req_id(4) key_len(2) val_len(4) = 16B
    hdr = struct.pack('<4sBBIHI',
                      REQ_MAGIC, VERSION, opcode,
                      req_id, len(key), len(val))
    assert len(hdr) == 16, f'header size {len(hdr)}'

    crc = crc32c(val, crc32c(key))  # CRC over key+val
    payload = hdr + key + val + struct.pack('<I', crc)
    sock.sendall(payload)
    return req_id


def recv_response(sock: socket.socket) -> tuple[int, int, bytes]:
    """Returns (status, request_id, value_bytes)."""
    # ApiResponseHeader is 13B packed: magic(4) status(1) req_id(4) val_len(4)
    raw = recv_exact(sock, RESP_HDR_SIZE)
    magic = raw[0:4]
    status = raw[4]
    req_id = struct.unpack_from('<I', raw, 5)[0]
    val_len = struct.unpack_from('<I', raw, 9)[0]

    if magic != RESP_MAGIC:
        raise ValueError(f'Bad response magic: {magic!r}')

    val = recv_exact(sock, val_len) if val_len else b''
    crc_recv = struct.unpack('<I', recv_exact(sock, 4))[0]

    crc_calc = crc32c(val)
    if crc_recv != crc_calc:
        raise ValueError(f'CRC32C mismatch: got {crc_recv:#010x}, expected {crc_calc:#010x}')

    return status, req_id, val


# ── High-level ops ────────────────────────────────────────────────────────────

def op_put(sock: socket.socket, key: str | bytes, val: str | bytes) -> bool:
    k = key.encode() if isinstance(key, str) else key
    v = val.encode() if isinstance(val, str) else val
    rid = send_request(sock, OP_PUT, k, v)
    status, _, _ = recv_response(sock)
    return status == STATUS_OK


def op_get(sock: socket.socket, key: str | bytes) -> bytes | None:
    k = key.encode() if isinstance(key, str) else key
    send_request(sock, OP_GET, k)
    status, _, val = recv_response(sock)
    if status == STATUS_OK:
        return val
    return None  # NotFound or Error


def op_remove(sock: socket.socket, key: str | bytes) -> bool:
    k = key.encode() if isinstance(key, str) else key
    rid = send_request(sock, OP_REMOVE, k)
    status, _, _ = recv_response(sock)
    return status == STATUS_OK


# ── Test suite ────────────────────────────────────────────────────────────────

def run_tests(sock: socket.socket) -> None:
    passed = 0
    failed = 0

    def check(label: str, actual, expected) -> None:
        nonlocal passed, failed
        ok = actual == expected
        mark = '✓' if ok else '✗'
        print(f'  [{mark}] {label}')
        if not ok:
            print(f'        expected: {expected!r}')
            print(f'        actual:   {actual!r}')
            failed += 1
        else:
            passed += 1

    print('\n── Basic PUT / GET ──────────────────────────────────')
    op_put(sock, 'hello', 'world')
    check('GET hello → world', op_get(sock, 'hello'), b'world')

    op_put(sock, 'num', '42')
    check('GET num → 42', op_get(sock, 'num'), b'42')

    print('\n── Overwrite ────────────────────────────────────────')
    op_put(sock, 'hello', 'updated!')
    check('GET hello → updated!', op_get(sock, 'hello'), b'updated!')

    print('\n── Not Found ────────────────────────────────────────')
    check('GET nonexistent → None', op_get(sock, 'does_not_exist'), None)

    print('\n── Binary key / value ───────────────────────────────')
    bin_key = b'\x00\x01\x02\xff'
    bin_val = bytes(range(256))
    op_put(sock, bin_key, bin_val)
    check('GET binary key → 256-byte value', op_get(sock, bin_key), bin_val)

    print('\n── Remove ───────────────────────────────────────────')
    op_remove(sock, 'hello')
    check('GET hello after remove → None', op_get(sock, 'hello'), None)

    print('\n── Pipelining (3 PUTs then 3 GETs) ─────────────────')
    # Send 3 requests without waiting for responses (pipelining)
    send_request(sock, OP_PUT, b'p1', b'v1')
    send_request(sock, OP_PUT, b'p2', b'v2')
    send_request(sock, OP_PUT, b'p3', b'v3')
    s1, _, _ = recv_response(sock)
    s2, _, _ = recv_response(sock)
    s3, _, _ = recv_response(sock)
    check('Pipeline PUT p1', s1, STATUS_OK)
    check('Pipeline PUT p2', s2, STATUS_OK)
    check('Pipeline PUT p3', s3, STATUS_OK)
    check('GET p1 → v1', op_get(sock, 'p1'), b'v1')
    check('GET p2 → v2', op_get(sock, 'p2'), b'v2')
    check('GET p3 → v3', op_get(sock, 'p3'), b'v3')

    print(f'\n── Result: {passed} passed, {failed} failed ──────────────────')
    if failed:
        sys.exit(1)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    host = '127.0.0.1'
    port = 7071

    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == '--host' and i + 1 < len(args):
            host = args[i + 1]
        elif a == '--port' and i + 1 < len(args):
            port = int(args[i + 1])

    print(f'AkkaraDB TCP client test → {host}:{port}')

    with socket.create_connection((host, port), timeout=5) as sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        print(f'Connected.')
        run_tests(sock)


if __name__ == '__main__':
    main()
