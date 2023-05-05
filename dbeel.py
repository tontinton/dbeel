import socket
import msgpack
import struct
import contextlib

@contextlib.contextmanager
def client():
    with contextlib.closing(socket.socket()) as s:
        s.connect(('127.0.0.1', 10000))
        yield s

def _db_send(s, p):
    r = msgpack.dumps(p)
    s.send(struct.pack("<H", len(r)))
    s.send(r)
    buf = b''
    while packet := s.recv(1024 * 64):
          buf += packet
    return msgpack.loads(buf)

def _db_request(**kwargs):
    with client() as s:
         return _db_send(s, kwargs)

class DB:
    @staticmethod
    def create(c):
        return _db_request(type="create_collection", name=c)

    @staticmethod
    def drop(c):
        return _db_request(type="drop_collection", name=c)

    @staticmethod
    def set(c, k, v):
        return _db_request(type="set", collection=c, key=k, value=v)

    @staticmethod
    def delete(c, k):
        return _db_request(type="delete", collection=c, key=k)

    @staticmethod
    def get(c, k):
        return _db_request(type="get", collection=c, key=k)

