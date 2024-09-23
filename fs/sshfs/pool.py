
import contextlib
from queue import Queue
from threading import RLock, local
from paramiko import SSHClient, SFTPClient

_local = local()

class ConnectionPool(object):

    def __init__(self, open_func, close_func, healty_func, max_connections=4, timeout=None):
        """
        A generic connection pool.

        :param open_func: Function that opens a new connection.
        :param close_func: Function that closes a given connection.
        :param healty_func: Function to check if an active connection is still healty.
        :param max_connections: Maximum number of open connections
        :param timeout: Maximum time to wait for a connection
        """

        self._open_func = open_func
        self._close_func = close_func
        self._healthy = healty_func
        self.timeout = timeout
        self._lock = RLock()
        # self.unused_timeout = unused_timeout :param unused_timeout: Time after which an unused connection should be closed

        self.active = 0
        self._q = Queue(maxsize=max_connections)

    @contextlib.contextmanager
    def connection(self):
        if getattr(_local, "conn", None) is None:
            try:
                _local.conn = self.acquire()
                yield _local.conn
            finally:
                self.release(_local.conn)
                _local.conn = None
        else:
            yield _local.conn


    def _open(self):
        self.active += 1
        return self._open_func()

    def _close(self, conn):
        self.active -= 1
        self._close_func(conn)

    def acquire(self):
        while True:
            with self._lock:
                if self._q.empty() and self.active < self._q.maxsize:
                    return self._open()
            
            conn = self._q.get(timeout=self.timeout)
            if not self._healthy(conn):
                self._close()
                continue
                
            return conn

    def release(self, conn):
        self._q.put(conn, block=False)

class SFTPClientPool(ConnectionPool):

    def __init__(self, client:SSHClient, max_connections:int=4, timeout=None):
        
        def open_sftp():
            return client.open_sftp()
        
        def close_sftp(sftp:SFTPClient):
            sftp.close()

        def is_healthy(sftp:SFTPClient) -> bool:
            return not sftp.get_channel().closed
        
        super().__init__(open_sftp, close_sftp, is_healthy, max_connections, timeout)

    def acquire(self) -> SFTPClient:
        conn: SFTPClient = super().acquire()
        channel = conn.get_channel()
        transport = channel.get_transport()

        if channel.closed:
            raise Exception("Channel is closed")
        
        if not transport.is_active():
            raise Exception("Transport is closed")

        return conn

