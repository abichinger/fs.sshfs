
import contextlib
from queue import Queue
from threading import local
from paramiko import SSHClient, SFTPClient

_local = local()

class ConnectionPool(object):

    def __init__(self, open_func, max_connections=4, timeout=None):
        """
        A generic connection pool.

        :param open_func: Function that opens a new connection.
        :param max_connections: Maximum number of open connections
        :param timeout: Maximum time to wait for a connection
        """

        self._open_func = open_func
        self.timeout = timeout

        self._q = Queue(maxsize=max_connections)

        while not self._q.full():
            self._q.put(None)

    @contextlib.contextmanager
    def connection(self):
        '''
        Returns a ContextManager with an open connection.
        This function returns the same connection when called recursively.
        '''
        if getattr(_local, "conn", None) is None:
            try:
                _local.conn = self.acquire()
                yield _local.conn
            finally:
                self.release(_local.conn)
                _local.conn = None
        else:
            yield _local.conn

    def acquire(self):
        conn = self._q.get(timeout=self.timeout)
        return self._open_func(conn)

    def release(self, conn):
        self._q.put(conn, block=False)

class SFTPClientPool(ConnectionPool):

    def __init__(self, client:SSHClient, max_connections:int=4, timeout=None):
        """
        A pool of SFTPClient sessions

        :param max_connections: Maximum number of open sessions
        :param timeout: Maximum time to wait for a session
        """

        def open_sftp(conn: SFTPClient | None):
            if conn is None or conn.get_channel().closed:
                return client.open_sftp()
            return conn
        
        super().__init__(open_sftp, max_connections, timeout)

    def acquire(self) -> SFTPClient:
        """
        Acquire an SFTPClient.
        If timeout is None this functions blocks until a free session is available.
        Otherwise an Empty exception is raised.
        """
        return super().acquire()

