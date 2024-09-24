
import contextlib
from queue import Queue
from threading import local
from paramiko import SSHClient, SFTPClient

_local = local()

class ConnectionPool(object):
    """A generic connection pool.

    Arguments:
        open_func (Callable): Function that opens a new connection.
        max_connections (int): Maximum number of open connections
        timeout (float): Maximum time to wait for a connection
    """

    def __init__(self, open_func, max_connections=4, timeout=None):
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
        if not hasattr(_local, "conn"):
            try:
                _local.conn = self.acquire()
                yield _local.conn
            finally:
                if not hasattr(_local, "conn"):
                    # nothing has been acquired
                    return
                self.release(_local.conn)
                del _local.conn
        else:
            yield _local.conn

    def acquire(self):
        conn = self._q.get(timeout=self.timeout)
        try:
            return self._open_func(conn)
        except Exception:
            self.release(conn)
            raise

    def release(self, conn):
        self._q.put(conn, block=False)

class SFTPClientPool(ConnectionPool):
    """A pool of SFTPClient sessions

    Arguments:
        client (SSHClient): ssh client
        max_connections (int): Maximum number of open sessions
        timeout (float): Maximum time to wait for a session
    """

    def __init__(self, client, max_connections=4, timeout=None):
        # type: (SSHClient, int, float | None) -> SFTPClientPool
        def open_sftp(conn):
            # type: (SFTPClient | None) -> SFTPClient
            if conn is None or conn.get_channel().closed:
                return client.open_sftp()
            return conn
        
        super().__init__(open_sftp, max_connections, timeout)

    def acquire(self) -> SFTPClient:
        # type: () -> SFTPClient
        """
        Acquire an SFTPClient.
        If timeout is None this functions blocks until a free session is available.
        Otherwise an Empty exception is raised.
        """
        return super().acquire()

