import concurrent.futures
import os
import time
import socket
import logging
import threading
from contextlib import contextmanager

import pytest

import psycopg
from psycopg import conninfo

logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    for item in items:
        # TODO: there is a race condition on macOS and Windows in the CI:
        # listen returns before really listening and tests based on 'deaf_listen'
        # fail 50% of the times. Just add the 'proxy' mark on these tests
        # because they are already skipped in the CI.
        if "proxy" in item.fixturenames:
            item.add_marker(pytest.mark.proxy)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "proxy: the test uses the proxy (the marker is set automatically"
        " on tests using the fixture)",
    )


@pytest.fixture
def proxy(dsn):
    """Return a proxy to the --test-dsn database"""
    p = Proxy(dsn)
    yield p
    p.stop()


def forward(src: socket.socket, dst: socket.socket, name: str) -> None:
    while True:
        try:
            data = src.recv(4096)
        except TimeoutError:
            continue
        except OSError as e:
            logger.debug("%s: errored (%s)", name, e)
            return
        if not data:  # connection closed
            logger.debug("%s: finished (0 bytes received)", name)
            return
        try:
            if not dst.send(data):
                logger.debug("%s: finished (0 bytes sent)", name)
                return
        except OSError as e:
            logger.debug("%s: errored (%s)", name, e)
            return
        logger.debug("%s: handled %d bytes", name, len(data))


class Proxy:
    """
    Proxy a Postgres service for testing purpose.

    Allow to lose connectivity and restart it using stop/start.
    """

    timeout: float = 0.001
    # The timeout for created sockets and polling interval.

    def __init__(self, server_dsn: str):
        cdict = conninfo.conninfo_to_dict(server_dsn)

        # Get server params
        host = cdict.get("host") or os.environ.get("PGHOST", "")
        assert isinstance(host, str)
        self.server_host = host if host and not host.startswith("/") else "127.0.0.1"
        server_port = cdict.get("port") or os.environ.get("PGPORT", "5432")
        assert server_port is not None
        self.server_port = int(server_port)

        # Get client params
        self.client_host = "127.0.0.1"
        self.client_port = self._get_random_port()

        # Make a connection string to the proxy
        cdict["host"] = self.client_host
        cdict["port"] = self.client_port
        cdict["sslmode"] = "disable"  # not supported by the proxy
        self.client_dsn = conninfo.make_conninfo("", **cdict)

        self._state: (
            tuple[socket.socket, concurrent.futures.Executor, threading.Event] | None
        ) = None

    def start(self, n: int = 1) -> None:
        """Start the proxy server with 'n' worker threads.

        Handle at most 'n' client connections by forwarding requests to a
        remote connection to Postgres.
        """
        if self._state is not None:
            logger.warning("proxy already started")
            return

        assert n >= 1

        sock = socket.create_server((self.client_host, self.client_port))
        sock.settimeout(self.timeout)

        terminate = threading.Event()
        executor = concurrent.futures.ThreadPoolExecutor(n)
        for _ in range(n):
            executor.submit(self._handle_conn, sock, terminate)
        logger.info("proxy started")
        self._state = sock, executor, terminate

        self._wait_listen()
        self._check_connect()

    def stop(self) -> None:
        if self._state is None:
            return
        sock, executor, terminate = self._state
        sock.close()  # Stop waiting for new connections.
        terminate.set()  # Terminate opened connections.
        logger.debug("waiting for proxy to terminate...")
        executor.shutdown()
        logger.info("proxy terminated")
        self._state = None

    @contextmanager
    def deaf_listen(self):
        """Open the proxy port to listen, but without accepting a connection.

        A connection attempt on the proxy `client_host` and `client_port` will
        block. Useful to test connection timeouts.
        """
        if self._state:
            raise Exception("the proxy is already listening")

        with socket.socket(socket.AF_INET) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.client_host, self.client_port))
            s.listen(0)
            yield s

    def _handle_conn(
        self,
        proxy_sock: socket.socket,
        terminate: threading.Event,
    ) -> None:
        """Handle new client connections to the proxy (one at a time).

        For each connection, open a respective connection to Postgres and
        forward client requests in both directions.

        Closing 'proxy_sock' terminates the task.
        """
        while True:
            try:
                clientsocket, addr = proxy_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                logger.info("proxy closing, terminating client connection handler")
                break
            logger.info("connection received from %s", addr)
            clientsocket.settimeout(self.timeout)
            with socket.create_connection(
                (self.server_host, self.server_port), timeout=self.timeout
            ) as remote_sock, clientsocket:
                incoming_t = threading.Thread(
                    target=forward, args=(clientsocket, remote_sock, "incoming")
                )
                outgoing_t = threading.Thread(
                    target=forward, args=(remote_sock, clientsocket, "outgoing")
                )
                incoming_t.start()
                outgoing_t.start()
                while incoming_t.is_alive():
                    if terminate.wait(self.timeout):
                        clientsocket.close()
                        incoming_t.join()
                    else:
                        incoming_t.join(self.timeout)
            outgoing_t.join()
            logger.info("finished handling client connection from %s", addr)

    def _check_connect(self):
        # verify that the proxy works
        try:
            with psycopg.connect(self.client_dsn):
                pass
        except Exception as e:
            pytest.fail(f"failed to create a working proxy: {e}")

    @classmethod
    def _get_random_port(cls):
        with socket.socket() as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def _wait_listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for i in range(20):
                if 0 == sock.connect_ex((self.client_host, self.client_port)):
                    break
                time.sleep(0.1)
            else:
                raise ValueError("the proxy didn't start listening in time")

        logger.info("proxy listening")
