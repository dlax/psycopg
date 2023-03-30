"""
psycopg_c.pq.PGcancel object implementation.
"""

# Copyright (C) 2020 The Psycopg Team


cdef class PGcancelConn:
    def __cinit__(self):
        self.pgcancelconn_ptr = NULL

    @staticmethod
    cdef PGcancelConn _from_ptr(libpq.PGcancelConn *ptr):
        cdef PGcancelConn rv = PGcancelConn.__new__(PGcancelConn)
        rv.pgcancelconn_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.finish()

    @property
    def status(self) -> int:
        return libpq.PQcancelStatus(self.pgcancelconn_ptr)

    @property
    def socket(self) -> int:
        rv = libpq.PQcancelSocket(self.pgcancelconn_ptr)
        if rv == -1:
            raise e.OperationalError("cancel connection not opened")
        return rv

    def poll(self) -> int:
        return libpq.PQcancelPoll(self.pgcancelconn_ptr)

    @property
    def error_message(self) -> str:
        return libpq.PQcancelErrorMessage(self.pgcancelconn_ptr).decode()

    def send(self) -> None:
        """Requests that the server abandons processing of the current command.

        See :pq:`PQcancelSend` for details.
        """
        if not libpq.PQcancelSend(self.pgcancelconn_ptr):
            raise e.OperationalError(
                f"couldn't send cancellation: {self.error_message}"
            )

    def finish(self) -> None:
        if self.pgcancelconn_ptr is not NULL:
            libpq.PQcancelFinish(self.pgcancelconn_ptr)
            self.pgcancelconn_ptr = NULL

    def reset(self) -> None:
        libpq.PQcancelReset(self.pgcancelconn_ptr)


cdef class PGcancel:
    def __cinit__(self):
        self.pgcancel_ptr = NULL

    @staticmethod
    cdef PGcancel _from_ptr(libpq.PGcancel *ptr):
        cdef PGcancel rv = PGcancel.__new__(PGcancel)
        rv.pgcancel_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.free()

    def free(self) -> None:
        if self.pgcancel_ptr is not NULL:
            libpq.PQfreeCancel(self.pgcancel_ptr)
            self.pgcancel_ptr = NULL

    def cancel(self) -> None:
        cdef char buf[256]
        cdef int res = libpq.PQcancel(self.pgcancel_ptr, buf, sizeof(buf))
        if not res:
            raise e.OperationalError(
                f"cancel failed: {buf.decode('utf8', 'ignore')}"
            )
