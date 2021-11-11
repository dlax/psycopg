"""
Code concerned with waiting in different contexts (blocking, async, etc).

These functions are designed to consume the generators returned by the
`generators` module function and to return their final value.

"""

# Copyright (C) 2020-2021 The Psycopg Team


import select
import selectors
import socket
from enum import IntEnum
from typing import Optional
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

import anyio

from . import errors as e
from .abc import PQGen, PQGenConn, RV


class Wait(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE
    RW = EVENT_READ | EVENT_WRITE


class Ready(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE


def wait_selector(
    gen: PQGen[RV], fileno: int, timeout: Optional[float] = None
) -> RV:
    """
    Wait for a generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :type timeout: float
    :return: whatever *gen* returns on completion.

    Consume *gen*, scheduling `fileno` for completion when it is reported to
    block. Once ready again send the ready state back to *gen*.
    """
    try:
        s = next(gen)
        sel = DefaultSelector()
        while 1:
            sel.register(fileno, s)
            rlist = None
            while not rlist:
                rlist = sel.select(timeout=timeout)
            sel.unregister(fileno)
            # note: this line should require a cast, but mypy doesn't complain
            ready: Ready = rlist[0][1]
            s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


def wait_conn(gen: PQGenConn[RV], timeout: Optional[float] = None) -> RV:
    """
    Wait for a connection generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C. If zero or None, wait indefinitely.
    :type timeout: float
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    timeout = timeout or None
    try:
        fileno, s = next(gen)
        sel = DefaultSelector()
        while 1:
            sel.register(fileno, s)
            rlist = sel.select(timeout=timeout)
            sel.unregister(fileno)
            if not rlist:
                raise e.OperationalError("timeout expired")
            ready: Ready = rlist[0][1]  # type: ignore[assignment]
            fileno, s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


async def wait_async(gen: PQGen[RV], fileno: int) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but exposing an `async` interface.
    """
    s: Wait
    ready: Ready

    try:
        sock = socket.fromfd(fileno, socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        # TODO: set a meaningful error message
        raise e.OperationalError

    async def readable(ev: anyio.Event) -> None:
        await anyio.wait_socket_readable(sock)
        nonlocal ready
        ready = Ready.R
        ev.set()

    async def writable(ev: anyio.Event) -> None:
        await anyio.wait_socket_writable(sock)
        nonlocal ready
        ready = Ready.W
        ev.set()

    try:
        s = next(gen)
        while 1:
            ev = anyio.Event()
            async with anyio.create_task_group() as tg:
                if s == Wait.R:
                    tg.start_soon(readable, ev)
                if s == Wait.W:
                    tg.start_soon(writable, ev)
                await ev.wait()

            s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv

    finally:
        sock.close()


async def wait_conn_async(
    gen: PQGenConn[RV], timeout: Optional[float] = None
) -> RV:
    """
    Coroutine waiting for a connection generator to complete.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C. If zero or None, wait indefinitely.
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    s: Wait
    ready: Ready

    async def readable(sock: socket.socket, ev: anyio.Event) -> None:
        await anyio.wait_socket_readable(sock)
        nonlocal ready
        ready = Ready.R
        ev.set()

    async def writable(sock: socket.socket, ev: anyio.Event) -> None:
        await anyio.wait_socket_writable(sock)
        nonlocal ready
        ready = Ready.W
        ev.set()

    timeout = timeout or None
    try:
        fileno, s = next(gen)

        while 1:
            try:
                sock = socket.fromfd(
                    fileno, socket.AF_INET, socket.SOCK_STREAM
                )
            except OSError:
                # TODO: set a meaningful error message
                raise e.OperationalError
            with sock:
                ev = anyio.Event()
                async with anyio.create_task_group() as tg:
                    if s == Wait.R:
                        tg.start_soon(readable, sock, ev)
                    elif s == Wait.W:
                        tg.start_soon(writable, sock, ev)
                    else:
                        raise e.InternalError(f"bad poll status: {s}")

                    try:
                        with anyio.fail_after(timeout):
                            await ev.wait()
                    except TimeoutError:
                        raise e.OperationalError("timeout expired")

                fileno, s = gen.send(ready)

    except TimeoutError:
        raise e.OperationalError("timeout expired")

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


def wait_epoll(
    gen: PQGen[RV], fileno: int, timeout: Optional[float] = None
) -> RV:
    """
    Wait for a generator using epoll where supported.

    Parameters are like for `wait()`. If it is detected that the best selector
    strategy is `epoll` then this function will be used instead of `wait`.

    See also: https://linux.die.net/man/2/epoll_ctl
    """
    if timeout is None or timeout < 0:
        timeout = 0
    else:
        timeout = int(timeout * 1000.0)

    try:
        s = next(gen)
        epoll = select.epoll()
        evmask = poll_evmasks[s]
        epoll.register(fileno, evmask)
        while 1:
            fileevs = None
            while not fileevs:
                fileevs = epoll.poll(timeout)
            ev = fileevs[0][1]
            if ev & ~select.EPOLLOUT:
                s = Ready.R
            else:
                s = Ready.W
            s = gen.send(s)
            evmask = poll_evmasks[s]
            epoll.modify(fileno, evmask)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


if selectors.DefaultSelector is getattr(selectors, "EpollSelector", None):
    wait = wait_epoll

    poll_evmasks = {
        Wait.R: select.EPOLLONESHOT | select.EPOLLIN,
        Wait.W: select.EPOLLONESHOT | select.EPOLLOUT,
        Wait.RW: select.EPOLLONESHOT | select.EPOLLIN | select.EPOLLOUT,
    }

else:
    wait = wait_selector
