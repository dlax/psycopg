"""
Code concerned with waiting in different contexts (blocking, async, etc).

These functions are designed to consume the generators returned by the
`generators` module function and to return their final value.

"""

# Copyright (C) 2020 The Psycopg Team


import select
import selectors
import socket
from enum import IntEnum
from typing import Optional, TYPE_CHECKING
from asyncio import (
    get_event_loop,
    wait_for,
    Event,
    TimeoutError as AsyncIOTimeoutError,
)
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

from . import errors as e
from .abc import PQGen, PQGenConn, RV

if TYPE_CHECKING:
    import anyio
else:
    anyio = None


class Wait(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE
    RW = EVENT_READ | EVENT_WRITE


class Ready(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE
    RW = EVENT_READ | EVENT_WRITE


def wait_selector(gen: PQGen[RV], fileno: int, timeout: Optional[float] = None) -> RV:
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
        with DefaultSelector() as sel:
            while True:
                sel.register(fileno, s)
                rlist = None
                while not rlist:
                    rlist = sel.select(timeout=timeout)
                sel.unregister(fileno)
                # note: this line should require a cast, but mypy doesn't complain
                ready: Ready = rlist[0][1]
                assert s & ready
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
    try:
        fileno, s = next(gen)
        if not timeout:
            timeout = None
        with DefaultSelector() as sel:
            while True:
                sel.register(fileno, s)
                rlist = sel.select(timeout=timeout)
                sel.unregister(fileno)
                if not rlist:
                    raise e.ConnectionTimeout("connection timeout expired")
                ready: Ready = rlist[0][1]  # type: ignore[assignment]
                fileno, s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


async def wait_asyncio(gen: PQGen[RV], fileno: int) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but exposing an `asyncio` interface.
    """
    # Use an event to block and restart after the fd state changes.
    # Not sure this is the best implementation but it's a start.
    ev = Event()
    loop = get_event_loop()
    ready: Ready
    s: Wait

    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready |= state  # type: ignore[assignment]
        ev.set()

    try:
        s = next(gen)
        while True:
            reader = s & Wait.R
            writer = s & Wait.W
            if not reader and not writer:
                raise e.InternalError(f"bad poll status: {s}")
            ev.clear()
            ready = 0  # type: ignore[assignment]
            if reader:
                loop.add_reader(fileno, wakeup, Ready.R)
            if writer:
                loop.add_writer(fileno, wakeup, Ready.W)
            try:
                await ev.wait()
            finally:
                if reader:
                    loop.remove_reader(fileno)
                if writer:
                    loop.remove_writer(fileno)
            s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


async def wait_conn_asyncio(gen: PQGenConn[RV], timeout: Optional[float] = None) -> RV:
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
    # Use an event to block and restart after the fd state changes.
    # Not sure this is the best implementation but it's a start.
    ev = Event()
    loop = get_event_loop()
    ready: Ready
    s: Wait

    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready = state
        ev.set()

    try:
        fileno, s = next(gen)
        if not timeout:
            timeout = None
        while True:
            reader = s & Wait.R
            writer = s & Wait.W
            if not reader and not writer:
                raise e.InternalError(f"bad poll status: {s}")
            ev.clear()
            ready = 0  # type: ignore[assignment]
            if reader:
                loop.add_reader(fileno, wakeup, Ready.R)
            if writer:
                loop.add_writer(fileno, wakeup, Ready.W)
            try:
                await wait_for(ev.wait(), timeout)
            finally:
                if reader:
                    loop.remove_reader(fileno)
                if writer:
                    loop.remove_writer(fileno)
            fileno, s = gen.send(ready)

    except AsyncIOTimeoutError:
        raise e.ConnectionTimeout("connection timeout expired")

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


# AnyIO's wait_socket_readable() and wait_socket_writable() functions work
# with socket object (despite the underlying async libraries -- asyncio and
# trio -- accept integer 'fileno' values):
# https://github.com/agronholm/anyio/issues/386


def _fromfd(fileno: int) -> socket.socket:
    try:
        return socket.fromfd(fileno, socket.AF_INET, socket.SOCK_STREAM)
    except OSError as exc:
        raise e.OperationalError(
            f"failed to build a socket from connection file descriptor: {exc}"
        )


async def wait_anyio(gen: PQGen[RV], fileno: int) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but exposing an `anyio` interface.
    """
    global anyio
    import anyio  # Should not fail when coming from AnyIOConnection.

    s: Wait
    ready: Ready
    sock = _fromfd(fileno)

    async def readable(ev: "anyio.Event") -> None:
        await anyio.wait_socket_readable(sock)
        nonlocal ready
        ready |= Ready.R  # type: ignore[assignment]
        ev.set()

    async def writable(ev: anyio.Event) -> None:
        await anyio.wait_socket_writable(sock)
        nonlocal ready
        ready |= Ready.W  # type: ignore[assignment]
        ev.set()

    try:
        s = next(gen)
        while True:
            reader = s & Wait.R
            writer = s & Wait.W
            if not reader and not writer:
                raise e.InternalError(f"bad poll status: {s}")
            ev = anyio.Event()
            ready = 0  # type: ignore[assignment]
            async with anyio.create_task_group() as tg:
                if reader:
                    tg.start_soon(readable, ev)
                if writer:
                    tg.start_soon(writable, ev)
                await ev.wait()
                tg.cancel_scope.cancel()  # Move on upon first task done.

            s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv

    finally:
        sock.close()


async def wait_conn_anyio(gen: PQGenConn[RV], timeout: Optional[float] = None) -> RV:
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
    global anyio
    import anyio  # Should not fail when coming from AnyIOConnection.

    s: Wait
    ready: Ready

    async def readable(sock: socket.socket, ev: "anyio.Event") -> None:
        await anyio.wait_socket_readable(sock)
        nonlocal ready
        ready = Ready.R
        ev.set()

    async def writable(sock: socket.socket, ev: "anyio.Event") -> None:
        await anyio.wait_socket_writable(sock)
        nonlocal ready
        ready = Ready.W
        ev.set()

    timeout = timeout or None
    try:
        fileno, s = next(gen)

        while True:
            reader = s & Wait.R
            writer = s & Wait.W
            if not reader and not writer:
                raise e.InternalError(f"bad poll status: {s}")
            ev = anyio.Event()
            ready = 0  # type: ignore[assignment]
            with _fromfd(fileno) as sock:
                async with anyio.create_task_group() as tg:
                    if reader:
                        tg.start_soon(readable, sock, ev)
                    if writer:
                        tg.start_soon(writable, sock, ev)
                    with anyio.fail_after(timeout):
                        await ev.wait()

            fileno, s = gen.send(ready)

    except TimeoutError:
        raise e.OperationalError("timeout expired")

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


def wait_epoll(gen: PQGen[RV], fileno: int, timeout: Optional[float] = None) -> RV:
    """
    Wait for a generator using epoll where supported.

    Parameters are like for `wait()`. If it is detected that the best selector
    strategy is `epoll` then this function will be used instead of `wait`.

    See also: https://linux.die.net/man/2/epoll_ctl
    """
    try:
        s = next(gen)

        if timeout is None or timeout < 0:
            timeout = 0
        else:
            timeout = int(timeout * 1000.0)

        with select.epoll() as epoll:
            evmask = poll_evmasks[s]
            epoll.register(fileno, evmask)
            while True:
                fileevs = None
                while not fileevs:
                    fileevs = epoll.poll(timeout)
                ev = fileevs[0][1]
                ready = 0
                if ev & ~select.EPOLLOUT:
                    ready = Ready.R
                if ev & ~select.EPOLLIN:
                    ready |= Ready.W
                assert s & ready
                s = gen.send(ready)
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
